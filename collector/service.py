"""采集调度服务。

调度原则：
- 额度按站点组（共享访问身份）记账，先记账再发请求，多进程多任务不会一起触发封禁；
- 一个站点组冷却或等待只影响该组，其他组的城市专题照常推进；
- robots 在发请求之前判断，转址先按新地址的规则决定能否继续；
- 来源返回 Retry-After 时整组冷却，冷却到期不等于 robots 禁止的路径重新获准；
- 已获访问次数少者优先、同等时新增专题优先，旧链接不会长期挡住新专题的首次机会；
- 取消的专题不再促成新访问，但进行中的传输如实继续完成，不虚报为已取消。
"""
import hashlib
import json
import time
import uuid
from html.parser import HTMLParser
from urllib.parse import quote, urljoin, urlsplit

from .robots import is_disallowed, parse_disallows
from .source import SourceClient
from .store import Store, wall_now

REDIRECT_STATUSES = (301, 302, 303, 307, 308)


def entry_url(base, name):
    """专题的授权入口地址。"""
    return base.rstrip('/') + '/topics/' + quote(name, safe='')


def add_topic(store, name):
    """资料组新增城市专题。只登记专题，入口链接由服务按来源策略补建。"""
    with store.lock:
        row = store.db.execute('SELECT status FROM topics WHERE name=?', (name,)).fetchone()
        reactivated = row is not None and row['status'] != 'active'
        if row is None:
            store.db.execute(
                "INSERT INTO topics(name,status,granted,added_seq) VALUES(?,'active',0,?)",
                (name, store.next_seq()))
        elif reactivated:
            store.db.execute(
                "UPDATE topics SET status='active', cancelled_wall=NULL WHERE name=?", (name,))
        store.db.commit()
    revived = store.revive_dropped()
    store.event(None, '专题新增', name)
    return {'name': name, 'reactivated': reactivated, 'revived': revived}


def cancel_topic(store, name):
    """取消专题：不再促成新访问；进行中的传输如实继续，不虚报为已取消。"""
    with store.lock:
        row = store.db.execute('SELECT status FROM topics WHERE name=?', (name,)).fetchone()
        if row is None:
            return {'name': name, 'found': False}
        if row['status'] == 'cancelled':
            return {'name': name, 'found': True, 'already': True}
        store.db.execute(
            "UPDATE topics SET status='cancelled', cancelled_wall=? WHERE name=?",
            (wall_now(), name))
        store.db.commit()
        in_flight = store.db.execute(
            "SELECT COUNT(*) AS n FROM links WHERE state='in_flight' AND url IN"
            ' (SELECT url FROM topic_links WHERE topic=?)', (name,)).fetchone()['n']
        received = store.db.execute(
            "SELECT COUNT(*) AS n FROM links WHERE state='done' AND url IN"
            ' (SELECT url FROM topic_links WHERE topic=?)', (name,)).fetchone()['n']
    dropped = store.drop_orphans()
    store.event(None, '专题取消', '%s（停止促成 %d 条，传输中 %d 条继续）' % (name, dropped, in_flight))
    return {'name': name, 'found': True, 'dropped': dropped,
            'in_flight': in_flight, 'received': received}


class _DocumentLinkParser(HTMLParser):
    """从专题页 HTML 中找出 rel=document 的资料链接。"""

    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag != 'a':
            return
        attr = dict(attrs)
        rel = (attr.get('rel') or '').split()
        if 'document' in rel and attr.get('href'):
            self.hrefs.append(attr['href'])


def extract_document_links(body):
    parser = _DocumentLinkParser()
    parser.feed(body.decode('utf-8', 'replace'))
    return parser.hrefs


class Service:
    def __init__(self, state_dir, source, poll=0.5):
        self.store = Store(state_dir)
        self.client = SourceClient(source)
        self.source = source.rstrip('/')
        self.poll = poll
        self._load_policies()
        self.recover()

    # ---- 来源契约 ----
    def _load_policies(self):
        policies = self.client.policies()
        self.groups = policies.get('groups', {})
        self.host_group = {}
        for name, group in self.groups.items():
            for origin in group.get('origins', []):
                self.host_group[origin] = name
        self.store.set_meta('policies', json.dumps(policies, ensure_ascii=False))

    def group_of(self, host):
        """主机所属的站点组（共享访问身份）；未知主机按自身成组。"""
        return self.host_group.get(host, host)

    def limits_of(self, grp):
        group = self.groups.get(grp)
        if group:
            return group['maximum'], group['window_seconds']
        return 1, 5

    def _check_source_clock(self, clock):
        """来源时钟回退说明来源已重启，其额度与冷却状态以来源为准重置。"""
        last = self.store.get_meta('last_clock')
        if last is not None and clock < int(last):
            self.store.clear_ledger()
            self.store.clear_cooldowns()
            self.store.event(clock, '来源重启', '来源时钟回退，按来源状态重置台账与冷却')
        self.store.set_meta('last_clock', clock)

    def recover(self):
        """启动恢复：保留仍有效的冷却与台账，重置中断的传输，补登记暂存正文。"""
        clock = self.client.health()['time']
        self._check_source_clock(clock)
        reset = self.store.reset_in_flight()
        if reset:
            self.store.event(clock, '中断恢复', '%d 条传输中的链接重新排队' % reset)
        self.store.drop_orphans()
        staged = self.register_staging()
        if staged:
            self.store.event(clock, '中断恢复', '%d 份已取回正文补登记' % staged)

    # ---- 调度 ----
    def ensure_entries(self):
        """为每个进行中专题补建授权入口链接。"""
        host = urlsplit(self.source).hostname
        grp = self.group_of(host)
        for name in self.store.active_topics():
            url = entry_url(self.source, name)
            self.store.add_link(url, host, grp, 'page', '入口', (name,))

    def tick(self):
        """处理一轮当前可放行的工作；返回是否有进展。"""
        clock = self.client.health()['time']
        self._check_source_clock(clock)
        self.ensure_entries()
        did = False
        while self._one_round(clock):
            did = True
        return did

    def _one_round(self, clock):
        cooldowns = self.store.cooldowns()
        for cand in self.store.next_candidates():
            cooling = cooldowns.get(cand['grp'])
            if cooling and clock < cooling['until_clock']:
                continue
            disallows = self._robots(cand['host'], cand['url'], clock)
            path = urlsplit(cand['url']).path or '/'
            if is_disallowed(disallows, path):
                self.store.refuse_robots(cand['url'], 'robots 禁止 ' + path)
                self.store.event(clock, 'robots 拒绝', cand['url'])
                return True
            maximum, window = self.limits_of(cand['grp'])
            req_id, _wait = self.store.try_reserve(
                cand['grp'], cand['host'], cand['url'], clock, maximum, window)
            if req_id is None:
                continue
            self._execute(dict(cand), req_id, clock)
            return True
        return False

    def _robots(self, host, url, clock):
        cached = self.store.get_robots(host, clock)
        if cached is not None:
            return cached
        disallows = parse_disallows(self.client.robots_text(url))
        self.store.put_robots(host, disallows, clock)
        return disallows

    def _execute(self, link, req_id, clock):
        url = link['url']
        if not self.store.mark_in_flight(url):
            # 并发取消已使其不再待访问；预约记录保留（保守计额），不发出请求
            self.store.record_status(req_id, None)
            return
        try:
            status, headers, body = self.client.get(url)
        except OSError as exc:
            # 台账记录保留（保守计额），链接重新排队
            self.store.record_status(req_id, None)
            self.store.back_to_pending(url, '网络错误：%s' % exc)
            self.store.event(clock, '网络错误', url)
            return
        self.store.record_status(req_id, status)
        if status == 429:
            try:
                retry = int(headers.get('retry-after') or '1')
            except ValueError:
                retry = 1
            self.store.set_cooldown(link['grp'], clock + retry, 'Retry-After %d' % retry, clock)
            self.store.back_to_pending(url, '来源冷却')
            self.store.event(clock, '站点组冷却', '%s 至时钟 %d' % (link['grp'], clock + retry))
            return
        redirect = status in REDIRECT_STATUSES
        kind = 'redirect' if redirect else link['kind']
        meta = {'url': url, 'kind': kind, 'status': status, 'headers': headers,
                'clock': clock, 'wall': wall_now(), 'link_state': 'done'}
        target = None
        if redirect:
            target = urljoin(url, headers.get('location') or '')
            meta['target'] = target
        elif status >= 400:
            meta['link_state'] = 'failed'
            meta['detail'] = 'HTTP %d' % status
        token = self._stage(meta, body)
        self._register_staged(token)
        if redirect and target:
            host = urlsplit(target).hostname
            if host:
                self.store.add_link(target, host, self.group_of(host), 'document',
                                    url, self.store.topics_for_link(url))
                self.store.event(clock, '发现转址', '%s -> %s' % (url, target))
        elif kind == 'page' and status == 200:
            self._discover(url, body, clock)
        if meta['link_state'] == 'failed':
            self.store.event(clock, '访问失败', '%s HTTP %d' % (url, status))

    def _discover(self, page_url, body, clock):
        topics = self.store.topics_for_link(page_url)
        found = 0
        for href in extract_document_links(body):
            url = urljoin(page_url, href)
            host = urlsplit(url).hostname
            if not host:
                continue
            self.store.add_link(url, host, self.group_of(host), 'document', page_url, topics)
            found += 1
        if found:
            self.store.event(clock, '发现链接', '%s 发现 %d 条' % (page_url, found))

    # ---- 正文暂存与登记 ----
    def _stage(self, meta, body):
        token = uuid.uuid4().hex
        staging = self.store.dir / 'staging'
        (staging / (token + '.bin')).write_bytes(body)
        note = dict(meta, token=token)
        (staging / (token + '.json')).write_text(
            json.dumps(note, ensure_ascii=False), encoding='utf-8')
        return token

    def _register_staged(self, token):
        staging = self.store.dir / 'staging'
        jf = staging / (token + '.json')
        bf = staging / (token + '.bin')
        meta = json.loads(jf.read_text(encoding='utf-8'))
        body = bf.read_bytes()
        digest = hashlib.sha256(body).hexdigest()
        doc = self.store.document_by_url(meta['url'])
        doc_id = doc['id'] if doc else self.store.insert_document(meta, digest, len(body))
        final = self.store.dir / 'documents' / ('%d.bin' % doc_id)
        final.write_bytes(body)
        note = dict(meta, sha256=digest, size=len(body))
        note.pop('token', None)
        (self.store.dir / 'documents' / ('%d.json' % doc_id)).write_text(
            json.dumps(note, ensure_ascii=False, indent=1), encoding='utf-8')
        self.store.update_document_path(doc_id, 'documents/%d.bin' % doc_id)
        self.store.finish_link(meta['url'], doc_id,
                               meta.get('link_state', 'done'), meta.get('detail'))
        jf.unlink(missing_ok=True)
        bf.unlink(missing_ok=True)
        self.store.event(meta['clock'], '登记响应', '%s -> documents/%d.bin（%s）'
                         % (meta['url'], doc_id, meta.get('link_state', 'done')))

    def register_staging(self):
        """补登记已取回但尚未登记的正文（中断恢复）；返回份数。"""
        staging = self.store.dir / 'staging'
        count = 0
        for jf in sorted(staging.glob('*.json')):
            bf = staging / (jf.stem + '.bin')
            try:
                json.loads(jf.read_text(encoding='utf-8'))
            except (ValueError, OSError):
                jf.unlink(missing_ok=True)
                bf.unlink(missing_ok=True)
                continue
            if not bf.exists():
                jf.unlink(missing_ok=True)
                continue
            self._register_staged(jf.stem)
            count += 1
        return count

    # ---- 拒绝复查 ----
    def retry_refused(self, include_failed=False):
        """复查被来源拒绝的链接：robots 已放行的恢复排队；可选重试失败项。"""
        clock = self.client.health()['time']
        self._check_source_clock(clock)
        resumed = 0
        for link in self.store.refused_links('refused_robots'):
            disallows = parse_disallows(self.client.robots_text(link['url']))
            self.store.put_robots(link['host'], disallows, clock)
            path = urlsplit(link['url']).path or '/'
            if not is_disallowed(disallows, path):
                self.store.back_to_pending(link['url'], 'robots 复查后恢复')
                resumed += 1
        failed = self.store.reset_failed() if include_failed else 0
        self.store.event(clock, '拒绝复查', '恢复 %d 条 robots 拒绝，%d 条失败重试' % (resumed, failed))
        return resumed, failed

    # ---- 运行 ----
    def serve(self, stop=None, max_idle=None):
        """持续运行：有工作就调度，额度等待时按来源时钟轮询，不空耗真实时间。"""
        idle = 0
        while stop is None or not stop.is_set():
            try:
                did = self.tick()
            except OSError:
                did = False
            if did:
                idle = 0
                continue
            if not self.store.has_open_work():
                idle += 1
                if max_idle and idle >= max_idle:
                    return
            time.sleep(self.poll)
