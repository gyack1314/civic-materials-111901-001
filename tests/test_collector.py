"""采集端调度与恢复检查：以本地来源的可回查记录（/__admin/log）验证行为。"""
import http.client
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from urllib.parse import quote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collector import report
from collector.service import Service, add_topic, cancel_topic, entry_url
from collector.store import Store
from source.server import running_source


def get_json(url):
    parts = urlsplit(url)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=10)
    conn.request('GET', parts.path)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, json.loads(data.decode('utf-8'))


def get_status(url):
    """只取状态码的 GET（专题页是 HTML，不按 JSON 解析）。"""
    parts = urlsplit(url)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=10)
    conn.request('GET', parts.path)
    resp = conn.getresponse()
    resp.read()
    status = resp.status
    conn.close()
    return status


def post_json(url, payload):
    parts = urlsplit(url)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=10)
    conn.request('POST', parts.path, json.dumps(payload).encode())
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, json.loads(data.decode('utf-8'))


def source_log(base):
    return get_json(base + '/__admin/log')[1]


def product_entries(log):
    return [x for x in log if x['path'].startswith(('/topics/', '/files/', '/redirect/'))]


def host_of(entry):
    return entry['headers'].get('host', '')


def port_of(base):
    return urlsplit(base).port


def wait_for(pred, timeout=15.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


class CollectorCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name) / 'state'
        self.services = []

    def tearDown(self):
        self.stop_all()

    def start_service(self, base, poll=0.02):
        service = Service(self.state, base, poll=poll)
        stop = threading.Event()
        thread = threading.Thread(target=service.serve, kwargs={'stop': stop}, daemon=True)
        thread.start()
        self.services.append((service, stop, thread))
        return service

    def stop_all(self):
        for service, stop, thread in self.services:
            stop.set()
            thread.join(timeout=10)
            service.store.close()
        self.services = []

    def pump(self, base, service, seconds=10, rounds=100):
        """扮演演练操作员：推进时钟直到没有待办。"""
        idle = 0
        for _ in range(rounds):
            if not service.store.has_open_work():
                idle += 1
                if idle >= 3:
                    return True
                time.sleep(0.1)
                continue
            idle = 0
            post_json(base + '/__admin/advance', {'seconds': seconds})
            time.sleep(0.05)
        return False

    def entry(self, base, name):
        return entry_url(base, name)

    def link_state(self, service, url):
        row = service.store.link(url)
        return row['state'] if row else None

    def test_end_to_end_shared_budget_and_catalog(self):
        with running_source() as (base, source):
            service = self.start_service(base)
            add_topic(service.store, '滨江')
            add_topic(service.store, '临港')
            self.assertTrue(self.pump(base, service), '待办未在预期内完成')
            self.stop_all()

            log = source_log(base)
            prod = product_entries(log)
            # 共同原件只取回一次，但两个专题的来源关系都保留
            shared = [x for x in prod if x['path'] == '/files/shared']
            self.assertEqual(len(shared), 1, '共同原件不应重复下载')
            # 市站组每 10 演练秒不超过 2 次，文件站组间隔至少 5 演练秒
            city = sorted(x['time'] for x in prod if not host_of(x).startswith('127.0.0.2'))
            for t in city:
                self.assertLessEqual(len([u for u in city if t - 10 < u <= t]), 2)
            files = sorted(x['time'] for x in prod if host_of(x).startswith('127.0.0.2'))
            for a, b in zip(files, files[1:]):
                self.assertGreaterEqual(b - a, 5)
            # 调度精准：没有一次访问被来源 429 拒绝
            store = Store(self.state)
            try:
                self.assertNotIn(429, [r['status'] for r in store.ledger()])
            finally:
                store.close()
            # 每个主机在首次产品访问前都先核对过 robots
            for host, prefix in (('127.0.0.1', '/topics/'), ('127.0.0.2', '/files/')):
                first = next(i for i, x in enumerate(log)
                             if x['path'].startswith(prefix) and host_of(x).startswith(host))
                self.assertTrue(any(x['path'] == '/robots.txt' and host_of(x).startswith(host)
                                    for x in log[:first]), host + ' 访问先于 robots 核对')
            # 转址目标的一跳记在实际目标（文件站）的额度上
            self.assertTrue(any(x['path'] == '/files/' + quote('滨江')
                                and host_of(x).startswith('127.0.0.2') for x in prod))

            store = Store(self.state)
            try:
                data = report.gather(store)
                topics = {t['名称']: t for t in data['专题']}
                self.assertEqual(topics['滨江']['进展'], '已齐')
                self.assertEqual(topics['临港']['进展'], '已齐')
                shared_doc = next(d for d in data['原件'] if d['地址'].endswith('/files/shared'))
                self.assertEqual(sorted(shared_doc['引用专题']), ['临港', '滨江'])
                self.assertTrue((self.state / shared_doc['保存']).exists())
            finally:
                store.close()

    def test_robots_phase_change_blocks_city_files_but_not_file_host(self):
        with running_source() as (base, source):
            post_json(base + '/__admin/advance', {'seconds': 0, 'phase': 1})
            service = self.start_service(base)
            add_topic(service.store, '滨江')
            self.assertTrue(self.pump(base, service))
            # 冷却与窗口全部到期后，robots 禁止的路径仍不得访问
            post_json(base + '/__admin/advance', {'seconds': 30})
            time.sleep(0.3)
            self.stop_all()

            prod = product_entries(source_log(base))
            self.assertFalse(any(x['path'] == '/files/shared' for x in prod),
                             '阶段 1 市站 robots 禁止 /files/，不得访问')
            self.assertTrue(any(x['path'].startswith('/files/')
                                and host_of(x).startswith('127.0.0.2') for x in prod),
                            '文件站授权不受市站 robots 影响')
            store = Store(self.state)
            try:
                refused = [r['url'] for r in store.refused_links('refused_robots')]
                self.assertEqual(len(refused), 1)
                self.assertTrue(refused[0].endswith('/files/shared'))
            finally:
                store.close()

    def test_retry_after_cools_whole_group_and_survives_restart(self):
        with running_source() as (base, source):
            service = self.start_service(base)
            # 演练操作员从另一入口占满市站组额度，采集端下一次访问将收到 429
            get_status(base + '/topics/external1')
            get_status(base + '/topics/external2')
            add_topic(service.store, '甲')
            self.assertTrue(wait_for(lambda: 'shared-city' in service.store.cooldowns()))
            cooling = service.store.cooldowns()['shared-city']
            self.assertEqual(cooling['until_clock'], 10)

            # 冷却期间市站组不再有产品访问；文件站组不受同一冷却限制
            direct = 'http://127.0.0.2:%d/files/direct' % port_of(base)
            service.store.add_link(direct, '127.0.0.2', 'files', 'document', '改入口', ('甲',))
            self.assertTrue(wait_for(lambda: self.link_state(service, direct) == 'done'))
            city_count = len([x for x in product_entries(source_log(base))
                              if not host_of(x).startswith('127.0.0.2')])
            post_json(base + '/__admin/advance', {'seconds': 3})
            time.sleep(0.3)
            self.assertEqual(city_count, len([x for x in product_entries(source_log(base))
                                              if not host_of(x).startswith('127.0.0.2')]))

            # 采集端重启后冷却仍然有效，不会忘掉仍有效的来源冷却
            self.stop_all()
            service = self.start_service(base)
            post_json(base + '/__admin/advance', {'seconds': 2})
            time.sleep(0.3)
            self.assertEqual(city_count, len([x for x in product_entries(source_log(base))
                                              if not host_of(x).startswith('127.0.0.2')]))
            # 冷却到期后自动继续，并最终收齐
            post_json(base + '/__admin/advance', {'seconds': 6})
            entry = self.entry(base, '甲')
            self.assertTrue(self.pump(base, service))
            self.assertEqual(self.link_state(service, entry), 'done')
            self.stop_all()

    def test_new_topic_gets_chance_before_old_backlog(self):
        with running_source() as (base, source):
            service = self.start_service(base)
            add_topic(service.store, '旧')
            # 等旧专题占满市站组当前窗口（专题页 + 共同原件）
            self.assertTrue(wait_for(
                lambda: len([r for r in service.store.ledger()
                             if r['grp'] == 'shared-city']) >= 2))
            add_topic(service.store, '新')
            post_json(base + '/__admin/advance', {'seconds': 10})
            new_entry = self.entry(base, '新')
            self.assertTrue(wait_for(lambda: self.link_state(service, new_entry) == 'done'))
            self.assertTrue(self.pump(base, service))
            self.stop_all()

            prod = product_entries(source_log(base))
            first_new = next(i for i, x in enumerate(prod)
                             if x['path'] == '/topics/' + quote('新'))
            old_redirect = next(i for i, x in enumerate(prod)
                                if x['path'] == '/redirect/' + quote('旧'))
            self.assertLess(first_new, old_redirect,
                            '新增专题的首次机会不应排在旧链接积压之后')

    def test_cancel_stops_new_access_but_keeps_relations_and_in_flight(self):
        with running_source() as (base, source):
            service = self.start_service(base)
            add_topic(service.store, '甲')
            add_topic(service.store, '乙')
            for name in ('甲', '乙'):
                self.assertTrue(wait_for(
                    lambda n=name: self.link_state(service, self.entry(base, n)) == 'done'))
                # 等专题页发现的链接全部登记，再取消，避免与发现过程竞态
                redirect = self.entry(base, name).replace('/topics/', '/redirect/')
                self.assertTrue(wait_for(
                    lambda u=redirect: service.store.link(u) is not None))
            result = cancel_topic(service.store, '甲')
            self.assertGreaterEqual(result['dropped'], 1)
            self.assertEqual(result['in_flight'], 0)
            self.assertTrue(self.pump(base, service))
            self.stop_all()

            prod = product_entries(source_log(base))
            self.assertFalse(any(x['path'] == '/redirect/' + quote('甲') for x in prod),
                             '取消后不得再为甲促成新访问')
            self.assertTrue(any(x['path'] == '/redirect/' + quote('乙') for x in prod),
                            '乙的待办应继续')
            store = Store(self.state)
            try:
                data = report.gather(store)
                shared_doc = next(d for d in data['原件'] if d['地址'].endswith('/files/shared'))
                self.assertIn('甲', shared_doc['引用专题'], '共享原件不得丢掉甲的来源关系')
                self.assertIn('乙', shared_doc['引用专题'])
            finally:
                store.close()

            # 进行中的传输不能被虚报为取消成功
            service = Service(self.state, base, poll=0.02)
            add_topic(service.store, '丙')
            slow = 'http://127.0.0.2:%d/files/q2' % port_of(base)
            service.store.add_link(slow, '127.0.0.2', 'files', 'document', '测试', ('丙',))
            service.store.mark_in_flight(slow)
            result = cancel_topic(service.store, '丙')
            self.assertEqual(result['in_flight'], 1, '传输中的链接必须如实报告')
            self.assertEqual(service.store.link(slow)['state'], 'in_flight')
            service.store.close()
            # 中断恢复后该链接不再促成访问（丙已取消）
            service = self.start_service(base)
            self.assertTrue(self.pump(base, service))
            self.stop_all()
            self.assertFalse(any(x['path'] == '/files/q2'
                                 for x in product_entries(source_log(base))))

    def test_recovery_registers_staging_and_avoids_duplicates(self):
        with running_source() as (base, source):
            service = self.start_service(base)
            add_topic(service.store, '甲')
            entry = self.entry(base, '甲')
            self.assertTrue(wait_for(lambda: self.link_state(service, entry) == 'done'))
            self.stop_all()
            # 上次只发现链接、未取回正文的地址仍是待访问，不是已取得
            store = Store(self.state)
            try:
                pending = [r for r in store.refused_links('pending')]
            finally:
                store.close()
            self.assertTrue(pending, '专题页之后应存在待访问链接')

            service = self.start_service(base)
            self.assertTrue(self.pump(base, service))
            self.stop_all()
            counts = {}
            for x in product_entries(source_log(base)):
                counts[x['path']] = counts.get(x['path'], 0) + 1
            self.assertTrue(all(n == 1 for n in counts.values()),
                            '已保存的原件不得重复下载：%s' % counts)
            # 再次启动不再产生任何新的产品访问
            before = len(product_entries(source_log(base)))
            self.start_service(base)
            time.sleep(0.4)
            self.stop_all()
            self.assertEqual(before, len(product_entries(source_log(base))))

            # 正文已取得但尚未登记的暂存，重启时补登记，不重新下载
            store = Store(self.state)
            add_topic(store, '丁')
            manual = 'http://127.0.0.1:%d/files/manual' % port_of(base)
            store.add_link(manual, '127.0.0.1', 'shared-city', 'document', '测试', ('丁',))
            token = 'testtoken'
            (store.dir / 'staging' / (token + '.bin')).write_bytes('已取得未登记的正文'.encode())
            (store.dir / 'staging' / (token + '.json')).write_text(json.dumps({
                'url': manual, 'kind': 'document', 'status': 200, 'headers': {},
                'clock': 0, 'wall': '2026-11-18T00:00:00+00:00', 'link_state': 'done',
            }, ensure_ascii=False))
            store.close()
            service = self.start_service(base)
            self.assertTrue(wait_for(lambda: self.link_state(service, manual) == 'done'))
            self.assertTrue(self.pump(base, service))
            self.stop_all()
            self.assertFalse(any(x['path'] == '/files/manual'
                                 for x in product_entries(source_log(base))),
                             '暂存正文补登记不得触发重新下载')

    def test_redirect_target_prechecked_before_any_request(self):
        with running_source() as (base, source):
            service = self.start_service(base)
            add_topic(service.store, '甲')
            closed = 'http://127.0.0.2:%d/closed/secret' % port_of(base)
            service.store.add_link(closed, '127.0.0.2', 'files', 'document', '测试', ('甲',))
            self.assertTrue(self.pump(base, service))
            state = self.link_state(service, closed)
            self.stop_all()
            self.assertEqual(state, 'refused_robots')
            self.assertFalse(any(x['path'].startswith('/closed/')
                                 for x in source_log(base)),
                             'robots 禁止的地址不得先发出请求再补记拒绝')

    def test_source_restart_resets_cooldown_with_source_state(self):
        port = 18971
        with running_source(port=port) as (base, source):
            service = self.start_service(base)
            post_json(base + '/__admin/advance', {'seconds': 30})
            get_status(base + '/topics/external1')
            get_status(base + '/topics/external2')
            add_topic(service.store, '甲')
            self.assertTrue(wait_for(lambda: 'shared-city' in service.store.cooldowns()))
            self.stop_all()
        # 来源进程重启会清空其演练状态，采集端按来源当前状态重新对齐
        with running_source(port=port) as (base, source):
            service = self.start_service(base)
            entry = self.entry(base, '甲')
            self.assertTrue(wait_for(lambda: self.link_state(service, entry) == 'done'))
            self.assertTrue(self.pump(base, service))
            self.assertFalse(service.store.cooldowns(), '来源重启后旧冷却不再有效')
            self.stop_all()


if __name__ == '__main__':
    unittest.main()
