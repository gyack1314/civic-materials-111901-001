"""原件目录与专题收集状态报告。

资料员据此区分三种情况并决定处置：
- 额度等待：继续等待，冷却或窗口到期后服务自动继续；
- 来源拒绝：robots 禁止的改入口或待政策调整后复查，HTTP 失败的人工处理；
- 正文已取得但尚未登记：重启服务自动补登记，不必重新下载。
"""
import json

from .store import wall_now

STATE_LABELS = {
    'pending': '待访问',
    'in_flight': '传输中',
    'done': '已取得',
    'dropped': '已停止促成',
    'refused_robots': '来源拒绝（robots）',
    'failed': '来源拒绝（HTTP）',
}


def _group_states(store):
    raw = store.get_meta('policies')
    groups = json.loads(raw).get('groups', {}) if raw else {}
    clock = int(store.get_meta('last_clock') or 0)
    cooldowns = store.cooldowns()
    states = {}
    for name, group in groups.items():
        until = cooldowns[name]['until_clock'] if name in cooldowns else None
        cooling = until is not None and clock < until
        states[name] = {
            '冷却至时钟': until if cooling else None,
            '窗口下一可用时钟': store.group_wait(
                name, clock, group['maximum'], group['window_seconds']),
        }
    return states, clock


def classify(link, groups):
    """把链接状态翻译成资料员可处置的情况与建议。"""
    state = link['state']
    if state == 'done':
        return '已取得'
    if state == 'in_flight':
        return '传输中 → 等待完成'
    if state == 'refused_robots':
        return '来源拒绝（robots 禁止）→ 改入口，或待政策调整后用 retry-refused 复查'
    if state == 'failed':
        return '来源拒绝（%s）→ 人工处理' % (link['detail'] or '')
    if state == 'dropped':
        return '已停止促成（无进行中专题引用）'
    group = groups.get(link['grp'], {})
    if group.get('冷却至时钟') is not None:
        return '额度等待（%s 冷却至时钟 %s）→ 继续等待' % (link['grp'], group['冷却至时钟'])
    if group.get('窗口下一可用时钟') is not None:
        return '额度等待（%s 窗口至时钟 %s）→ 继续等待' % (link['grp'], group['窗口下一可用时钟'])
    return '排队中 → 继续等待'


def gather_catalog(store):
    """原件目录：每份已保存的响应及其被哪些专题引用。"""
    items = []
    with store.lock:
        docs = store.db.execute('SELECT * FROM documents ORDER BY id').fetchall()
        for doc in docs:
            topics = [r['topic'] for r in store.db.execute(
                'SELECT tl.topic FROM topic_links tl'
                ' JOIN links l ON l.url=tl.url WHERE l.document_id=? ORDER BY tl.topic',
                (doc['id'],)).fetchall()]
            headers = json.loads(doc['headers'])
            items.append({
                '编号': doc['id'],
                '类型': doc['kind'],
                '地址': doc['url'],
                '状态': doc['status'],
                '来源文档号': headers.get('x-document-id'),
                'SHA256': doc['sha256'],
                '字节': doc['size'],
                '保存': doc['path'],
                '取得时钟': doc['fetched_clock'],
                '取得时间': doc['fetched_wall'],
                '引用专题': topics,
            })
    return items


def gather_topics(store, groups):
    """专题收集状态：每个专题已收到什么、还有哪些链接未取得。"""
    result = []
    with store.lock:
        topics = store.db.execute('SELECT * FROM topics ORDER BY added_seq').fetchall()
        for topic in topics:
            links = store.db.execute(
                'SELECT l.*, d.path AS doc_path FROM topic_links tl'
                ' JOIN links l ON l.url=tl.url'
                ' LEFT JOIN documents d ON d.id=l.document_id'
                ' WHERE tl.topic=? ORDER BY l.discovered_seq', (topic['name'],)).fetchall()
            received, waiting, refused, dropped = [], [], [], []
            for link in links:
                if link['state'] == 'done':
                    received.append({'链接': link['url'], '保存': link['doc_path']})
                elif link['state'] in ('pending', 'in_flight'):
                    waiting.append({'链接': link['url'], '情况': classify(link, groups)})
                elif link['state'] in ('refused_robots', 'failed'):
                    refused.append({'链接': link['url'], '情况': classify(link, groups)})
                else:
                    dropped.append({'链接': link['url'], '情况': classify(link, groups)})
            result.append({
                '名称': topic['name'],
                '状态': '进行中' if topic['status'] == 'active' else '已取消',
                '已获访问次数': topic['granted'],
                '进展': '已齐' if not waiting and topic['status'] == 'active' else
                        ('已取消' if topic['status'] != 'active' else '收集中'),
                '已取得': received,
                '未取得': waiting + refused,
                '不再促成': dropped,
            })
    return result


def gather(store):
    groups, clock = _group_states(store)
    staged = len(list((store.dir / 'staging').glob('*.json')))
    return {
        '生成时间': wall_now(),
        '来源时钟': clock,
        '站点组': groups,
        '正文已取得尚未登记': staged,
        '专题': gather_topics(store, groups),
        '原件': gather_catalog(store),
        '最近事件': store.recent_events(50),
    }


def write_reports(store):
    """把原件目录与专题收集状态写入状态目录 report/ 下。"""
    data = gather(store)
    report_dir = store.dir / 'report'
    catalog = {k: data[k] for k in ('生成时间', '来源时钟')}
    catalog['原件'] = data['原件']
    topics = {k: data[k] for k in ('生成时间', '来源时钟', '站点组', '正文已取得尚未登记')}
    topics['专题'] = data['专题']
    topics['最近事件'] = data['最近事件']
    catalog_path = report_dir / 'catalog.json'
    topics_path = report_dir / 'topics.json'
    catalog_path.write_text(json.dumps(catalog, ensure_ascii=False, indent=1), encoding='utf-8')
    topics_path.write_text(json.dumps(topics, ensure_ascii=False, indent=1), encoding='utf-8')
    return catalog_path, topics_path


def format_status(store, only=None):
    """终端可读的中文专题状态。"""
    data = gather(store)
    lines = ['专题收集状态（来源时钟 %s，生成于 %s）' % (data['来源时钟'], data['生成时间'])]
    for name, group in data['站点组'].items():
        notes = []
        if group['冷却至时钟'] is not None:
            notes.append('冷却至时钟 %s' % group['冷却至时钟'])
        if group['窗口下一可用时钟'] is not None:
            notes.append('窗口至时钟 %s' % group['窗口下一可用时钟'])
        lines.append('站点组 %s：%s' % (name, '，'.join(notes) if notes else '可访问'))
    if data['正文已取得尚未登记']:
        lines.append('正文已取得尚未登记 %d 份：重启服务自动补登记' % data['正文已取得尚未登记'])
    for topic in data['专题']:
        if only and topic['名称'] != only:
            continue
        lines.append('')
        lines.append('== %s（%s，已获访问 %d 次）==' % (
            topic['名称'], topic['进展'], topic['已获访问次数']))
        lines.append('  已取得 %d 份：' % len(topic['已取得']))
        for item in topic['已取得']:
            lines.append('    - %s → %s' % (item['链接'], item['保存']))
        if topic['未取得']:
            lines.append('  未取得 %d 条：' % len(topic['未取得']))
            for item in topic['未取得']:
                lines.append('    - %s：%s' % (item['链接'], item['情况']))
        if topic['不再促成']:
            lines.append('  不再促成 %d 条：' % len(topic['不再促成']))
            for item in topic['不再促成']:
                lines.append('    - %s：%s' % (item['链接'], item['情况']))
    if only and not any(t['名称'] == only for t in data['专题']):
        lines.append('（无专题「%s」）' % only)
    return '\n'.join(lines)
