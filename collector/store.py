"""采集端持久状态。

SQLite 数据库保存专题、链接、原件登记、额度台账、站点组冷却与 robots 缓存；
状态目录下 documents/ 保存原件正文与响应元信息，staging/ 暂存已取回但尚未登记的正文。

进程中断后用同一状态目录重启即可继续：
- requests 台账记录每一次已发出的产品访问（含被来源拒绝的），
  与来源的滚动窗口计数口径一致，重启后不会超额补发；
- cooldowns 记录来源返回 Retry-After 后整个站点组的冷却截止时钟，不会被忘掉；
- staging 中的正文在下次启动时补登记，不重复下载；
- 只发现过链接、未取回正文的地址保持 pending，不会被当成已取得。
"""
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS topics(
  name TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'active',
  granted INTEGER NOT NULL DEFAULT 0,
  added_seq INTEGER NOT NULL,
  cancelled_wall TEXT
);
CREATE TABLE IF NOT EXISTS links(
  url TEXT PRIMARY KEY,
  host TEXT NOT NULL,
  grp TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'document',
  state TEXT NOT NULL DEFAULT 'pending',
  detail TEXT,
  discovered_seq INTEGER NOT NULL,
  document_id INTEGER,
  target_url TEXT
);
CREATE TABLE IF NOT EXISTS topic_links(
  topic TEXT NOT NULL,
  url TEXT NOT NULL,
  via TEXT,
  PRIMARY KEY(topic, url)
);
CREATE TABLE IF NOT EXISTS documents(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT UNIQUE NOT NULL,
  kind TEXT NOT NULL,
  status INTEGER NOT NULL,
  headers TEXT NOT NULL,
  sha256 TEXT,
  size INTEGER NOT NULL DEFAULT 0,
  path TEXT,
  fetched_clock INTEGER NOT NULL,
  fetched_wall TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS requests(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  grp TEXT NOT NULL,
  host TEXT NOT NULL,
  url TEXT NOT NULL,
  clock INTEGER NOT NULL,
  status INTEGER,
  wall TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cooldowns(
  grp TEXT PRIMARY KEY,
  until_clock INTEGER NOT NULL,
  reason TEXT,
  set_clock INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS robots(
  host TEXT PRIMARY KEY,
  disallow TEXT NOT NULL,
  fetched_clock INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clock INTEGER,
  wall TEXT NOT NULL,
  kind TEXT NOT NULL,
  detail TEXT
);
"""


def wall_now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


class Store:
    """状态目录的数据库入口；进程内串行化，进程间靠 SQLite 事务协调。"""

    def __init__(self, state_dir):
        self.dir = Path(state_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        for sub in ('documents', 'staging', 'report'):
            (self.dir / sub).mkdir(exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.dir / 'collector.db', timeout=30, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self.lock:
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.executescript(SCHEMA)
            self.db.commit()

    def close(self):
        with self.lock:
            self.db.close()

    # ---- 元信息与事件 ----
    def get_meta(self, key):
        with self.lock:
            row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
            return row['value'] if row else None

    def set_meta(self, key, value):
        with self.lock:
            self.db.execute(
                'INSERT INTO meta(key,value) VALUES(?,?) '
                'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, str(value)))
            self.db.commit()

    def next_seq(self):
        with self.lock:
            row = self.db.execute("SELECT value FROM meta WHERE key='seq'").fetchone()
            value = int(row['value']) + 1 if row else 1
            self.set_meta('seq', value)
            return value

    def event(self, clock, kind, detail):
        with self.lock:
            self.db.execute('INSERT INTO events(clock,wall,kind,detail) VALUES(?,?,?,?)',
                            (clock, wall_now(), kind, detail))
            self.db.commit()

    def recent_events(self, limit=50):
        with self.lock:
            rows = self.db.execute(
                'SELECT * FROM events ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
            return [dict(r) for r in reversed(rows)]

    # ---- 专题 ----
    def active_topics(self):
        with self.lock:
            return [r['name'] for r in self.db.execute(
                "SELECT name FROM topics WHERE status='active' ORDER BY added_seq").fetchall()]

    def topics_overview(self):
        with self.lock:
            return [dict(r) for r in self.db.execute(
                'SELECT * FROM topics ORDER BY added_seq').fetchall()]

    # ---- 链接与来源关系 ----
    def add_link(self, url, host, grp, kind, via, topics):
        """登记链接与各专题的来源关系；已存在时只补关系，不重置状态。"""
        with self.lock:
            created = self.db.execute(
                'INSERT OR IGNORE INTO links(url,host,grp,kind,state,discovered_seq) '
                'VALUES(?,?,?,?,?,?)',
                (url, host, grp, kind, 'pending', self.next_seq())).rowcount > 0
            for topic in topics:
                self.db.execute(
                    'INSERT OR IGNORE INTO topic_links(topic,url,via) VALUES(?,?,?)',
                    (topic, url, via))
            self.db.commit()
            self.revive_dropped()
            self.drop_orphans()
            return created

    def link(self, url):
        with self.lock:
            return self.db.execute('SELECT * FROM links WHERE url=?', (url,)).fetchone()

    def topics_for_link(self, url):
        with self.lock:
            return [r['topic'] for r in self.db.execute(
                'SELECT topic FROM topic_links WHERE url=?', (url,)).fetchall()]

    def next_candidates(self):
        """待访问链接，按公平性排序：已获访问次数少者优先，同等时新增专题优先，
        再按发现先后。只包含有进行中专题引用的链接。"""
        with self.lock:
            return self.db.execute(
                'SELECT l.url, l.host, l.grp, l.kind,'
                ' MIN(t.granted) AS pr, MAX(t.added_seq) AS nseq, l.discovered_seq AS dseq'
                ' FROM links l JOIN topic_links tl ON tl.url=l.url'
                ' JOIN topics t ON t.name=tl.topic'
                " WHERE l.state='pending' AND t.status='active'"
                ' GROUP BY l.url ORDER BY pr ASC, nseq DESC, dseq ASC').fetchall()

    def has_open_work(self):
        with self.lock:
            return self.db.execute(
                "SELECT 1 FROM links l WHERE l.state IN ('pending','in_flight') AND EXISTS("
                'SELECT 1 FROM topic_links tl JOIN topics t ON t.name=tl.topic'
                " WHERE tl.url=l.url AND t.status='active') LIMIT 1").fetchone() is not None

    def mark_in_flight(self, url):
        """把待访问链接标记为传输中；返回是否成功（并发取消时不应再发请求）。"""
        with self.lock:
            cur = self.db.execute(
                "UPDATE links SET state='in_flight' WHERE url=? AND state='pending'", (url,))
            self.db.commit()
            return cur.rowcount > 0

    def back_to_pending(self, url, detail):
        with self.lock:
            self.db.execute(
                "UPDATE links SET state='pending', detail=? WHERE url=? AND state!='done'",
                (detail, url))
            self.db.commit()

    def reset_in_flight(self):
        """中断恢复：上次传输未登记完成的链接重新排队。"""
        with self.lock:
            cur = self.db.execute(
                "UPDATE links SET state='pending', detail='中断恢复，重新排队' WHERE state='in_flight'")
            self.db.commit()
            return cur.rowcount

    def refuse_robots(self, url, detail):
        with self.lock:
            self.db.execute(
                "UPDATE links SET state='refused_robots', detail=? WHERE url=?", (detail, url))
            self.db.commit()

    def finish_link(self, url, document_id, state, detail=None):
        """访问已发出并登记响应：记录结果，并为引用它的进行中专题计一次访问机会。"""
        with self.lock:
            self.db.execute(
                'UPDATE links SET state=?, document_id=?, detail=? WHERE url=?',
                (state, document_id, detail, url))
            self.db.execute(
                "UPDATE topics SET granted=granted+1 WHERE status='active' AND name IN"
                ' (SELECT topic FROM topic_links WHERE url=?)', (url,))
            self.db.commit()

    def drop_orphans(self):
        """没有进行中专题引用的待访问链接不再促成新访问。"""
        with self.lock:
            cur = self.db.execute(
                "UPDATE links SET state='dropped', detail='无有效专题引用'"
                " WHERE state='pending' AND NOT EXISTS("
                'SELECT 1 FROM topic_links tl JOIN topics t ON t.name=tl.topic'
                " WHERE tl.url=links.url AND t.status='active')")
            self.db.commit()
            return cur.rowcount

    def revive_dropped(self):
        """重新被进行中专题引用的链接恢复排队。"""
        with self.lock:
            cur = self.db.execute(
                "UPDATE links SET state='pending' WHERE state='dropped' AND EXISTS("
                'SELECT 1 FROM topic_links tl JOIN topics t ON t.name=tl.topic'
                " WHERE tl.url=links.url AND t.status='active')")
            self.db.commit()
            return cur.rowcount

    def refused_links(self, state):
        with self.lock:
            return self.db.execute('SELECT * FROM links WHERE state=?', (state,)).fetchall()

    def reset_failed(self):
        with self.lock:
            cur = self.db.execute(
                "UPDATE links SET state='pending', detail='人工复查后重试' WHERE state='failed'")
            self.db.commit()
            return cur.rowcount

    # ---- 额度台账（与来源滚动窗口同口径：产品访问含被拒的都计入） ----
    def try_reserve(self, grp, host, url, clock, maximum, window):
        """原子检查并登记一次访问额度。

        返回 (请求ID, None) 表示已放行并记账；(None, 可用时钟) 表示需等待。
        先记账再发请求，多个采集进程共用同一数据库也不会一起超额。
        """
        with self.lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                rows = self.db.execute(
                    'SELECT clock FROM requests WHERE grp=? AND clock>? ORDER BY clock',
                    (grp, clock - window)).fetchall()
                if len(rows) >= maximum:
                    self.db.execute('ROLLBACK')
                    return None, rows[len(rows) - maximum]['clock'] + window
                cur = self.db.execute(
                    'INSERT INTO requests(grp,host,url,clock,status,wall) VALUES(?,?,?,?,NULL,?)',
                    (grp, host, url, clock, wall_now()))
                req_id = cur.lastrowid
                self.db.execute('COMMIT')
                return req_id, None
            except Exception:
                self.db.execute('ROLLBACK')
                raise

    def record_status(self, req_id, status):
        with self.lock:
            self.db.execute('UPDATE requests SET status=? WHERE id=?', (status, req_id))
            self.db.commit()

    def group_wait(self, grp, clock, maximum, window):
        """该站点组下一个可用窗口的时钟；None 表示现在可发。"""
        with self.lock:
            rows = self.db.execute(
                'SELECT clock FROM requests WHERE grp=? AND clock>? ORDER BY clock',
                (grp, clock - window)).fetchall()
            if len(rows) >= maximum:
                return rows[len(rows) - maximum]['clock'] + window
            return None

    def ledger(self):
        with self.lock:
            return [dict(r) for r in self.db.execute('SELECT * FROM requests ORDER BY id').fetchall()]

    def clear_ledger(self):
        with self.lock:
            self.db.execute('DELETE FROM requests')
            self.db.commit()

    # ---- 站点组冷却（Retry-After 对共享访问身份整体生效） ----
    def cooldowns(self):
        with self.lock:
            return {r['grp']: dict(r) for r in self.db.execute('SELECT * FROM cooldowns').fetchall()}

    def set_cooldown(self, grp, until_clock, reason, clock):
        with self.lock:
            self.db.execute(
                'INSERT INTO cooldowns(grp,until_clock,reason,set_clock) VALUES(?,?,?,?) '
                'ON CONFLICT(grp) DO UPDATE SET until_clock=excluded.until_clock,'
                ' reason=excluded.reason, set_clock=excluded.set_clock',
                (grp, until_clock, reason, clock))
            self.db.commit()

    def clear_cooldowns(self):
        with self.lock:
            self.db.execute('DELETE FROM cooldowns')
            self.db.commit()

    # ---- robots 缓存（按来源时钟校验，时钟推进后重新确认政策） ----
    def get_robots(self, host, clock):
        with self.lock:
            row = self.db.execute(
                'SELECT disallow FROM robots WHERE host=? AND fetched_clock=?',
                (host, clock)).fetchone()
            return json.loads(row['disallow']) if row else None

    def put_robots(self, host, disallows, clock):
        with self.lock:
            self.db.execute(
                'INSERT INTO robots(host,disallow,fetched_clock) VALUES(?,?,?) '
                'ON CONFLICT(host) DO UPDATE SET disallow=excluded.disallow,'
                ' fetched_clock=excluded.fetched_clock',
                (host, json.dumps(disallows), clock))
            self.db.commit()

    def clear_robots(self):
        with self.lock:
            self.db.execute('DELETE FROM robots')
            self.db.commit()

    # ---- 原件登记 ----
    def document_by_url(self, url):
        with self.lock:
            return self.db.execute('SELECT * FROM documents WHERE url=?', (url,)).fetchone()

    def insert_document(self, meta, sha256, size):
        with self.lock:
            cur = self.db.execute(
                'INSERT INTO documents(url,kind,status,headers,sha256,size,path,'
                ' fetched_clock,fetched_wall) VALUES(?,?,?,?,?,?,NULL,?,?)',
                (meta['url'], meta['kind'], meta['status'],
                 json.dumps(meta['headers'], ensure_ascii=False),
                 sha256, size, meta['clock'], meta['wall']))
            self.db.commit()
            return cur.lastrowid

    def update_document_path(self, doc_id, path):
        with self.lock:
            self.db.execute('UPDATE documents SET path=? WHERE id=?', (path, doc_id))
            self.db.commit()

    def documents(self):
        with self.lock:
            return [dict(r) for r in self.db.execute('SELECT * FROM documents ORDER BY id').fetchall()]
