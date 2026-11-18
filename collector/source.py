"""来源访问客户端：只负责 HTTP 与来源契约接口，不做调度判断。

调度（额度、冷却、robots、公平性）全部在 service.py；
这里发出的每个产品请求都假定已经过调度器放行。
"""
import http.client
import json
from urllib.parse import urlsplit


class SourceError(Exception):
    """来源契约接口不可用。"""


class SourceClient:
    def __init__(self, base, timeout=10):
        self.base = base.rstrip('/')
        self.timeout = timeout

    def _request(self, method, url, body=None, headers=None):
        parts = urlsplit(url)
        conn = http.client.HTTPConnection(parts.hostname, parts.port or 80, timeout=self.timeout)
        try:
            path = parts.path or '/'
            if parts.query:
                path += '?' + parts.query
            conn.request(method, path, body=body, headers=headers or {})
            resp = conn.getresponse()
            data = resp.read()
            return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data
        finally:
            conn.close()

    def get(self, url):
        """GET 任意地址；不自动跟随重定向，转址由调度器按新地址规则决定。"""
        return self._request('GET', url)

    def get_json(self, path):
        status, _, data = self._request('GET', self.base + path)
        if status >= 400:
            raise SourceError('%s -> HTTP %d' % (path, status))
        return json.loads(data.decode('utf-8'))

    def health(self):
        """来源时间与阶段（不扣额度）。"""
        return self.get_json('/health')

    def policies(self):
        """站点组额度定义（不扣额度）。"""
        return self.get_json('/policies')

    def robots_text(self, url):
        """取 url 所在来源的 robots.txt（不扣额度）。"""
        parts = urlsplit(url)
        root = '%s://%s' % (parts.scheme or 'http', parts.netloc)
        status, _, data = self._request('GET', root + '/robots.txt')
        return data.decode('utf-8', 'replace') if status == 200 else ''
