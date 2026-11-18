"""本地合成资料来源，仅提供来源行为与观察记录。"""
import argparse
from contextlib import contextmanager
import gzip
import hashlib
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
from pathlib import Path
import threading
from urllib.parse import parse_qs, urlsplit

DATA = Path(__file__).resolve().parents[1] / 'data'

class Source:
    def __init__(self):
        self.lock = threading.RLock()
        self.clock = 0
        self.phase = 0
        self.log = []
        self.extra = {}

    def response(self, method, raw_path, headers, body):
        path = urlsplit(raw_path).path
        q = {k:v[-1] for k,v in parse_qs(urlsplit(raw_path).query).items()}
        with self.lock:
            self.log.append({'method':method,'path':raw_path,'time':self.clock,
                             'headers':{k.lower():v for k,v in headers.items()}})
            if path == '/health': return 200, {}, {'来源':'本地合成资料','phase':self.phase,'time':self.clock}
            if path == '/__admin/log': return 200, {}, self.log[:-1]
            if path == '/__admin/advance' and method == 'POST':
                value=json.loads(body or b'{}')
                self.clock += int(value.get('seconds',0))
                self.phase = int(value.get('phase',self.phase))
                return 200, {}, {'phase':self.phase,'time':self.clock}
            if path.startswith('/__admin/'):
                return 404, {}, {'error':'未知演练控制'}
            return self.product(method,path,q,{k.lower():v for k,v in headers.items()},body)

    def product(self,method,path,q,h,body):
        host=h.get('host','').split(':')[0]
        group='shared-city' if host in ('127.0.0.1','localhost') else host
        if path=='/policies':
            return 200,{}, {'groups':{'shared-city':{'origins':['127.0.0.1','localhost'],'maximum':2,'window_seconds':10},'files':{'origins':['127.0.0.2'],'maximum':1,'window_seconds':5}},'clock':self.clock}
        if path=='/robots.txt':
            return 200,{'Content-Type':'text/plain'},'User-agent: *\nDisallow: '+('/closed/' if self.phase<1 else '/files/')+'\n'
        prior=[x for x in self.log[:-1] if x['path'].startswith(('/topics/','/files/','/redirect/')) and x['headers'].get('host','').split(':')[0] in ('127.0.0.1','localhost') and x['time']>self.clock-10]
        if group=='shared-city' and len(prior)>=2:
            return 429,{'Retry-After':str(max(1,10-(self.clock-prior[0]['time'])))},{'error':'站点组冷却'}
        if path.startswith('/topics/'):
            t=path.rsplit('/',1)[-1]
            return 200,{},f'<main><h1>专题 {html.escape(t)}</h1><a rel="document" href="/files/shared">共同原件</a><a rel="document" href="/redirect/{html.escape(t)}">专题原件</a></main>'
        if path.startswith('/redirect/'):
            port=h['host'].split(':')[-1]
            return 302,{'Location':f'http://127.0.0.2:{port}/files/'+path.rsplit('/',1)[-1]},b''
        if path.startswith('/files/'):
            return 200,{'Content-Type':'text/plain; charset=utf-8','X-Document-Id':path.rsplit('/',1)[-1]},('合成公开原件 '+path).encode()
        return 404,{}, {'error':'无此来源'}

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self,*args): pass
    def do_GET(self): self.handle_request()
    def do_POST(self): self.handle_request()
    def handle_request(self):
        size=int(self.headers.get('Content-Length','0'))
        try:
            status,headers,body=self.server.source.response(self.command,self.path,self.headers,self.rfile.read(size))
            if not isinstance(body,(str,bytes)):
                body=json.dumps(body,ensure_ascii=False).encode();headers.setdefault('Content-Type','application/json; charset=utf-8')
            elif isinstance(body,str):
                body=body.encode();headers.setdefault('Content-Type','text/html; charset=utf-8')
        except (ValueError,KeyError,TypeError) as exc:
            status,headers,body=400,{'Content-Type':'application/json'},json.dumps({'error':str(exc)},ensure_ascii=False).encode()
        if status==0:
            self.close_connection=True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        self.send_response(status)
        for k,v in headers.items():self.send_header(k,str(v))
        self.send_header('Content-Length',len(body))
        self.end_headers()
        if body:self.wfile.write(body)

@contextmanager
def running_source(port=0):
    server=ThreadingHTTPServer(('127.0.0.1',port),Handler)
    server.source=Source()
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try: yield 'http://127.0.0.1:'+str(server.server_port),server.source
    finally: server.shutdown();server.server_close();thread.join()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description='启动本地资料来源')
    parser.add_argument('--port',type=int,default=8765)
    args=parser.parse_args()
    server=ThreadingHTTPServer(('0.0.0.0',args.port),Handler);server.source=Source()
    print('资料来源已启动，端口 '+str(args.port),flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:server.server_close()
