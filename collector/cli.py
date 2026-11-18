"""命令行入口：python -m collector <命令>。

资料组常用流程：
  python -m collector add 滨江 临港        # 新增城市专题
  python -m collector serve                # 运行采集服务（可持续运行）
  python -m collector status               # 查看各专题收到了什么、还缺什么
  python -m collector report               # 生成原件目录与专题状态报告
  python -m collector cancel 滨江          # 取消专题（不再促成新访问）
"""
import argparse
import sys

from . import report
from .service import Service, add_topic, cancel_topic
from .store import Store


def build_parser():
    parser = argparse.ArgumentParser(
        prog='collector', description='低碳公开资料采集服务')
    parser.add_argument('--state-dir', default='collector-state',
                        help='状态目录（默认 ./collector-state），中断后用同一目录继续')
    parser.add_argument('--source', default='http://127.0.0.1:8765',
                        help='资料来源地址（默认 http://127.0.0.1:8765）')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p = sub.add_parser('serve', help='运行采集服务（持续调度）')
    p.add_argument('--poll', type=float, default=0.5, help='空闲轮询间隔秒数（默认 0.5）')
    p.add_argument('--max-idle', type=int, default=0,
                   help='连续空闲轮数达到后退出（默认 0，不退出）')

    p = sub.add_parser('add', help='新增城市专题')
    p.add_argument('names', nargs='+', help='专题名（城市）')

    p = sub.add_parser('cancel', help='取消城市专题')
    p.add_argument('name', help='专题名')

    p = sub.add_parser('status', help='查看专题收集状态')
    p.add_argument('name', nargs='?', help='只看某个专题')

    sub.add_parser('report', help='生成原件目录与专题状态报告（写入状态目录 report/）')

    p = sub.add_parser('retry-refused', help='复查被来源拒绝的链接')
    p.add_argument('--include-failed', action='store_true', help='同时重试 HTTP 失败的链接')

    sub.add_parser('refresh-robots', help='清除 robots 缓存，访问前重新确认政策')
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)

    if args.cmd in ('add', 'cancel', 'status', 'report', 'refresh-robots'):
        store = Store(args.state_dir)
        try:
            if args.cmd == 'add':
                for name in args.names:
                    result = add_topic(store, name)
                    note = '（重新启用，恢复 %d 条链接）' % result['revived'] \
                        if result['reactivated'] else ''
                    print('已登记专题「%s」%s，服务运行时将沿授权入口发现文件。' % (name, note))
            elif args.cmd == 'cancel':
                result = cancel_topic(store, args.name)
                if not result['found']:
                    print('没有专题「%s」。' % args.name)
                elif result.get('already'):
                    print('专题「%s」此前已取消。' % args.name)
                else:
                    print('专题「%s」已取消：不再促成新访问。' % args.name)
                    print('  待访问链接停止促成 %d 条；传输中 %d 条仍将继续完成'
                          '（不当作已取消）；已取得 %d 份保留。'
                          % (result['dropped'], result['in_flight'], result['received']))
            elif args.cmd == 'status':
                print(report.format_status(store, only=args.name))
            elif args.cmd == 'report':
                catalog_path, topics_path = report.write_reports(store)
                print(report.format_status(store))
                print('\n原件目录：%s\n专题状态：%s' % (catalog_path, topics_path))
            elif args.cmd == 'refresh-robots':
                store.clear_robots()
                print('robots 缓存已清除，下次访问前将重新确认政策。')
        finally:
            store.close()
        return 0

    if args.cmd in ('serve', 'retry-refused'):
        try:
            service = Service(args.state_dir, args.source,
                              poll=getattr(args, 'poll', 0.5))
        except OSError as exc:
            print('无法连接资料来源 %s：%s' % (args.source, exc), file=sys.stderr)
            return 2
        try:
            if args.cmd == 'serve':
                print('采集服务已启动，状态目录 %s，来源 %s。' % (args.state_dir, args.source))
                service.serve(max_idle=args.max_idle or None)
            else:
                try:
                    resumed, failed = service.retry_refused(
                        include_failed=args.include_failed)
                except OSError as exc:
                    print('复查时来源不可达：%s' % exc, file=sys.stderr)
                    return 2
                print('复查完成：robots 拒绝恢复 %d 条，失败重试 %d 条。' % (resumed, failed))
        finally:
            service.store.close()
        return 0
    return 1


if __name__ == '__main__':
    sys.exit(main())
