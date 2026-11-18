"""robots.txt 解析与路径匹配。

只处理来源契约需要的部分：User-agent: * 下的 Disallow 前缀。
robots 是对来源政策的遵守，是否允许访问必须在发出请求之前判断，
不能先发出请求再补记拒绝。
"""


def parse_disallows(text):
    """从 robots.txt 文本中取出 User-agent: * 的 Disallow 前缀列表。"""
    disallows = []
    applies = False
    for raw in text.splitlines():
        line = raw.split('#', 1)[0].strip()
        if not line or ':' not in line:
            continue
        key, _, value = line.partition(':')
        key = key.strip().lower()
        value = value.strip()
        if key == 'user-agent':
            applies = value == '*'
        elif key == 'disallow' and applies and value:
            disallows.append(value)
    return disallows


def is_disallowed(disallows, path):
    """路径是否命中任一 Disallow 前缀。"""
    return any(path.startswith(prefix) for prefix in disallows)
