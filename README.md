# 低碳公开资料的跨站访问调度与覆盖交付

这是围绕城市低碳公开资料的本地研发项目。当前内容提供授权合成来源、资料、来源契约，以及按契约调度访问的采集服务（collector）。

## 运行来源

使用 Python 3.11 或 3.12，先执行 python -m pip install -r requirements.txt。

启动命令：python -m source.server --port 8765。浏览器或 HTTP 客户端访问 http://127.0.0.1:8765。接口和资料定义见 docs/source-contract.md。

## 运行采集服务

```bash
python -m collector add 滨江 临港     # 登记城市专题
python -m collector serve             # 持续采集（状态目录默认 ./collector-state）
python -m collector status            # 各专题收到了什么、还缺什么
python -m collector report            # 生成原件目录与专题状态（collector-state/report/）
```

演练时钟由来源控制接口推进：curl -XPOST http://127.0.0.1:8765/__admin/advance -d '{"seconds":10}'。采集调度规则、状态分类与中断恢复见 docs/collector-guide.md。

## 检查来源与采集端

执行 python -m unittest discover -s tests -v。tests/test_source.py 核对来源行为；tests/test_collector.py 用来源的可回查记录验证采集端的调度与恢复。

## 容器

执行 docker build -t civic-crawl-budget .，然后 docker run --rm -p 8765:8765 civic-crawl-budget。测试可用 docker run --rm civic-crawl-budget python -m unittest discover -s tests -v。

source 提供来源服务，collector 是采集端，data 保存合成原始资料，tests 核对来源与采集端行为；docs 记录字段与交互契约。不要将运行日志、凭据或采集数据（如 collector-state/）提交到版本库。
