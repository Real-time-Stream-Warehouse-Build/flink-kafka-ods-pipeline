# Real-time-stream-processing-and-data-warehouse-development
基于 Flink/Kafka/HBase 的实时流计算与数仓建设核心实战
# 实时流计算与数仓建设 (Real-time Stream Computing and Data Warehouse Construction)

## 仓库简介
本仓库聚焦于**大数据开发核心**，涵盖实时流计算、数据湖入仓、智能风控预测、推荐系统召回及底层存储调度。通过四个递进式实战项目，呈现从数据清洗（ODS层）到业务应用（预测/推荐）再到集群自优化的全链路能力。

## 核心项目列表

### 项目 1：基于 Flink + Kafka 的实时电商流量治理与数仓 ODS 层建设
- **业务背景**：双11大促埋点乱序/脏数据频发，需构建实时清洗层，保障 BI 看板准确。
- **核心技术**：Java, Flink, Kafka, Watermark, RocksDB 状态后端, Hudi/Iceberg, CDC。
- **迭代亮点**：JSON解析过滤 -> 乱序处理与精准去重 -> 湖格式实时入仓（精确一次语义，解决小文件问题）。
- **交付物**：Flink SQL 源码、数据血缘图、Prometheus+Grafana 监控 Dashboard。

### 项目 2：美团风格——智能配送订单的“超时风险”实时预测与熔断
- **业务背景**：恶劣天气运力紧张，基于 GPS/路况提前 10 分钟预测超时并触发改派/赔付。
- **核心技术**：Python, Pandas, XGBoost/LightGBM, Flink CEP, Redis, A/B Test。
- **迭代亮点**：离线静态阈值 -> 在线推理与复杂事件处理(CEP) -> 特征 Pipeline 化与秒级决策。
- **交付物**：特征存储设计、离线训练 Notebook、实时打分混合服务。

### 项目 3：B站/抖音风格——“冷启动”视频的实时推荐召回池构建
- **业务背景**：新视频无交互历史，利用多模态信息（标题/OCR/音频）构建内容召回向量池。
- **核心技术**：PaddleNLP, Faiss, Flink, 向量检索, 近线(Near-line)计算。
- **迭代亮点**：暴力全量检索 -> 实时索引更新与混合过滤 -> 解决读写锁冲突，毫秒级(<50ms)召回。
- **交付物**：gRPC 向量召回接口、索引自动更新脚本、全链路压测报告(QPS>5000)。

### 项目 4：万亿级 HBase 集群的“Region 热点”自动感知与分裂预调度
- **业务背景**：IoT 百万设备接入，Rowkey 设计不合理导致 RegionServer 热点与 Full GC。
- **核心技术**：HBase, Redis, Coprocessor, Salt 盐值预分区, OS 负载感知。
- **迭代亮点**：人工监控手动 Split -> 盐值预分区打散 -> 自适应负载感知调度与故障自愈。
- **交付物**：HBase 辅助运维工具包、Rowkey 设计白皮书、混沌工程演练报告。

## 技术栈总览
- **流计算**：Apache Flink, Kafka, Flink CEP
- **存储与数仓**：HBase, MySQL, Redis, Hudi, Iceberg, Faiss
- **算法与机器学习**：XGBoost, LightGBM, PaddleNLP, 向量检索
- **监控与运维**：Prometheus, Grafana, RocksDB, Linux OS 指标
