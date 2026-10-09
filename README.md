# 项目 1：基于 Flink + Kafka 的实时电商流量治理与数仓 ODS 层建设
**Project 1: Real-time E-commerce Traffic Governance & ODS Construction**

## 项目概述
本项目属于【第一组：实时流计算与数仓建设（大数据开发核心）】的实战模块。核心目标是解决双 11 等大促场景下，APP 埋点日志乱序、脏数据频发导致的 BI 看板数据漂移问题。通过构建实时清洗层，将杂乱无章的用户行为转化为结构化的 ODS 层数据，确保数据时间的准确性与流式写入的“精确一次”语义。

## 关联课程
- 《大数据平台技术》
- 《计算机网络》
- 《数据库原理》

## 业务故事
双 11 大促期间，高并发请求导致 APP 埋点日志出现严重乱序与脏数据，下游 BI 看板发生数据漂移。亟需构建一套基于 Flink 的实时流量治理体系，完成数据清洗、乱序处理、精准去重，并最终对接数据湖格式实现稳定入仓。

## 技术栈
- **流计算**：Apache Flink, Kafka
- **存储与数仓**：MySQL, Hudi, Iceberg, RocksDB
- **监控与运维**：Prometheus, Grafana
- **核心能力**：Watermark 乱序处理、CDC 实时入湖、数据血缘、数据质量监控

## 迭代计划

### V1.0：基础接入与清洗
使用 Java + Flink 读取 Kafka，完成简单的 JSON 解析和字段过滤，清洗后写入 MySQL 供临时查询。
- **核心**：Kafka Source -> JSON Parsing -> Filter -> MySQL Sink

### V2.0：乱序处理与精准去重
引入 Watermark 处理乱序数据，实现迟到数据的侧输出流；利用 Flink 状态后端（RocksDB）进行精准去重。
- **核心**：Watermark -> Side Output -> RocksDB Stateful Deduplication

### V3.0：湖格式入仓与优化
对接 Hudi/Iceberg 湖格式，实现 CDC 实时入湖，保证流式写入的“精确一次”语义，并解决小文件合并导致的 NameNode 压力问题。
- **核心**：Hudi/Iceberg -> CDC -> Exactly-Once -> Small File Merge

## 交付物清单
- **Flink SQL 作业源码**：覆盖 V1.0 - V3.0 迭代逻辑
- **数据血缘关系图**：全链路数据流向与依赖可视化
- **数据质量监控 Dashboard**：基于 Prometheus + Grafana 的实时监控配置

## 目录结构（建议）
