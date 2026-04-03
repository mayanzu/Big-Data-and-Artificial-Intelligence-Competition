# 大数据与人工智能竞赛 - 数据模拟练习平台

## 项目简介

本项目包含两大部分：

1. **竞赛手册** (`docs/`) — 理论知识和实践指南
2. **大数据模拟练习平台** (`platform/`) — 基于 Flask + SocketIO 的交互式数据练习系统

## 大数据模拟练习平台

### 功能特性

- 🔗 **REST API** — 提供清洗后数据集、脏数据、关联查询等接口
- 📊 **仪表板** — 浏览器端可视化数据查询与操作
- 🔄 **实时数据流** — Server-Sent Events 推送时间序列数据
- 📤 **数据导出** — 支持 CSV、JSON、SQL、Hive DDL 导出
- 🛡️ **SQLite 本地存储** — 无需安装数据库即可练习

### 技术栈

- **Flask** — Web 框架
- **Flask-SocketIO** — 实时通信
- **SQLite** — 嵌入式数据库
- **Pandas** — 数据处理
- **eventlet** — 异步支持
- **前端** — HTML + Chart.js + 原生 JS

### 快速启动

```bash
# 安装依赖
pip install flask flask-socketio pandas eventlet

# 启动服务
python app.py

# 访问仪表板
# http://127.0.0.1:8889
```

### API 接口

| 端点 | 说明 |
|------|------|
| `GET /api/data?n=100` | 获取 n 条清洗后的电商数据 |
| `GET /api/dirty?n=100` | 获取 n 条包含脏数据的记录 |
| `GET /api/join?n=50` | 获取 n 条用户-订单关联数据 |
| `GET /api/timeseries?days=7` | 获取最近 n 天的销售时间序列 |
| `GET /api/export/csv?n=1000` | 导出 n 条 CSV 数据 |
| `GET /api/export/json?n=1000` | 导出 n 条 JSON 数据 |
| `GET /api/export/sql?n=1000` | 导出 n 条 SQL INSERT 语句 |
| `GET /api/hive/ddl` | 生成 Hive 建表语句 |
| `GET /` | 仪表板仪表盘 |

### 数据模型

- **orders** — 订单表 (orderId, customerId, date, amount, status)
- **customers** — 用户表 (customerId, name, email, age, gender, region)
- **products** — 商品表 (productId, name, category, price, stock)
