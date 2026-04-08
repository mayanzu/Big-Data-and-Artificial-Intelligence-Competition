#!/bin/bash
# 大数据仿真平台 - 部署脚本

echo "=== 大数据仿真平台部署脚本 ==="

# 1. 创建日志目录
echo "[1/5] 创建日志目录..."
sudo mkdir -p /var/log
sudo touch /var/log/bigdata-simulator.log /var/log/bigdata-simulator-error.log /var/log/bigdata-simulator-out.log
sudo chmod 644 /var/log/bigdata-simulator*.log 2>/dev/null || true

# 2. 安装依赖（如果缺失）
echo "[2/5] 检查依赖..."
python3 -c "import flask" 2>/dev/null || pip3 install flask
python3 -c "import flask_sqlalchemy" 2>/dev/null || pip3 install flask-sqlalchemy
python3 -c "import flask_socketio" 2>/dev/null || pip3 install flask-socketio
python3 -c "import gevent" 2>/dev/null || pip3 install gevent
python3 -c "import eventlet" 2>/dev/null || pip3 install eventlet

# 3. 初始化数据库（如果不存在）
echo "[3/5] 初始化数据库..."
cd /opt/bigdata-simulator
if [ ! -f "instance/ecommerce.db" ]; then
    echo "首次启动，生成种子数据..."
    timeout 30 python3 app_optimized.py || true
else
    echo "数据库已存在，跳过初始化"
fi

# 4. 使用 PM2 启动
echo "[4/5] 使用 PM2 启动服务..."
cd /opt/bigdata-simulator
pm2 start ecosystem.config.js
pm2 save

# 5. 设置开机自启
echo "[5/5] 设置 PM2 开机自启..."
pm2 startup 2>/dev/null || true

echo ""
echo "✅ 部署完成！"
echo "访问地址: http://163.7.1.176:8889/"
echo "查看日志: pm2 logs bigdata-simulator"
echo "停止服务: pm2 stop bigdata-simulator"
