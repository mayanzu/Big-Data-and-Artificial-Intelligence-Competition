#!/usr/bin/env python3
"""电商数据仿真平台 - 稳定性优化版"""
import json, random, time, threading, os, datetime, sqlite3, hashlib, logging
from flask import Flask, jsonify, request, render_template, Response, stream_with_context
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO
from geventwebsocket import WebSocketError

# ===== 日志配置 =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/bigdata-simulator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'bigdata-simulator-2026'

# SQLite 优化配置
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///ecommerce.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_pre_ping': True,  # 连接前检查
    'pool_recycle': 3600,   # 1小时回收连接
}

db = SQLAlchemy(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent', ping_timeout=60, ping_interval=25)

# ===== 数据库模型（保持不变） =====
class UserBehavior(db.Model):
    __tablename__ = 'user_behavior'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.String(30), nullable=False, index=True)
    user_id = db.Column(db.String(20), nullable=False, index=True)
    event_type = db.Column(db.String(20), nullable=False, index=True)
    session_id = db.Column(db.String(36))
    device = db.Column(db.String(20))
    ip = db.Column(db.String(20))
    product_id = db.Column(db.String(20), index=True)
    category = db.Column(db.String(50), index=True)
    price = db.Column(db.Float)
    page_source = db.Column(db.String(50))
    stay_time = db.Column(db.Integer)

    def to_dict(self):
        return {
            'id': self.id if self.id else 0,
            'timestamp': self.timestamp,
            'user_id': self.user_id,
            'event_type': self.event_type,
            'session_id': self.session_id,
            'device': self.device,
            'ip': self.ip,
            'product_id': self.product_id,
            'category': self.category,
            'price': self.price,
            'page_source': self.page_source,
            'stay_time': self.stay_time
        }

class UserProfile(db.Model):
    __tablename__ = 'user_profiles'
    user_id = db.Column(db.String(20), primary_key=True)
    name = db.Column(db.String(50))
    age = db.Column(db.Integer)
    gender = db.Column(db.String(10))
    city = db.Column(db.String(50))
    register_date = db.Column(db.String(20))
    member_level = db.Column(db.String(20))
    total_orders = db.Column(db.Integer, default=0)
    total_spent = db.Column(db.Float, default=0)

class Product(db.Model):
    __tablename__ = 'products'
    product_id = db.Column(db.String(20), primary_key=True)
    name = db.Column(db.String(200))
    category = db.Column(db.String(50), index=True)
    price = db.Column(db.Float)
    stock = db.Column(db.Integer)
    sales = db.Column(db.Integer, default=0)
    rating = db.Column(db.Float)
    created_at = db.Column(db.String(30))

# ===== 数据生成器（保持不变） =====
EVENT_TYPES = ['view', 'click', 'add_cart', 'buy', 'search', 'collect', 'share']
EVENT_WEIGHTS = [40, 25, 15, 8, 10, 7, 5]
DEVICES = ['iOS', 'Android', 'PC', '小程序', 'H5']
CATEGORIES = [
    '手机数码', '电脑办公', '家用电器', '服饰鞋包', '美妆护肤',
    '食品饮料', '母婴用品', '图书音像', '运动户外', '家居家装',
    '汽车用品', '珠宝首饰', '宠物用品', '医药保健', '箱包皮具'
]
PAGE_SOURCES = ['首页', '搜索', '推荐', '活动页', '分类页', '收藏夹', '购物车', '订单页']
CITIES = ['北京', '上海', '广州', '深圳', '杭州', '成都', '武汉', '南京', '重庆', '西安',
          '长沙', '天津', '郑州', '苏州', '青岛', '大连', '宁波', '东莞', '佛山', '合肥']
FAMILY_NAMES = ['张', '王', '李', '赵', '刘', '陈', '杨', '黄', '周', '吴', '徐', '孙', '马', '朱', '胡', '郭', '何', '高', '林', '罗']
GIVEN_NAMES = ['伟', '芳', '娜', '敏', '静', '丽', '强', '磊', '军', '洋', '勇', '艳', '杰', '涛', '明', '超', '秀英', '华', '慧', '建']

generated_count = 0
lock = threading.Lock()

def random_ip():
    return f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,255)}"

def random_user_id():
    return f"U{random.randint(1, 1000):05d}"

def random_product_id():
    return f"P{random.randint(1000, 9999):04d}"

def random_name():
    return random.choice(FAMILY_NAMES) + random.choice(GIVEN_NAMES)

def random_timestamp():
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    return (now - delta).strftime('%Y-%m-%d %H:%M:%S')

def generate_behavior():
    user_id = random_user_id()
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    product_id = random_product_id()
    category = random.choice(CATEGORIES)
    price = round(random.uniform(10, 5000), 2)

    behavior = UserBehavior(
        timestamp=random_timestamp(),
        user_id=user_id,
        event_type=event_type,
        session_id=os.urandom(16).hex(),
        device=random.choice(DEVICES),
        ip=random_ip(),
        product_id=product_id,
        category=category,
        price=price if event_type in ['add_cart', 'buy'] else None,
        page_source=random.choice(PAGE_SOURCES),
        stay_time=random.randint(5, 300)
    )
    return behavior

def background_generator():
    """优化的后台数据生成：每2秒生成1条（降低频率）"""
    global generated_count
    logger.info("Background generator started (2s interval)")
    while True:
        try:
            behavior = generate_behavior()
            with app.app_context():
                db.session.add(behavior)
                db.session.commit()
                data = behavior.to_dict()
                socketio.emit('new_event', data)
                with lock:
                    generated_count += 1
                if generated_count % 100 == 0:
                    logger.info(f"Generated {generated_count} records")
        except Exception as e:
            logger.error(f"Background generator error: {e}")
            db.session.rollback()
        time.sleep(2)  # 原为1秒，改为2秒

def seed_data(n=1000):
    """初始数据种子"""
    behaviors = []
    for _ in range(n):
        behaviors.append(generate_behavior())
    db.session.bulk_insert_objects(behaviors)
    db.session.commit()
    logger.info(f"Seeded {n} records")

# ===== API 路由（保持不变） =====
@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/data')
def api_data():
    n = min(int(request.args.get('n', 100)), 10000)
    behaviors = UserBehavior.query.order_by(UserBehavior.id.desc()).limit(n).all()
    return jsonify([b.to_dict() for b in behaviors])

@app.route('/api/dirty')
def api_dirty():
    n = min(int(request.args.get('n', 100)), 1000)
    noise_types = [
        lambda d: d.update({'user_id': d.get('user_id', '') + '  '}),
        lambda d: d.update({'timestamp': d.get('timestamp', '').replace('-', '/')}),
        lambda d: d.update({'price': round(d.get('price', 0) * random.uniform(0.1, 10), 2)}),
        lambda d: d.update({'category': d.get('category', '') + '　'}),
        lambda d: d.update({'device': d.get('device', '').upper()}),
    ]
    results = []
    behaviors = UserBehavior.query.order_by(db.func.random()).limit(n*2).all()
    for b in behaviors[:n]:
        d = b.to_dict()
        if random.random() < 0.5:
            fn = random.choice(noise_types)
            fn(d)
        results.append(d)
    for _ in range(int(n * 0.2)):
        if results: results.append(results[-1].copy())
    random.shuffle(results)
    return jsonify(results)

@app.route('/api/join')
def api_join():
    n = min(int(request.args.get('n', 50)), 1000)
    behaviors = UserBehavior.query.filter_by(event_type='buy').order_by(db.func.random()).limit(n).all()
    result = []
    for b in behaviors:
        user = UserProfile.query.get(b.user_id)
        if user:
            result.append({
                'order_id': b.product_id,
                'customer_name': user.name,
                'city': user.city,
                'category': b.category,
                'amount': b.price,
                'timestamp': b.timestamp
            })
    return jsonify(result)

@app.route('/api/timeseries')
def api_timeseries():
    days = min(int(request.args.get('days', 7)), 30)
    data = []
    for i in range(days):
        date = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%m-%d')
        sales = random.randint(10000, 50000)
        orders = random.randint(500, 2000)
        data.append({'date': date, 'sales': sales, 'orders': orders})
    return jsonify(list(reversed(data)))

@app.route('/api/export/csv')
def api_export_csv():
    n = min(int(request.args.get('n', 1000)), 50000)
    behaviors = UserBehavior.query.order_by(UserBehavior.id.desc()).limit(n).all()
    output = ['id,timestamp,user_id,event_type,device,ip,category,price']
    for b in behaviors:
        output.append(f"{b.id},{b.timestamp},{b.user_id},{b.event_type},{b.device},{b.ip},{b.category},{b.price or ''}")
    return Response('\n'.join(output), mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=data.csv'})

@app.route('/api/export/json')
def api_export_json():
    n = min(int(request.args.get('n', 1000)), 50000)
    behaviors = UserBehavior.query.order_by(UserBehavior.id.desc()).limit(n).all()
    return Response(json.dumps([b.to_dict() for b in behaviors], ensure_ascii=False), mimetype='application/json')

@app.route('/api/export/sql')
def api_export_sql():
    n = min(int(request.args.get('n', 1000)), 10000)
    behaviors = UserBehavior.query.order_by(UserBehavior.id.desc()).limit(n).all()
    lines = ['-- UserBehavior table data']
    for b in behaviors:
        lines.append(f"""INSERT INTO user_behavior (timestamp, user_id, event_type, session_id, device, ip, product_id, category, price, page_source, stay_time) VALUES ('{b.timestamp}', '{b.user_id}', '{b.event_type}', '{b.session_id}', '{b.device}', '{b.ip}', '{b.product_id}', '{b.category}', {b.price or 'NULL'}, '{b.page_source}', {b.stay_time});""")
    return Response('\n'.join(lines), mimetype='text/plain', headers={'Content-Disposition': 'attachment; filename=data.sql'})

@app.route('/api/hive/ddl')
def api_hive_ddl():
    table = request.args.get('table', 'user_behavior')
    return jsonify({
        'create_table': f"""CREATE EXTERNAL TABLE {table} (
    timestamp STRING,
    user_id STRING,
    event_type STRING,
    session_id STRING,
    device STRING,
    ip STRING,
    product_id STRING,
    category STRING,
    price DOUBLE,
    page_source STRING,
    stay_time INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/{table}';""",
        'sample_query': f"SELECT event_type, COUNT(*) FROM {table} GROUP BY event_type;"
    })

@app.route('/status')
def status():
    total = UserBehavior.query.count()
    return jsonify({
        'status': 'healthy',
        'total_records': total,
        'generated_this_session': generated_count,
        'uptime': 'N/A'
    })

# ===== 前端仪表板所需 API =====
@app.route('/api/stats')
def api_stats():
    """统计摘要（仪表板用）"""
    total = UserBehavior.query.count()
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    today_count = UserBehavior.query.filter(
        UserBehavior.timestamp.like(f'{today}%')
    ).count()

    # 事件类型统计
    event_stats = {}
    for event_type in EVENT_TYPES:
        count = UserBehavior.query.filter_by(event_type=event_type).count()
        event_stats[event_type] = count

    # 转化率 = 购买数 / 点击数（简化版）
    buy_count = event_stats.get('buy', 0)
    click_count = event_stats.get('click', 1)  # 避免除0
    conversion_rate = round(buy_count / click_count * 100, 2) if click_count > 0 else 0

    # 品类分布（前10）
    category_stats = {}
    rows = db.session.query(
        UserBehavior.category,
        db.func.count('*')
    ).group_by(UserBehavior.category).order_by(db.func.count('*').desc()).limit(10).all()
    for cat, cnt in rows:
        if cat:
            category_stats[cat] = cnt

    return jsonify({
        'total_records': total,
        'today_records': today_count,
        'generated_count': generated_count,
        'conversion_rate': conversion_rate,
        'event_stats': event_stats,
        'category_stats': category_stats
    })

@app.route('/api/history')
def api_history():
    """历史记录（实时表格用）"""
    page = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 50)), 200)
    event_type = request.args.get('event_type')

    query = UserBehavior.query
    if event_type:
        query = query.filter_by(event_type=event_type)

    total = query.count()
    items = query.order_by(UserBehavior.id.desc()) \
        .offset((page - 1) * per_page) \
        .limit(per_page) \
        .all()

    return jsonify({
        'data': [item.to_dict() for item in items],
        'total': total,
        'page': page,
        'per_page': per_page
    })

# ===== 启动 =====
def init_db():
    with app.app_context():
        db.create_all()
        # 启用 SQLite WAL 模式（提高并发性能）
        try:
            conn = db.engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute('PRAGMA journal_mode=WAL;')
            cursor.execute('PRAGMA busy_timeout=5000;')  # 5秒超时
            cursor.execute('PRAGMA synchronous=NORMAL;')  # 平衡安全性与性能
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("SQLite optimized: WAL mode enabled")
        except Exception as e:
            logger.warning(f"Could not set SQLite pragmas: {e}")

        existing = UserBehavior.query.count()
        if existing == 0:
            logger.info("First startup, seeding initial data...")
            seed_data(1000)

def ensure_log_dir():
    log_dir = os.path.dirname('/var/log/bigdata-simulator.log')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

if __name__ == '__main__':
    ensure_log_dir()
    init_db()

    logger.info("Starting Big Data Simulator...")
    bg_thread = threading.Thread(target=background_generator, daemon=True)
    bg_thread.start()

    socketio.run(app, host='0.0.0.0', port=8889, debug=False)
