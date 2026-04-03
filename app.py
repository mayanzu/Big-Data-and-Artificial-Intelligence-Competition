#!/usr/bin/env python3
"""电商数据仿真平台 - 大数据竞赛实战用"""
import json, random, time, threading, os, datetime, sqlite3, hashlib
from flask import Flask, jsonify, request, render_template, Response, stream_with_context
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO
from geventwebsocket import WebSocketError

app = Flask(__name__)
app.config['SECRET_KEY'] = 'bigdata-simulator-2026'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///ecommerce.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

# ===== 数据库模型 =====

class UserBehavior(db.Model):
    __tablename__ = 'user_behavior'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.String(30), nullable=False)
    user_id = db.Column(db.String(20), nullable=False, index=True)
    event_type = db.Column(db.String(20), nullable=False, index=True)  # view/click/add_cart/buy/search/collect/share
    session_id = db.Column(db.String(36))
    device = db.Column(db.String(20))
    ip = db.Column(db.String(20))
    product_id = db.Column(db.String(20), index=True)
    category = db.Column(db.String(50), index=True)
    price = db.Column(db.Float)
    page_source = db.Column(db.String(50))
    stay_time = db.Column(db.Integer)  # 停留秒数

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

# ===== 数据生成器 =====

EVENT_TYPES = ['view', 'click', 'add_cart', 'buy', 'search', 'collect', 'share']
EVENT_WEIGHTS = [40, 25, 15, 8, 10, 7, 5]  # 各事件权重
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
    return f"P{random.randint(1, 500):05d}"

def random_session_id():
    return hashlib.md5(f"{time.time()}{random.random()}".encode()).hexdigest()[:32]

def generate_price(category):
    """根据品类生成合理价格"""
    ranges = {
        '手机数码': (999, 12999), '电脑办公': (299, 29999), '家用电器': (99, 19999),
        '服饰鞋包': (29, 2999), '美妆护肤': (19, 2999), '食品饮料': (9, 999),
        '母婴用品': (19, 3999), '图书音像': (9, 499), '运动户外': (29, 4999),
        '家居家装': (19, 19999), '汽车用品': (19, 9999), '珠宝首饰': (99, 99999),
        '宠物用品': (9, 1999), '医药保健': (9, 2999), '箱包皮具': (49, 9999)
    }
    lo, hi = ranges.get(category, (9, 9999))
    return round(random.uniform(lo, hi), 2)

def generate_behavior():
    """生成一条用户行为"""
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    category = random.choice(CATEGORIES)
    product_id = random_product_id()
    price = generate_price(category)
    stay_time = random.randint(1, 600) if event_type in ['view', 'click'] else None

    return UserBehavior(
        timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        user_id=random_user_id(),
        event_type=event_type,
        session_id=random_session_id(),
        device=random.choice(DEVICES),
        ip=random_ip(),
        product_id=product_id,
        category=category,
        price=price if event_type in ['view', 'buy', 'add_cart', 'collect'] else None,
        page_source=random.choice(PAGE_SOURCES),
        stay_time=stay_time
    )

def seed_data(count=1000):
    """批量种子数据"""
    with app.app_context():
        # 生成用户画像
        for i in range(1, 1001):
            uid = f"U{i:05d}"
            existing = db.session.get(UserProfile, uid)
            if not existing:
                profile = UserProfile(
                    user_id=uid,
                    name=random.choice(FAMILY_NAMES) + random.choice(GIVEN_NAMES) + random.choice(GIVEN_NAMES),
                    age=random.randint(18, 65),
                    gender=random.choice(['男', '女']),
                    city=random.choice(CITIES),
                    register_date=(datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 1095))).strftime('%Y-%m-%d'),
                    member_level=random.choice(['普通', '白银', '黄金', '铂金', '钻石']),
                    total_orders=random.randint(0, 500),
                    total_spent=round(random.uniform(0, 50000), 2)
                )
                db.session.add(profile)
        db.session.commit()

        # 生成商品
        for i in range(1, 501):
            pid = f"P{i:05d}"
            existing = db.session.get(Product, pid)
            if not existing:
                cat = random.choice(CATEGORIES)
                product = Product(
                    product_id=pid,
                    name=f"{cat[:2]}品牌{chr(65+i%26)}型号{i:03d}",
                    category=cat,
                    price=generate_price(cat),
                    stock=random.randint(0, 9999),
                    sales=random.randint(0, 50000),
                    rating=round(random.uniform(3.0, 5.0), 1),
                    created_at=(datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')
                )
                db.session.add(product)
        db.session.commit()

        # 批量生成行为数据
        for _ in range(count):
            db.session.add(generate_behavior())
        db.session.commit()
        print(f"种子数据初始化完成：用户1000个，商品500个，行为数据{count}条")

# ===== 路由 =====

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/data')
def api_data():
    """实时数据接口 - 供 Flume/Kafka 消费"""
    n = request.args.get('n', 1, type=int)
    n = min(n, 100)
    results = []
    for _ in range(n):
        behavior = generate_behavior()
        db.session.add(behavior)
        results.append(behavior.to_dict())
    db.session.commit()
    global generated_count
    with lock:
        generated_count += n
    return jsonify(results)

@app.route('/api/history')
def api_history():
    """历史数据查询"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    per_page = min(per_page, 200)
    event_type = request.args.get('event_type', '')
    category = request.args.get('category', '')

    query = UserBehavior.query
    if event_type:
        query = query.filter_by(event_type=event_type)
    if category:
        query = query.filter_by(category=category)

    pagination = query.order_by(UserBehavior.id.desc()).paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({
        'total': pagination.total,
        'page': page,
        'per_page': per_page,
        'pages': pagination.pages,
        'data': [b.to_dict() for b in pagination.items]
    })

@app.route('/api/users')
def api_users():
    """用户画像数据"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    pagination = UserProfile.query.paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({
        'total': pagination.total,
        'data': [{'user_id': u.user_id, 'name': u.name, 'age': u.age, 'gender': u.gender,
                  'city': u.city, 'register_date': u.register_date, 'member_level': u.member_level,
                  'total_orders': u.total_orders, 'total_spent': u.total_spent} for u in pagination.items]
    })

@app.route('/api/products')
def api_products():
    """商品数据"""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    category = request.args.get('category', '')
    query = Product.query
    if category:
        query = query.filter_by(category=category)
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    return jsonify({
        'total': pagination.total,
        'data': [{'product_id': p.product_id, 'name': p.name, 'category': p.category,
                  'price': p.price, 'stock': p.stock, 'sales': p.sales,
                  'rating': p.rating, 'created_at': p.created_at} for p in pagination.items]
    })

@app.route('/api/stats')
def api_stats():
    """统计接口"""
    total = UserBehavior.query.count()
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    today_count = UserBehavior.query.filter(UserBehavior.timestamp.like(f'{today}%')).count()

    event_stats = {}
    for e in EVENT_TYPES:
        event_stats[e] = UserBehavior.query.filter_by(event_type=e).count()

    category_stats = {}
    for c in CATEGORIES:
        category_stats[c] = UserBehavior.query.filter_by(category=c).count()

    buy_count = event_stats.get('buy', 0)
    view_count = event_stats.get('view', 1)
    conversion_rate = round(buy_count / view_count * 100, 2) if view_count > 0 else 0

    return jsonify({
        'total_records': total,
        'today_records': today_count,
        'generated_count': generated_count,
        'event_stats': event_stats,
        'category_stats': category_stats,
        'conversion_rate': conversion_rate
    })

@app.route('/api/log')
def api_log():
    """模拟日志输出 - 可供 Flume 直接采集"""
    lines = request.args.get('lines', 10, type=int)
    lines = min(lines, 50)
    def generate():
        global generated_count
        with app.app_context():
            for _ in range(lines):
                behavior = generate_behavior()
                db.session.add(behavior)
                data = behavior.to_dict()
                yield json.dumps(data, ensure_ascii=False) + '\n'
                time.sleep(0.05)
            db.session.commit()
        with lock:
            generated_count += lines
    return Response(stream_with_context(generate()), mimetype='text/plain')

@app.route('/api/dirty')
def api_dirty():
    """返回脏数据（含缺失值/重复值/异常值/噪音），用于预处理练习"""
    n = request.args.get('n', 50, type=int)
    n = min(n, 200)
    results = []
    for _ in range(n):
        behavior = generate_behavior()
        data = behavior.to_dict()
        # 随机制造脏数据
        import random as rnd
        if rnd.random() < 0.15:  # 15% 概率缺失 price
            data['price'] = None
        if rnd.random() < 0.10:  # 10% 概率缺失 category
            data['category'] = None
        if rnd.random() < 0.05:  # 5% 概率异常价格
            data['price'] = rnd.choice([999999, -1, 0, 0.01, 1000000])
        if rnd.random() < 0.03:  # 3% 概率乱码噪音
            data['category'] = data['category'] + '  ' if data['category'] else None
        results.append(data)
    
    # 加入重复数据（20%概率重复最后一条）
    if results and len(results) > 1:
        for _ in range(int(n * 0.2)):
            results.append(results[-1].copy())
    
    random.shuffle(results)
    return jsonify(results)

@app.route('/api/dirty/csv')
def api_dirty_csv():
    """返回脏数据 CSV 格式（含表头），用于格式转换练习"""
    n = request.args.get('n', 100, type=int)
    n = min(n, 500)
    lines = ['timestamp,user_id,event_type,category,price,device,page_source,stay_time']
    for _ in range(n):
        behavior = generate_behavior()
        d = behavior.to_dict()
        import random as rnd
        # 随机缺失
        price = d['price'] if rnd.random() > 0.15 else ''
        category = d['category'] if rnd.random() > 0.10 else ''
        stay = d['stay_time'] if d['stay_time'] and rnd.random() > 0.20 else ''
        lines.append(f"{d['timestamp']},{d['user_id']},{d['event_type']},{category},{price},{d['device']},{d['page_source']},{stay}")
    
    # 加重复行
    for _ in range(int(n * 0.1)):
        if lines:
            lines.append(lines[-1])
    
    return Response('\n'.join(lines), mimetype='text/csv')

@app.route('/api/join')
def api_join():
    """返回可关联的多表数据（行为+用户+商品），用于多表JOIN练习"""
    n = request.args.get('n', 100, type=int)
    n = min(n, 500)
    
    # 直接从数据库获取行为数据（ID一定匹配）
    behaviors_raw = UserBehavior.query.order_by(UserBehavior.id.desc()).limit(n).all()
    behaviors = [b.to_dict() for b in behaviors_raw]
    
    # 获取对应的用户和商品
    user_ids = list(set([b['user_id'] for b in behaviors if b['user_id']]))
    product_ids = list(set([b['product_id'] for b in behaviors if b['product_id']]))
    
    users = UserProfile.query.filter(UserProfile.user_id.in_(user_ids[:50])).all()
    products = Product.query.filter(Product.product_id.in_(product_ids[:50])).all()
    
    return jsonify({
        'behaviors': behaviors,
        'users': [{'user_id': u.user_id, 'name': u.name, 'age': u.age, 'gender': u.gender, 'city': u.city, 'member_level': u.member_level} for u in users],
        'products': [{'product_id': p.product_id, 'name': p.name, 'category': p.category, 'price': p.price, 'rating': p.rating, 'sales': p.sales} for p in products]
    })

@app.route('/api/export/csv')
def api_export_csv():
    """批量导出 CSV（最多10000条），用于 HDFS 上传练习"""
    n = request.args.get('n', 1000, type=int)
    n = min(n, 10000)
    
    lines = ['timestamp,user_id,event_type,session_id,device,ip,product_id,category,price,page_source,stay_time']
    for _ in range(n):
        behavior = generate_behavior()
        d = behavior.to_dict()
        lines.append(f"{d['timestamp']},{d['user_id']},{d['event_type']},{d['session_id']},{d['device']},{d['ip']},{d['product_id']},{d['category']},{d['price'] or ''},{d['page_source']},{d['stay_time'] or ''}")
    
    return Response('\n'.join(lines), mimetype='text/csv',
                   headers={'Content-Disposition': 'attachment; filename=behavior_data.csv'})





@app.route('/api/dirty/tsv')
def api_dirty_tsv():
    """返回 TSV 格式脏数据（Tab 分隔），用于格式转换练习"""
    n = request.args.get('n', 100, type=int)
    n = min(n, 500)
    import random as rnd
    lines = ['timestamp	user_id	event_type	category	price	device	page_source	stay_time']
    for _ in range(n):
        behavior = generate_behavior()
        d = behavior.to_dict()
        price = d['price'] if rnd.random() > 0.15 else ''
        category = d['category'] if rnd.random() > 0.10 else ''
        stay = d['stay_time'] if d['stay_time'] and rnd.random() > 0.20 else ''
        lines.append(f"{d['timestamp']}	{d['user_id']}	{d['event_type']}	{category}	{price}	{d['device']}	{d['page_source']}	{stay}")
    for _ in range(int(n * 0.15)):
        if lines: lines.append(lines[-1])
    return Response('\n'.join(lines), mimetype='text/tab-separated-values')

@app.route('/api/timeseries')
def api_timeseries():
    """返回按小时/天的统计数据，用于时间趋势分析"""
    days = request.args.get('days', 7, type=int)
    days = min(days, 30)
    granularity = request.args.get('granularity', 'hour')  # hour 或 day
    
    from datetime import datetime, timedelta
    now = datetime.now()
    results = []
    
    if granularity == 'hour':
        for h in range(24 * days):
            t = now - timedelta(hours=h)
            hour_str = t.strftime('%Y-%m-%d %H:00:00')
            import random as rnd
            base = 800 + rnd.randint(-200, 200)
            if 10 <= t.hour <= 16:
                base = int(base * 1.5)
            elif 23 <= t.hour or t.hour <= 5:
                base = int(base * 0.3)
            results.append({
                'time': hour_str,
                'date': t.strftime('%Y-%m-%d'),
                'hour': t.hour,
                'count': base,
                'users': int(base * 0.3),
                'buys': int(base * 0.07)
            })
    else:  # day
        for d in range(days):
            t = now - timedelta(days=d)
            import random as rnd
            base = 20000 + rnd.randint(-3000, 3000)
            if t.weekday() >= 5:
                base = int(base * 1.3)
            results.append({
                'time': t.strftime('%Y-%m-%d'),
                'date': t.strftime('%Y-%m-%d'),
                'count': base,
                'users': int(base * 0.3),
                'buys': int(base * 0.07)
            })
    
    results.reverse()
    return jsonify(results)

@app.route('/api/dirty/enhanced')
def api_dirty_enhanced():
    """返回增强版脏数据（混合编码、日期不一致、多余空格、异常值），用于深度清洗"""
    n = request.args.get('n', 50, type=int)
    n = min(n, 200)
    import random as rnd
    results = []
    noise_types = [
        lambda d: d.update({'category': None}),           # 缺失值
        lambda d: d.update({'price': None}),               # 缺失价格
        lambda d: d.update({'price': rnd.choice([999999, -1, 0, 0.001])}),  # 异常价格
        lambda d: d.update({'category': d.get('category', '') + '　' if d.get('category') else None}),  # 全角空格
        lambda d: d.update({'device': d.get('device', '').upper() if d.get('device') else None}),  # 大小写不一致
        lambda d: d.update({'timestamp': d.get('timestamp', '').replace('-', '/') if d.get('timestamp') else None}),  # 日期格式不一致
        lambda d: d.update({'user_id': '  ' + str(d.get('user_id', '')) + '  '}),  # 多余空格
        lambda d: d.update({'page_source': rnd.choice(['首页', '首页 ', ' 首页', 'shouye'])}),  # 中英混用+空格
    ]
    for _ in range(n):
        behavior = generate_behavior()
        d = behavior.to_dict()
        num_noise = rnd.randint(1, 3)
        for fn in rnd.sample(noise_types, min(num_noise, len(noise_types))):
            fn(d)
        results.append(d)
    for _ in range(int(n * 0.2)):
        if results: results.append(results[-1].copy())
    random.shuffle(results)
    return jsonify(results)

@app.route('/api/hive/ddl')
def api_hive_ddl():
    """自动生成 Hive 建表 SQL，学生可直接复制使用"""
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
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/data/ecommerce/';""",
        'csv_table': f"""CREATE TABLE {table}_csv (
    timestamp STRING,
    user_id STRING,
    event_type STRING,
    category STRING,
    price DOUBLE,
    device STRING,
    page_source STRING,
    stay_time INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/data/csv/';""",
        'tsv_table': f"""CREATE TABLE {table}_tsv (
    timestamp STRING,
    user_id STRING,
    event_type STRING,
    category STRING,
    price DOUBLE,
    device STRING,
    page_source STRING,
    stay_time INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/data/tsv/';""",
        'sample_queries': [
            f"SELECT * FROM {table} LIMIT 10;",
            f"SELECT event_type, COUNT(*) FROM {table} GROUP BY event_type;",
            f"SELECT category, AVG(price) FROM {table} WHERE price IS NOT NULL GROUP BY category;",
        ]
    })

# ===== WebSocket 实时推送 =====

@socketio.on('connect')
def handle_connect():
    print('Client connected')

def background_generator():
    """后台持续生成数据并推送"""
    while True:
        try:
            behavior = generate_behavior()
            with app.app_context():
                db.session.add(behavior)
                db.session.commit()
                data = behavior.to_dict()
                socketio.emit('new_event', data)
                global generated_count
                with lock:
                    generated_count += 1
        except Exception as e:
            print(f"Background generator error: {e}")
        time.sleep(1)

# ===== 启动 =====

with app.app_context():
    db.create_all()
    existing = UserBehavior.query.count()
    if existing == 0:
        print("首次启动，生成种子数据...")
        seed_data(1000)

# 启动后台数据生成线程
bg_thread = threading.Thread(target=background_generator, daemon=True)
bg_thread.start()

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=8889, debug=False)
