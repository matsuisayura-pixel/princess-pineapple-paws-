# -*- coding: utf-8 -*-
"""Fix remaining geocoding issues"""
import sqlite3, urllib.request, urllib.parse, json, time, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH = 'backend/data/pineapple_paws.db'
conn = sqlite3.connect(DB_PATH)

def gsi(q):
    url = 'https://msearch.gsi.go.jp/address-search/AddressSearch?q=' + urllib.parse.quote(q)
    req = urllib.request.Request(url, headers={'User-Agent': 'PineappleSeichi/1.0'})
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read())
    if d:
        return d[0]['geometry']['coordinates'][1], d[0]['geometry']['coordinates'][0]
    return None, None

def update(id_, lat, lng, note=''):
    conn.execute('UPDATE spots SET lat=?, lng=?, pineapple_score=NULL WHERE id=?', (lat, lng, id_))
    conn.commit()
    print('  Fixed id={}: lat={:.4f} lng={:.4f} {}'.format(id_, lat, lng, note))

# Fix misgeocoded: id=1496 (高岳駅=名古屋, not Ibaraki)
# 高岳駅 Nagoya Higashiyama line
lat, lng = gsi('高岳駅 名古屋市東区')
time.sleep(0.3)
if lat:
    update(1496, lat, lng, '高岳駅 名古屋')
else:
    update(1496, 35.1730, 136.9152, '高岳駅 名古屋 (hardcoded)')

# Fix misgeocoded: id=1548 (白山店=東京都文京区, not Akita)
lat, lng = gsi('俺の生きる道 東京都文京区白山')
time.sleep(0.3)
if lat:
    update(1548, lat, lng, '白山 東京')
else:
    lat, lng = gsi('白山駅 東京都文京区')
    time.sleep(0.3)
    if lat:
        update(1548, lat, lng, '白山駅 東京')
    else:
        update(1548, 35.7200, 139.7461, '白山 文京区 (hardcoded)')

# Fix id=1452 TruffleBAKERY (門前仲町本店)
lat, lng = gsi('東京都江東区富岡1丁目11-2')
time.sleep(0.3)
if lat:
    update(1452, lat, lng, 'TruffleBAKERY 門前仲町')
else:
    update(1452, 35.6712, 139.7981, '門前仲町 (hardcoded)')

# Fix id=1464 トイザらス 錦糸町店 (Kinshicho, Tokyo)
lat, lng = gsi('東京都墨田区江東橋4丁目27-14')
time.sleep(0.3)
if lat:
    update(1464, lat, lng, 'トイザらス 錦糸町')
else:
    update(1464, 35.6955, 139.8122, '錦糸町 (hardcoded)')

# Fix id=1480 ちゅんちゅん堂 エスパル仙台店
lat, lng = gsi('エスパル仙台 宮城県仙台市青葉区中央1丁目1-1')
time.sleep(0.3)
if lat:
    update(1480, lat, lng, 'エスパル仙台')
else:
    update(1480, 38.2601, 140.8824, 'エスパル仙台 (hardcoded)')

# id=1457 焼肉ホルモン 龍の巣 (no address, leave as Tokyo placeholder for now)
print('  Skip id=1457 (no address info)')

# Final check
remaining = conn.execute('SELECT count(*) FROM spots WHERE pineapple_score=0 AND lat=35.6895').fetchone()[0]
total = conn.execute('SELECT count(*) FROM spots').fetchone()[0]
print('\nTotal spots: {}'.format(total))
print('Remaining placeholders: {}'.format(remaining))
conn.close()
