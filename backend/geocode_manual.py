# -*- coding: utf-8 -*-
"""Manually fix 9 remaining placeholder spots"""
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

def nom(q):
    url = 'https://nominatim.openstreetmap.org/search?q=' + urllib.parse.quote(q) + '&format=json&limit=1'
    req = urllib.request.Request(url, headers={'User-Agent': 'PineappleSeichi/1.0 matsui.sayura@itghd.jp'})
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read())
    if d:
        return float(d[0]['lat']), float(d[0]['lon'])
    return None, None

# id -> (gsi_queries, nominatim_queries)
fixes = {
    1452: (['TruffleBAKERY 門前仲町', 'TruffleBAKERY'], []),
    1457: (['焼肉ホルモン 龍の巣 東京', '龍の巣 東京'], []),
    1464: (['トイザらス 錦糸町店', 'トイザらス 錦糸町'], []),
    1480: (['ちゅんちゅん堂 エスパル仙台', 'エスパル仙台'], []),
    1492: (['大黒屋鎌餅本舗 京都', '鞍馬口 京都'], []),
    1496: (['極上松阪牛 牛追道中 名古屋', '高岳駅 名古屋'], []),
    1548: (['俺の生きる道 白山 東京', '白山 東京都文京区'], []),
    1549: ([], ['Maha Nakhon Tower Bangkok', 'King Power Mahanakhon Bangkok Thailand']),
}

for id_, (gsi_queries, nom_queries) in fixes.items():
    lat, lng = None, None
    for q in gsi_queries:
        try:
            lat, lng = gsi(q)
            time.sleep(0.3)
        except Exception:
            time.sleep(0.3)
        if lat:
            break
    if not lat:
        for q in nom_queries:
            try:
                lat, lng = nom(q)
                time.sleep(1.1)
            except Exception:
                time.sleep(1.1)
            if lat:
                break

    name = conn.execute('SELECT name FROM spots WHERE id=?', (id_,)).fetchone()[0]
    if lat:
        conn.execute('UPDATE spots SET lat=?, lng=?, pineapple_score=NULL WHERE id=?', (lat, lng, id_))
        conn.commit()
        print('  OK id={}: {} -> ({:.4f}, {:.4f})'.format(id_, name, lat, lng))
    else:
        print('  NG id={}: {}'.format(id_, name))

# 1475 already fixed - check
r = conn.execute('SELECT lat, lng, pineapple_score FROM spots WHERE id=1475').fetchone()
print('  id=1475 check: lat={}, score={}'.format(r[0], r[2]))

remaining = conn.execute('SELECT count(*) FROM spots WHERE pineapple_score=0 AND lat=35.6895').fetchone()[0]
print('\nRemaining placeholders: {}'.format(remaining))
conn.close()
