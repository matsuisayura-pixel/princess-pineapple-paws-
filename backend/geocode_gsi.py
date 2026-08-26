# -*- coding: utf-8 -*-
"""GSI API geocoding for remaining placeholder spots"""
import sqlite3, urllib.request, urllib.parse, json, time, io, sys, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH = 'backend/data/pineapple_paws.db'

# Full-width to half-width mapping using Unicode ordinals
_ZEN = '０１２３４５６７８９　－'
_HAN = '0123456789 -'
ZEN_HAN = str.maketrans(_ZEN, _HAN)

KANJI_NUM = {'一':'1','二':'2','三':'3','四':'4','五':'5',
             '六':'6','七':'7','八':'8','九':'9'}

def zen2han(s):
    s = s.translate(ZEN_HAN)
    for k, v in KANJI_NUM.items():
        s = s.replace(k + '丁目', v + '丁目')  # k + '丁目'
    return s

def clean_address(addr):
    if not addr:
        return None
    addr = zen2han(addr)
    addr = re.sub(r'【.*?】.*', '', addr)  # 【...】以降を除去
    m = re.match(r'^(.+?(?:\d+丁目\s*\d+-\d+|\d+-\d+-\d+|\d+-\d+))', addr)
    if m:
        return m.group(1).strip()
    return addr.strip()

def gsi_geocode(query):
    q = urllib.parse.quote(query)
    url = 'https://msearch.gsi.go.jp/address-search/AddressSearch?q=' + q
    req = urllib.request.Request(url, headers={'User-Agent': 'PineappleSeichi/1.0'})
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.loads(r.read())
    if data:
        lng, lat = data[0]['geometry']['coordinates']
        return lat, lng
    return None, None

def nominatim(query):
    q = urllib.parse.quote(query)
    url = 'https://nominatim.openstreetmap.org/search?q=' + q + '&format=json&limit=1'
    req = urllib.request.Request(url, headers={'User-Agent': 'PineappleSeichi/1.0 matsui.sayura@itghd.jp'})
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.loads(r.read())
    if data:
        return float(data[0]['lat']), float(data[0]['lon'])
    return None, None

conn = sqlite3.connect(DB_PATH)
rows = conn.execute('SELECT id, name, address FROM spots WHERE pineapple_score=0 AND lat=35.6895').fetchall()
print('Target: {} spots'.format(len(rows)))

ok, ng = 0, 0
ng_list = []

for id_, name, address in rows:
    lat, lng = None, None
    is_overseas = bool(address and ('Bangkok' in address or 'Thailand' in address))

    if is_overseas:
        try:
            lat, lng = nominatim(address)
            time.sleep(1.1)
        except Exception:
            time.sleep(1.1)
    else:
        addr_clean = clean_address(address)
        addr_zen = zen2han(address) if address else None
        queries = []
        if addr_clean and addr_clean != (addr_zen or ''):
            queries.append(addr_clean)
        if addr_zen:
            queries.append(addr_zen)
        queries.append(name)

        for q in queries:
            if not q:
                continue
            try:
                lat, lng = gsi_geocode(q)
                time.sleep(0.3)
            except Exception:
                time.sleep(0.3)
            if lat:
                break

    if lat:
        conn.execute('UPDATE spots SET lat=?, lng=?, pineapple_score=NULL WHERE id=?', (lat, lng, id_))
        conn.commit()
        ok += 1
        print('  OK [{}] {}'.format(ok, name))
    else:
        ng += 1
        ng_list.append((id_, name, address))
        print('  NG {} | {}'.format(name, address))

conn.close()
print('\nDone: OK={} / NG={}'.format(ok, ng))
if ng_list:
    print('\nFailed:')
    for r in ng_list:
        print('  id={}: {} | {}'.format(r[0], r[1], r[2]))
