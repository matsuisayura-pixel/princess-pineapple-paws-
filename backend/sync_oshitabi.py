# -*- coding: utf-8 -*-
"""
oshitabi.net のスポットを毎日スクレイプしてDBに同期
優先度: sheet > fananablog > oshitabi（最低優先）
JSON-LD (TouristAttraction) から name/address/座標/グループを取得
"""
import sqlite3, urllib.request, urllib.parse, json, time, re, io, sys
from pathlib import Path
from xml.etree import ElementTree as ET

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH = Path(__file__).parent / 'data' / 'pineapple_paws.db'
STATE_FILE = Path(__file__).parent / 'sync_state_oshitabi.json'
SITEMAP_URL = 'https://oshitabi.net/sitemap.xml'

# キーワードから除外する固定語
_KW_EXCLUDE = {'推し活', '聖地巡礼', '推し旅', '観光', '旅行', 'ジャニーズ', '推し'}

# touristType → media_type マッピング
_TYPE_MAP = {
    'YouTube': 'YouTube',
    'MV': 'MV',
    'ドラマ': 'TV',
    '映画': '映画',
    'ライブ': 'Live',
    '舞台': '舞台',
}

_ZEN = '０１２３４５６７８９　－'
_HAN = '0123456789 -'
_ZEN_TABLE = str.maketrans(_ZEN, _HAN)

def zen2han(s):
    return s.translate(_ZEN_TABLE)

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding='utf-8'))
    return {}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def fetch_url(url, retries=2):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                return r.read()
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
            else:
                raise

def map_media_type(tourist_type):
    if not tourist_type:
        return 'TV'
    for key, mt in _TYPE_MAP.items():
        if key in tourist_type:
            return mt
    return 'TV'

def parse_spot_page(html_bytes, url):
    """JSON-LD (TouristAttraction/Place) からスポット情報を抽出"""
    text = html_bytes.decode('utf-8', errors='replace')

    # 全JSON-LDスクリプトを検索
    ld_scripts = re.findall(
        r'<script type="application/ld\+json"[^>]*>(.*?)</script>', text, re.DOTALL
    )
    spot_ld = None
    for ld_text in ld_scripts:
        try:
            ld = json.loads(ld_text.strip())
        except Exception:
            continue
        t = ld.get('@type', '')
        # @type はリストまたは文字列の場合あり
        types = t if isinstance(t, list) else [t]
        if any(x in ('TouristAttraction', 'Place', 'LocalBusiness') for x in types):
            spot_ld = ld
            break

    if not spot_ld:
        return None

    # name
    name = spot_ld.get('name', '').strip()
    if not name:
        return None

    # address
    addr_field = spot_ld.get('address', '')
    if isinstance(addr_field, dict):
        address = addr_field.get('streetAddress', '') or addr_field.get('addressLocality', '')
    else:
        address = str(addr_field)
    address = zen2han(address).strip()

    # 座標 (GSI不要！)
    geo = spot_ld.get('geo', {})
    try:
        lat = float(geo.get('latitude', 0))
        lng = float(geo.get('longitude', 0))
    except (TypeError, ValueError):
        lat, lng = 0.0, 0.0

    # メディア種別
    tourist_type = spot_ld.get('touristType', '')
    media_type = map_media_type(tourist_type)

    # 番組名: subjectOf[].name から最初のもの
    subject_of = spot_ld.get('subjectOf', [])
    media_title = None
    if subject_of and isinstance(subject_of, list):
        first = subject_of[0]
        if isinstance(first, dict):
            media_title = first.get('name') or first.get('alternateName')

    # グループ: keywords から抽出
    # パターン: "スポット名, カテゴリ, グループ1, グループ2, ..., 推し活, 聖地巡礼, 推し旅"
    kw_raw = spot_ld.get('keywords', '')
    group_name = None
    if kw_raw:
        kw_items = [k.strip() for k in kw_raw.split(',')]
        # 最初2つ(スポット名・カテゴリ)と固定語を除外
        groups = [k for k in kw_items[2:] if k and k not in _KW_EXCLUDE]
        if groups:
            group_name = '・'.join(groups)

    # アクセス情報: openingHours をそのまま使用
    access_info = spot_ld.get('openingHours', '') or None

    return {
        'name': name,
        'address': address,
        'lat': lat if lat != 0.0 else None,
        'lng': lng if lng != 0.0 else None,
        'group_name': group_name,
        'media_type': media_type,
        'media_title': media_title,
        'access_info': access_info,
    }

def name_exists(conn, name):
    """スポット名がより高い優先度で存在するか"""
    row = conn.execute("SELECT source FROM spots WHERE name=?", (name,)).fetchone()
    if row is None:
        return False
    # sheet・fananablog・oshitabi のいずれかがあれば追加しない
    return row[0] in ('sheet', 'fananablog', 'oshitabi')

def insert_spot(conn, spot):
    lat = spot.get('lat')
    lng = spot.get('lng')

    if lat is None or lng is None:
        lat, lng = 35.6895, 139.6917
        score = 0
    else:
        score = None

    conn.execute("""
        INSERT INTO spots (name, address, lat, lng, group_name,
                           media_type, media_title, access_info, pineapple_score, source)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        spot['name'], spot.get('address'), lat, lng,
        spot.get('group_name'),
        spot.get('media_type', 'TV'), spot.get('media_title'),
        spot.get('access_info'), score, 'oshitabi'
    ))

def main():
    state = load_state()
    conn = sqlite3.connect(DB_PATH)
    added = skipped = errors = 0

    print('oshitabi.net サイトマップ取得中...')
    xml_bytes = fetch_url(SITEMAP_URL)
    root = ET.fromstring(xml_bytes)
    ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

    urls = []
    for url_el in root.findall('sm:url', ns):
        loc = url_el.findtext('sm:loc', namespaces=ns) or ''
        lastmod = url_el.findtext('sm:lastmod', namespaces=ns) or ''
        if '/spots/' in loc:
            urls.append((loc, lastmod))

    print('対象URL: {}件'.format(len(urls)))

    for url, lastmod in urls:
        if state.get(url) == lastmod:
            skipped += 1
            continue

        try:
            html = fetch_url(url)
            time.sleep(0.5)
        except Exception as e:
            print('    取得エラー: {} → {}'.format(url, e))
            errors += 1
            continue

        spot = parse_spot_page(html, url)
        if not spot:
            errors += 1
            state[url] = lastmod
            continue

        if name_exists(conn, spot['name']):
            state[url] = lastmod
            skipped += 1
            continue

        try:
            insert_spot(conn, spot)
            conn.commit()
            added += 1
            print('  追加: {} | {}'.format(spot['name'], spot.get('address', '')))
        except Exception as e:
            print('  DB挿入エラー: {} → {}'.format(spot['name'], e))
            errors += 1

        state[url] = lastmod

    save_state(state)
    conn.close()
    print('\n完了: 追加={}件 / スキップ={}件 / エラー={}件'.format(added, skipped, errors))

if __name__ == '__main__':
    main()
