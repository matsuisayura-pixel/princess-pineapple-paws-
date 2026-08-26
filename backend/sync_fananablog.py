# -*- coding: utf-8 -*-
"""
fananablog.com の聖地ページを毎日スクレイプしてDBに同期
優先度: sheet > fananablog > oshitabi
"""
import sqlite3, urllib.request, urllib.parse, json, time, re, io, sys
from pathlib import Path
from xml.etree import ElementTree as ET

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH = Path(__file__).parent / 'data' / 'pineapple_paws.db'
STATE_FILE = Path(__file__).parent / 'sync_state_fananablog.json'
SITEMAP_URL = 'https://fananablog.com/post-sitemap.xml'

URL_GROUP_MAP = [
    ('snowman', 'Snow Man'), ('soresuno', 'Snow Man'), ('tabisuno', 'Snow Man'),
    ('tabiomosnowman', 'Snow Man'), ('sunomanma', 'Snow Man'),
    ('sixtones', 'SixTONES'),
    ('agroup', 'Aぇ! group'), ('acchikocchi', 'Aぇ! group'),
    ('kamenashi', 'KAT-TUN'), ('naniwa', 'なにわ男子'),
    ('kisumai', 'Kis-My-Ft2'), ('west', 'WEST.'),
]
URL_MEDIA_MAP = [
    ('soresuno', 'それSnow Manにやらせてください'),
    (r'tabisuno', 'Snow Manの旅するSnow Man'),
    (r'snowman.*youtube', 'Snow Man YouTube'),
    (r'sixtones.*youtube', 'SixTONES YouTube'),
    ('acchikocchi', 'あっちこっち'),
    ('kamenashi.*youtube', '亀梨和也 YouTube'),
]

# 無効な名前のパターン（セクションヘッダー・動画タイトル等）
INVALID_NAME_PATTERNS = [
    # セクション・記事構造
    r'出没地', r'の旅[①-⑩]', r'回[①-⑩]', r'各メンバー',
    r'^Snow Man', r'^SixTONES', r'^Aぇ',
    r'ロケ地記事', r'見逃し', r'サブスク', r'コメント',
    r'関連記事', r'新着記事', r'よく読まれ', r'最近の投稿',
    r'コメントをどうぞ', r'ディズニープラス',
    # YouTube動画タイトルパターン
    r'^\d{4}/\d{2}/\d{2}',        # 日付始まり (2025/07/27 〜)
    r'^#\d+[\s【]',                # #509【...】エピソード番号
    r'くん回', r'さん回',          # 〇〇くん回/さん回
    r'^Jr\.チャンネル',            # Jr.チャンネル
    r'^\d+(回|話|話目)',           # 第1回、1話目
    # チーム/グループ名
    r'チーム[①-⑩ABCDabcd]',      # チームA/チーム①
    r'^(.+)・(.+)チーム$',        # 末澤・佐野チーム
    # 都道府県名単体
    r'^(東京|神奈川|大阪|京都|北海道|埼玉|千葉|愛知|福岡|静岡|兵庫|広島|宮城|'
    r'新潟|長野|石川|富山|岡山|山口|栃木|茨城|群馬|福島|山形|秋田|岩手|青森|'
    r'大分|長崎|熊本|宮崎|鹿児島|佐賀|高知|愛媛|香川|徳島|島根|鳥取|和歌山|'
    r'奈良|三重|滋賀|岐阜|山梨|福井|沖縄|富山|石川|岐阜)$',
]

_ZEN = '０１２３４５６７８９　－'
_HAN = '0123456789 -'
_ZEN_TABLE = str.maketrans(_ZEN, _HAN)
_KANJI = {'一':'1','二':'2','三':'3','四':'4','五':'5','六':'6','七':'7','八':'8','九':'9'}

def zen2han(s):
    s = s.translate(_ZEN_TABLE)
    for k, v in _KANJI.items():
        s = s.replace(k + '丁目', v + '丁目')
    return s

def clean_html(s):
    return re.sub(r'<[^>]+>', ' ', s).strip()

def is_valid_name(name):
    if not name or len(name) < 2 or len(name) > 60:
        return False
    for pat in INVALID_NAME_PATTERNS:
        if re.search(pat, name):
            return False
    return True

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding='utf-8'))
    return {}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def fetch_page(url):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read().decode('utf-8', errors='replace')

def group_from_url(url):
    slug = url.rstrip('/').split('/')[-1].lower()
    for key, group in URL_GROUP_MAP:
        if key in slug:
            return group
    return None

def media_from_url(url):
    slug = url.rstrip('/').split('/')[-1].lower()
    for pattern, media in URL_MEDIA_MAP:
        if re.search(pattern, slug):
            return media
    return None

def parse_spots(html, url):
    """
    wp-block-table(住所あり)の直前にある最近傍のh2/h3を店名として抽出
    """
    group = group_from_url(url)
    media = media_from_url(url)
    spots = []

    # すべての <figure class="wp-block-table"> を検索
    for table_match in re.finditer(
        r'<figure[^>]*wp-block-table[^>]*>(.*?)</figure>', html, re.DOTALL
    ):
        table_html = table_match.group(1)
        if '住所' not in table_html:
            continue

        # 住所を抽出
        addr_m = re.search(r'<td>住所</td><td>(.*?)</td>', table_html, re.DOTALL)
        if not addr_m:
            continue
        addr_raw = clean_html(addr_m.group(1))
        addr = zen2han(addr_raw).strip()
        # 〒 を除去
        addr = re.sub(r'^〒\d{3}-\d{4}\s*', '', addr).strip()

        # アクセスを抽出
        acc_m = re.search(r'<td>アクセス</td><td>(.*?)</td>', table_html, re.DOTALL)
        access = clean_html(acc_m.group(1)).strip() if acc_m else ''

        # テーブルの直前のHTMLを取得してh2/h3を逆検索
        pre_html = html[:table_match.start()]
        heading_matches = list(re.finditer(r'<h([23])[^>]*>(.*?)</h\1>', pre_html, re.DOTALL))
        if not heading_matches:
            continue

        # 最後（最も近い）headingを取る
        last_heading = heading_matches[-1]
        name = clean_html(last_heading.group(2)).strip()

        if not is_valid_name(name):
            continue

        spots.append({
            'name': name,
            'address': addr,
            'access_info': access,
            'group_name': group,
            'media_title': media,
            'media_type': 'TV',
        })

    # 重複除去（同名）
    seen = set()
    unique = []
    for s in spots:
        if s['name'] not in seen:
            seen.add(s['name'])
            unique.append(s)
    return unique

def gsi_geocode(query):
    if not query:
        return None, None
    url = 'https://msearch.gsi.go.jp/address-search/AddressSearch?q=' + urllib.parse.quote(query)
    req = urllib.request.Request(url, headers={'User-Agent': 'PineappleSeichi/1.0'})
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.loads(r.read())
    if data:
        lng, lat = data[0]['geometry']['coordinates']
        return lat, lng
    return None, None

def name_exists(conn, name):
    row = conn.execute("SELECT source FROM spots WHERE name=?", (name,)).fetchone()
    if row is None:
        return False
    return row[0] in ('sheet', 'fananablog')

def insert_spot(conn, spot):
    lat, lng = gsi_geocode(spot.get('address', ''))
    time.sleep(0.3)
    if lat is None:
        lat, lng = 35.6895, 139.6917
        score = 0
    else:
        score = None
    conn.execute("""
        INSERT INTO spots (name, address, lat, lng, group_name, media_type, media_title,
                           access_info, pineapple_score, source)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        spot['name'], spot.get('address'), lat, lng,
        spot.get('group_name'), spot.get('media_type', 'TV'),
        spot.get('media_title'), spot.get('access_info'),
        score, 'fananablog'
    ))

def main():
    state = load_state()
    conn = sqlite3.connect(DB_PATH)
    added = skipped_no_change = skipped_dup = 0

    print('fananablog サイトマップ取得中...')
    req = urllib.request.Request(SITEMAP_URL, headers={'User-Agent': 'PineappleSeichi/1.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        xml_bytes = r.read()

    root = ET.fromstring(xml_bytes)
    ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    urls = [
        (url_el.findtext('sm:loc', namespaces=ns),
         url_el.findtext('sm:lastmod', namespaces=ns) or '')
        for url_el in root.findall('sm:url', ns)
        if 'seichi' in (url_el.findtext('sm:loc', namespaces=ns) or '')
        or 'seiti' in (url_el.findtext('sm:loc', namespaces=ns) or '')
    ]
    print('対象URL: {}件'.format(len(urls)))

    for url, lastmod in urls:
        if not url:
            continue
        if state.get(url) == lastmod:
            skipped_no_change += 1
            continue

        print('  取得: {}'.format(url.split('/')[-2]))
        try:
            html = fetch_page(url)
            time.sleep(1.0)
        except Exception as e:
            print('    取得エラー: {}'.format(e))
            continue

        spots = parse_spots(html, url)
        print('    パース: {}件'.format(len(spots)))
        for spot in spots:
            if name_exists(conn, spot['name']):
                skipped_dup += 1
                continue
            try:
                insert_spot(conn, spot)
                conn.commit()
                added += 1
                print('    追加: {}'.format(spot['name']))
            except Exception as e:
                print('    DBエラー: {}'.format(e))

        state[url] = lastmod

    save_state(state)
    conn.close()
    print('\n完了: 追加={}件 / 重複スキップ={}件 / 更新なし={}件'.format(
        added, skipped_dup, skipped_no_change))

if __name__ == '__main__':
    main()
