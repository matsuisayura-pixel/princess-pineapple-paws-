"""
Google Sheetsのh列（投稿内容）を毎日DBに同期するスクリプト
使い方: python sync_from_sheet.py
"""
import csv, io, re, sys, time, urllib.request, sqlite3, json
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# ===== 設定 =====
SHEET_ID = "158Hk2NzFMSU32eTEVb9nRLAaAd6uY5a8F59zY8wqDr0"
GID      = "1813009399"
DB_PATH  = Path(__file__).parent / "data" / "pineapple_paws.db"

SHEET_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

# メンバー→グループ対応
MEMBER_GROUP = {
    "深澤辰哉":"Snow Man","渡辺翔太":"Snow Man","阿部亮平":"Snow Man",
    "宮舘涼太":"Snow Man","宮館涼太":"Snow Man","向井康二":"Snow Man",
    "ラウール":"Snow Man","目黒蓮":"Snow Man","佐久間大介":"Snow Man","岩本照":"Snow Man",
    "松村北斗":"SixTONES","田中樹":"SixTONES","ジェシー":"SixTONES",
    "髙地優吾":"SixTONES","高地優吾":"SixTONES","京本大我":"SixTONES","森本慎太郎":"SixTONES",
    "末澤誠也":"Aぇ! group","小島健":"Aぇ! group","福本大晴":"Aぇ! group",
    "佐野晶哉":"Aぇ! group","草間リチャード敬太":"Aぇ! group","正門良規":"Aぇ! group",
    "西畑大吾":"なにわ男子","大西流星":"なにわ男子","道枝駿佑":"なにわ男子",
    "長尾謙杜":"なにわ男子","高橋恭平":"なにわ男子","藤原丈一郎":"なにわ男子","大橋和也":"なにわ男子",
    "二宮和也":"嵐","相葉雅紀":"嵐","松本潤":"嵐","大野智":"嵐","櫻井翔":"嵐",
    "山田涼介":"Hey! Say! JUMP","中島裕翔":"Hey! Say! JUMP","伊野尾慧":"Hey! Say! JUMP",
    "有岡大貴":"Hey! Say! JUMP","八乙女光":"Hey! Say! JUMP","薮宏太":"Hey! Say! JUMP",
    "髙木雄也":"Hey! Say! JUMP","知念侑李":"Hey! Say! JUMP","岡本圭人":"Hey! Say! JUMP",
    "菊池風磨":"timelesz","中島健人":"timelesz","松島聡":"timelesz",
    "佐藤勝利":"timelesz","橋本将生":"timelesz","篠塚大輝":"timelesz",
    "重岡大毅":"WEST.","神山智洋":"WEST.","濱田崇裕":"WEST.","桐山照史":"WEST.",
    "小瀧望":"WEST.","藤井流星":"WEST.","中間淳太":"WEST.",
    "村上信五":"SUPER EIGHT","横山裕":"SUPER EIGHT","丸山隆平":"SUPER EIGHT",
    "大倉忠義":"SUPER EIGHT","安田章大":"SUPER EIGHT","錦戸亮":"SUPER EIGHT","渋谷すばる":"SUPER EIGHT",
    "玉森裕太":"Kis-My-Ft2","藤ヶ谷太輔":"Kis-My-Ft2","北山宏光":"Kis-My-Ft2",
    "宮田俊哉":"Kis-My-Ft2","横尾渉":"Kis-My-Ft2","千賀健永":"Kis-My-Ft2","二階堂高嗣":"Kis-My-Ft2",
    "永瀬廉":"King & Prince","高橋海人":"King & Prince","岸優太":"King & Prince",
    "神宮寺勇太":"King & Prince","岩橋玄樹":"King & Prince",
    "川島如恵留":"Travis Japan","松田元太":"Travis Japan","中村海人":"Travis Japan",
    "吉澤閑也":"Travis Japan","七五三掛龍也":"Travis Japan","宮近海斗":"Travis Japan","ノエル川島":"Travis Japan",
    "木村拓哉":"SMAP",
}


def fetch_sheet_csv():
    print("スプレッドシートを取得中...")
    req = urllib.request.Request(SHEET_CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def parse_spots_from_content(content):
    """H列のテキストから複数スポットを抽出（フォーマットA・B両対応）"""
    spots = []
    # ────で区切る
    blocks = re.split(r'─{4,}', content)
    for block in blocks:
        block = block.strip()
        if not block or '📍' not in block:
            continue
        spot = parse_single_block(block)
        if spot and spot.get('name'):
            spots.append(spot)
    return spots


# メディアマーカー絵文字（📺・🎬 両対応）
MEDIA_EMOJIS = ['📺', '🎬']
# 住所マーカー（🏠・【住所 両対応）
ADDR_MARKERS = ['🏠', '【住所', '【アクセス】']
# 停止マーカー（タレント行の終わりを示す）
STOP_EMOJIS = ['🗓', '🍽', '🏠', '【', '🎬', '📺']
# メニュー系絵文字（🍽以外にも使われるもの）
MENU_EMOJIS = ['🍽', '🍜', '🍣', '🍤', '🍡', '🥢', '☕', '🕺', '🚶', '🏃',
               '🎡', '🎁', '🏢', '🌊', '🚗', '⭐', '📱', '🍶', '🏄', '⛷',
               '🎠', '🛶', '🎯', '🗝', '⚽', '🏊', '🎤', '🎭']


def parse_single_block(block):
    lines = [l.strip() for l in block.split('\n') if l.strip()]
    spot = {}

    # 店名：📍 の行
    for line in lines:
        if '📍' in line:
            name = re.sub(r'📍\s*', '', line).strip()
            name = re.sub(r'^[🔵🔴🟡🟢⚫⚪▶►★☆♦♠♣♥🛼]+\s*', '', name).strip()
            name = re.split(r'←', name)[0].strip()
            spot['name'] = name
            break

    if not spot.get('name'):
        return spot

    # タレント名：📍の次〜メディアマーカーの前
    talent_lines = []
    in_talent = False
    for line in lines:
        if '📍' in line:
            in_talent = True
            continue
        if in_talent:
            if any(e in line for e in MEDIA_EMOJIS + ['🗓'] + ADDR_MARKERS):
                break
            talent_lines.append(line)

    talent_raw = ' '.join(talent_lines).strip()
    # 先頭の絵文字（グループアイコン）を除去
    talent_clean = re.sub(r'^[\U0001F300-\U0001FFFF☀-⟿]\s*', '', talent_raw)
    talent_clean = re.sub(r'（[^）]*）', '', talent_clean)
    talent_clean = re.sub(r'\([^)]*\)', '', talent_clean)
    talent_clean = re.sub(r'[　\s]+', '・', talent_clean.strip()).strip('・')
    spot['talent_name'] = talent_clean if talent_clean else talent_raw

    # グループ名を推定
    groups = set()
    for name, grp in MEMBER_GROUP.items():
        if name in talent_raw:
            groups.add(grp)
    spot['group_names'] = json.dumps(list(groups), ensure_ascii=False) if groups else None
    spot['group_name'] = list(groups)[0] if len(groups) == 1 else (list(groups)[0] if groups else None)

    # メディア（📺 または 🎬）
    for line in lines:
        if any(e in line for e in MEDIA_EMOJIS):
            media = re.sub(r'[📺🎬]\s*', '', line).strip()
            spot['media_title'] = media
            if any(x in media for x in ['チューブ', 'ちゅーぶ', 'Tube', 'tube', 'YouTube']):
                spot['media_type'] = 'YouTube'
            elif any(x in media for x in ['Instagram', 'instagram', 'TikTok', 'X.com', 'Twitter', 'SNS', 'ストーリー', '公式X', 'Instagram']):
                spot['media_type'] = 'SNS'
            elif any(x in media for x in ['映画', 'Netflix', 'ドラマ', 'MV', 'CM']):
                spot['media_type'] = '映画' if '映画' in media else ('MV' if 'MV' in media else 'TV')
            else:
                spot['media_type'] = 'TV'
            break

    # 放送日 🗓
    for line in lines:
        if '🗓' in line:
            spot['broadcast_date'] = re.sub(r'🗓\s*', '', line).strip()
            break

    # メニュー：🗓の後・住所マーカーの前にある行（絵文字に依存しない位置ベース）
    menu_lines = []
    after_date = False
    for line in lines:
        if '🗓' in line:
            after_date = True
            continue
        if after_date:
            if '🏠' in line or '【住所' in line or '【アクセス】' in line:
                break
            # メディア・タレント行は除外
            if any(e in line for e in MEDIA_EMOJIS):
                break
            # 先頭絵文字を除いたテキストを取得
            text = re.sub(r'^[\U0001F000-\U0001FFFF☀-⟿⛄☃️📱⭐]+\s*', '', line).strip()
            if text and not any(x in text for x in ['駅', '徒歩', '出口', '番出口']):
                menu_lines.append(text)
    spot['menu_items'] = '\n'.join(menu_lines) if menu_lines else None

    # 住所（🏠 または 【住所・アクセス】 の次行）
    for i, line in enumerate(lines):
        if '🏠' in line:
            addr = re.sub(r'🏠\s*', '', line).strip()
            if not addr and i + 1 < len(lines):
                addr = lines[i + 1].strip()
            spot['address'] = addr
            break
        if '【住所' in line or ('【アクセス】' in line and not spot.get('address')):
            # 次の行が住所
            if i + 1 < len(lines):
                spot['address'] = lines[i + 1].strip()
            # アクセス情報は住所の次の行
            if i + 2 < len(lines):
                spot['access_info'] = lines[i + 2].strip()
            break

    # 【アクセス】単独行（フォーマットA）
    for line in lines:
        if '【アクセス】' in line and '住所' not in line:
            acc = re.sub(r'【アクセス】\s*', '', line).strip()
            if acc:
                spot['access_info'] = acc
            break

    return spot


_ZEN = '０１２３４５６７８９　－'
_HAN = '0123456789 -'
_ZEN_HAN_TABLE = str.maketrans(_ZEN, _HAN)
_KANJI_NUM = {'一':'1','二':'2','三':'3','四':'4','五':'5',
              '六':'6','七':'7','八':'8','九':'9'}

def _zen2han(s):
    s = s.translate(_ZEN_HAN_TABLE)
    for k, v in _KANJI_NUM.items():
        s = s.replace(k + '丁目', v + '丁目')
    return s

def _clean_addr(addr):
    addr = _zen2han(addr)
    addr = re.sub(r'【.*?】.*', '', addr)
    m = re.match(r'^(.+?(?:\d+丁目\s*\d+-\d+|\d+-\d+-\d+|\d+-\d+))', addr)
    if m:
        return m.group(1).strip()
    return addr.strip()

def geocode_address(address):
    """国土地理院API（日本専用）→ Nominatim フォールバック"""
    if not address:
        return None, None

    is_overseas = any(w in address for w in ['Bangkok', 'Thailand', 'タイ', 'Korea', '中国', 'China'])

    if not is_overseas:
        cleaned = _clean_addr(address)
        for q in [cleaned, _zen2han(address)]:
            if not q:
                continue
            try:
                url = 'https://msearch.gsi.go.jp/address-search/AddressSearch?q=' + urllib.parse.quote(q)
                req = urllib.request.Request(url, headers={"User-Agent": "PineappleSeichi/1.0"})
                with urllib.request.urlopen(req, timeout=10) as r:
                    data = json.loads(r.read())
                if data:
                    lng, lat = data[0]['geometry']['coordinates']
                    return lat, lng
            except Exception as e:
                print(f"  GSIエラー: {e}")
            time.sleep(0.3)

    # 海外 or GSI失敗 → Nominatim
    try:
        query = urllib.parse.quote(address)
        url = f"https://nominatim.openstreetmap.org/search?q={query}&format=json&limit=1"
        req = urllib.request.Request(url, headers={"User-Agent": "PineappleSeichi/1.0 matsui.sayura@itghd.jp"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        if data:
            return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        print(f"  Nominatimエラー: {e}")
    return None, None


def spot_exists(conn, name, talent):
    """同名・同タレントのスポットが既にDBにあるか確認"""
    cur = conn.execute(
        "SELECT id FROM spots WHERE name=? AND talent_name=?",
        (name, talent)
    )
    return cur.fetchone() is not None


def insert_spot(conn, spot):
    lat, lng = geocode_address(spot.get('address'))
    time.sleep(0.3)  # GSI APIは制限緩め（Nominatimフォールバック時は内部でsleep済み）

    # ジオコーディング失敗時は東京中心座標を仮置き（pineapple_score=0で未確定扱い）
    if lat is None:
        lat, lng = 35.6895, 139.6917
        score = 0
    else:
        score = None

    conn.execute("""
        INSERT INTO spots (name, address, lat, lng, talent_name, group_name,
            media_type, media_title, broadcast_date, menu_items, access_info, group_names, pineapple_score)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        spot.get('name'), spot.get('address'), lat, lng,
        spot.get('talent_name'), spot.get('group_name'),
        spot.get('media_type'), spot.get('media_title'),
        spot.get('broadcast_date'), spot.get('menu_items'),
        spot.get('access_info'), spot.get('group_names'), score,
    ))


def main():
    import urllib.parse

    # シートデータ取得
    csv_text = fetch_sheet_csv()
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    print(f"シート行数: {len(rows)}")

    conn = sqlite3.connect(DB_PATH)
    added = 0
    skipped = 0
    failed = 0

    for row_i, row in enumerate(rows[1:], start=2):  # ヘッダースキップ
        if len(row) < 8:
            continue
        content = row[7]  # H列（0-indexed=7）
        if not content.strip():
            continue

        spots = parse_spots_from_content(content)
        for spot in spots:
            name = spot.get('name', '').strip()
            talent = spot.get('talent_name', '').strip()
            if not name:
                failed += 1
                continue
            if spot_exists(conn, name, talent):
                skipped += 1
                continue
            try:
                insert_spot(conn, spot)
                conn.commit()
                added += 1
                print(f"  追加: {name} / {talent}")
            except Exception as e:
                print(f"  エラー: {name} → {e}")
                failed += 1

    conn.close()
    print(f"\n完了: 追加={added}件 / スキップ={skipped}件 / エラー={failed}件")


if __name__ == "__main__":
    main()
