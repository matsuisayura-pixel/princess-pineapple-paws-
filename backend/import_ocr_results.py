#!/usr/bin/env python3
"""
OCR結果をDBにインポートするスクリプト。
ocr_results.json を読み込んで、既存のInstagramデータを置き換える。
"""
import json, sqlite3, time, re, sys, io, urllib.request, urllib.parse
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

DB_PATH = Path(__file__).parent / "data" / "pineapple_paws.db"
OCR_RESULTS_PATH = Path(__file__).parent.parent / "ocr_results.json"

AREA_COORDS = {
    "北海道": (43.0642, 141.3469), "札幌": (43.0642, 141.3469), "旭川": (43.7707, 142.3650),
    "東京": (35.6762, 139.6503), "都内": (35.6762, 139.6503),
    "新宿": (35.6938, 139.7036), "渋谷": (35.6580, 139.7016),
    "浅草": (35.7147, 139.7966), "六本木": (35.6628, 139.7317),
    "表参道": (35.6654, 139.7131), "原宿": (35.6715, 139.7025),
    "銀座": (35.6717, 139.7649), "池袋": (35.7295, 139.7109),
    "品川": (35.6284, 139.7387), "目黒": (35.6340, 139.7154),
    "恵比寿": (35.6467, 139.7101), "代官山": (35.6488, 139.7039),
    "横浜": (35.4437, 139.6380), "川崎": (35.5311, 139.7029),
    "埼玉": (35.8574, 139.6489), "川越": (35.9252, 139.4856),
    "千葉": (35.6074, 140.1065),
    "多摩": (35.6470, 139.4482),
    "大阪": (34.6937, 135.5023), "梅田": (34.7026, 135.4975),
    "難波": (34.6686, 135.5026), "なんば": (34.6686, 135.5026),
    "新大阪": (34.7334, 135.5003), "心斎橋": (34.6757, 135.5003),
    "京都": (35.0116, 135.7681),
    "神戸": (34.6901, 135.1956), "兵庫": (34.6901, 135.1956),
    "奈良": (34.6851, 135.8048),
    "名古屋": (35.1815, 136.9066), "愛知": (35.1815, 136.9066),
    "静岡": (34.9769, 138.3831), "浜松": (34.7108, 137.7268),
    "三重": (34.7303, 136.5086), "伊勢": (34.4922, 136.7066),
    "松阪": (34.5786, 136.5269),
    "広島": (34.3853, 132.4553),
    "岡山": (34.6618, 133.9350),
    "福岡": (33.5904, 130.4017), "博多": (33.5904, 130.4017),
    "仙台": (38.2688, 140.8721), "宮城": (38.2688, 140.8721),
    "沖縄": (26.2124, 127.6809), "那覇": (26.2124, 127.6809),
    "金沢": (36.5613, 136.6562), "石川": (36.5613, 136.6562),
    "富山": (36.6953, 137.2113),
    "長野": (36.6486, 138.1947), "松本": (36.2380, 137.9723),
    "新潟": (37.9161, 139.0364),
}

def get_area_coords(text: str) -> tuple:
    if not text:
        return (35.6762, 139.6503)
    for area, coords in AREA_COORDS.items():
        if area in text:
            return coords
    return (35.6762, 139.6503)


def geocode(name: str, address: str = "", area_hint: str = "") -> tuple | None:
    """Nominatimでジオコーディング"""
    # Try with address first, then name only
    queries = []
    if address and len(address) > 5:
        queries.append(address)
    if name:
        query = f"{name} {area_hint}".strip()
        queries.append(query)

    for query in queries:
        params = urllib.parse.urlencode({
            "q": query, "format": "json", "limit": 1,
            "accept-language": "ja", "countrycodes": "jp"
        })
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pineapple-seichi/1.0"})
            with urllib.request.urlopen(req, timeout=6) as resp:
                data = json.loads(resp.read())
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception:
            pass
        time.sleep(1.1)

    return None


def get_media_type(media_title: str) -> str:
    if not media_title:
        return "SNS"
    ml = media_title.lower()
    if 'youtube' in ml or 'youtu' in ml or 'チャンネル' in media_title:
        return "YouTube"
    if 'インスタ' in media_title or 'instagram' in ml:
        return "Instagram"
    if 'ドラマ' in media_title or 'テレ' in media_title or 'フジ' in media_title or 'tv' in ml:
        return "TV"
    if 'mv' in ml or 'music video' in ml:
        return "MV"
    if 'ラジオ' in media_title:
        return "ラジオ"
    return "TV"


def clean_broadcast_date(date_str: str) -> str:
    """OCR読み取り日付を YYYY-MM-DD 形式に変換"""
    if not date_str:
        return None
    # Pattern: 2022年9月16日 放送
    m = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return date_str


# OCR誤読→正しいグループ名のマッピング
GROUP_NORMALIZE = {
    # Snow Man
    "SnowMan": "Snow Man", "Snowman": "Snow Man", "SnowMan": "Snow Man",
    "snow man": "Snow Man", "すのちゅーぶ": "Snow Man", "すの": "Snow Man",
    # SixTONES
    "ストチューブ": "SixTONES", "SixStones": "SixTONES", "sixTONES": "SixTONES",
    # なにわ男子
    "なにわTube": "なにわ男子", "なにわ": "なにわ男子",
    # Aぇ! group
    "Aぇlgroup": "Aぇ! group", "Aえlgroup": "Aぇ! group", "Aえ!group": "Aぇ! group",
    "Aぇ!group": "Aぇ! group", "Aぇgroup": "Aぇ! group", "Aえgroup": "Aぇ! group",
    "Aぇちゅ~ぶ": "Aぇ! group", "(Aぇ!group)": "Aぇ! group", "Aぇ! Group": "Aぇ! group",
    # WEST.
    "WEST": "WEST.", "West.": "WEST.", "ジャニーズWEST": "WEST.",
    "GOiGoWEST": "WEST.", "GO!GO!WEST": "WEST.",
    # Travis Japan
    "TravisJapan": "Travis Japan", "travisjapan": "Travis Japan",
    # Kis-My-Ft2
    "Kis-My-FtZ": "Kis-My-Ft2", "KisMy": "Kis-My-Ft2", "キスマイ": "Kis-My-Ft2",
    # timelesz
    "(timelesz)": "timelesz",
    # King & Prince
    "KingPrince": "King & Prince", "King&Prince": "King & Prince",
    # 嵐
    "@嵐": "嵐", "ARASHI": "嵐",
    # NEWS
    "(NEVS)": "NEWS", "NEVS": "NEWS",
    # KinKi Kids
    "DOMOTO": "KinKi Kids",
    # 木村拓哉
    "木村拓哉 公式Instagram": "木村拓哉",
}

KNOWN_GROUPS = {
    "Snow Man", "SnowMan", "SixTONES", "なにわ男子", "Aぇ! group", "Aえ! group",
    "WEST.", "SUPER EIGHT", "Travis Japan", "King & Prince", "Hey! Say! JUMP",
    "timelesz", "Kis-My-Ft2", "嵐", "KAT-TUN", "KinKi Kids", "NEWS", "V6",
    "STARTO", "横山会", "よにのちゃんねる", "よにの", "なにわTube", "すのちゅーぶ",
    "SnowMan", "SixTONES",
}

MEMBER_GROUP_FULL = {
    "岩本照": "Snow Man", "深澤辰哉": "Snow Man", "ラウール": "Snow Man",
    "渡辺翔太": "Snow Man", "阿部亮平": "Snow Man", "宮舘涼太": "Snow Man",
    "宮館涼太": "Snow Man", "目黒蓮": "Snow Man", "向井康二": "Snow Man", "佐久間大介": "Snow Man",
    "ジェシー": "SixTONES", "京本大我": "SixTONES", "松村北斗": "SixTONES",
    "田中樹": "SixTONES", "森本慎太郎": "SixTONES", "髙地優吾": "SixTONES", "高地優吾": "SixTONES",
    "道枝駿佑": "なにわ男子", "高橋恭平": "なにわ男子", "西畑大吾": "なにわ男子",
    "大西流星": "なにわ男子", "藤原丈一郎": "なにわ男子", "長尾謙杜": "なにわ男子", "大橋和也": "なにわ男子",
    "末澤誠也": "Aぇ! group", "佐野晶哉": "Aぇ! group", "福本大晴": "Aぇ! group",
    "草間リチャード敬太": "Aぇ! group", "小島健": "Aぇ! group", "正門良規": "Aぇ! group",
    "桐山照史": "WEST.", "濵田崇裕": "WEST.", "小瀧望": "WEST.",
    "中間淳太": "WEST.", "藤井流星": "WEST.", "神山智洋": "WEST.", "重岡大毅": "WEST.",
    "永瀬廉": "King & Prince", "高橋海人": "King & Prince", "髙橋海人": "King & Prince",
    "岸優太": "King & Prince", "神宮寺勇太": "King & Prince", "平野紫耀": "King & Prince",
    "松田元太": "Travis Japan", "吉澤閑也": "Travis Japan", "宮近海斗": "Travis Japan",
    "七五三掛龍也": "Travis Japan", "川島如恵留": "Travis Japan",
    "中村海人": "Travis Japan", "松倉海斗": "Travis Japan",
    "横山裕": "SUPER EIGHT", "村上信五": "SUPER EIGHT",
    "丸山隆平": "SUPER EIGHT", "安田章大": "SUPER EIGHT", "大倉忠義": "SUPER EIGHT",
    "菊池風磨": "timelesz", "松島聡": "timelesz", "佐藤勝利": "timelesz",
    "中島健人": "中島健人",  # ソロ扱い（timelesz脱退）
    "二宮和也": "嵐", "大野智": "嵐", "相葉雅紀": "嵐", "松本潤": "嵐", "桜井翔": "嵐", "櫻井翔": "嵐",
    "上田竜也": "上田竜也",  # ソロ扱い
    "内博貴": "内博貴",      # ソロ扱い
    "亀梨和也": "KAT-TUN", "中丸雄一": "KAT-TUN",
    "山田涼介": "Hey! Say! JUMP", "中島裕翔": "Hey! Say! JUMP", "知念侑李": "Hey! Say! JUMP",
    "岡本圭人": "Hey! Say! JUMP", "八乙女光": "Hey! Say! JUMP", "髙木雄也": "Hey! Say! JUMP",
    "高木雄也": "Hey! Say! JUMP", "伊野尾慧": "Hey! Say! JUMP", "有岡大貴": "Hey! Say! JUMP",
    "薮宏太": "Hey! Say! JUMP",
    "藤ヶ谷太輔": "Kis-My-Ft2", "宮田俊哉": "Kis-My-Ft2", "玉森裕太": "Kis-My-Ft2",
    "千賀健永": "Kis-My-Ft2", "二階堂高嗣": "Kis-My-Ft2", "横尾渉": "Kis-My-Ft2", "北山宏光": "Kis-My-Ft2",
    "増田貴久": "NEWS", "小山慶一郎": "NEWS", "加藤シゲアキ": "NEWS",
    "堂本光一": "KinKi Kids", "堂本剛": "KinKi Kids",
    "坂本昌行": "V6", "長野博": "V6", "井ノ原快彦": "V6", "森田剛": "V6", "三宅健": "V6", "岡田准一": "V6",
}

def clean_spot(spot: dict) -> dict | None:
    """スポットデータのクリーンアップ"""
    s = dict(spot)

    # スポット名クリーン
    name = (s.get('spot_name') or '').strip()
    # グループ名が末尾に混入しているケースを除去
    for g in KNOWN_GROUPS:
        name = name.replace(g, '').strip()
    # 記号・余分な空白を整理
    name = re.sub(r'\s+', ' ', name).strip()
    name = name.strip('・ー（）()[]【】　')

    # スポット名が短すぎる・ASCII多すぎ・明らかに間違いをフィルタ
    if len(name) < 2:
        return None
    # ASCII文字だけで構成されている（地図上の場所名ではない）
    if re.match(r'^[A-Za-z0-9\s\-_/.]+$', name) and len(name) < 8:
        return None

    s['spot_name'] = name

    # タレント名クリーン：@記号・余分テキスト除去
    member = (s.get('member_name') or '').strip()
    member = member.lstrip('@＠').strip()

    # グループ名の初期値
    group = s.get('group_name') or 'STARTO'

    # カッコ内がグループ名なら先にグループを確定（「猪俣周杜 (timelesz)」→ timelesz）
    bracket_match = re.search(r'[\(（]([^）)]+)[）)]', member)
    if bracket_match:
        bracket_content = bracket_match.group(1).strip()
        for pat, grp in GROUP_NORMALIZE.items():
            if pat in bracket_content or bracket_content == pat:
                if group == 'STARTO':
                    group = grp
                break
        if group == 'STARTO':
            for k in MEMBER_GROUP_FULL:
                if k in bracket_content:
                    group = MEMBER_GROUP_FULL[k]
                    break

    member = re.sub(r'\s*[\(（][^）)]+[）)]\s*', ' ', member).strip()
    # 「〜は東京でお留守番」のような注記は除去
    member = re.sub(r'は.*?(?:なし|不参加|お留守番).*', '', member).strip()
    # 既知メンバー名に照合
    matched_member = next((k for k in MEMBER_GROUP_FULL if k in member), None)
    if matched_member:
        member = matched_member
    elif len(member) > 15:  # 長すぎるのはメンバー名ではない
        member = None
    s['member_name'] = member or None

    # まずGROUP_NORMALIZEでタレント名・グループ名をチェック
    raw_member = (spot.get('member_name') or '').strip()
    for pattern, normalized in GROUP_NORMALIZE.items():
        if pattern in raw_member or pattern == raw_member:
            if group == 'STARTO':
                group = normalized
                break

    # メンバー名から推定
    if (not group or group == 'STARTO') and member:
        inferred = MEMBER_GROUP_FULL.get(member)
        if inferred:
            group = inferred

    # media_title からグループ名を推定
    if not group or group == 'STARTO':
        combined = f"{s.get('media_title') or ''} {s.get('menu_items') or ''} {name}"
        for keyword, grp in [
            ("Snow Man", "Snow Man"), ("SnowMan", "Snow Man"), ("すのちゅーぶ", "Snow Man"), ("すの", "Snow Man"),
            ("SixTONES", "SixTONES"), ("ストーンズ", "SixTONES"),
            ("なにわ男子", "なにわ男子"), ("なにわTube", "なにわ男子"),
            ("Aぇ", "Aぇ! group"), ("Aえ", "Aぇ! group"), ("末澤", "Aぇ! group"),
            ("WEST.", "WEST."), ("ジャニーズWEST", "WEST."),
            ("SUPER EIGHT", "SUPER EIGHT"), ("エイト", "SUPER EIGHT"),
            ("Travis Japan", "Travis Japan"), ("トラジャ", "Travis Japan"),
            ("King & Prince", "King & Prince"), ("キンプリ", "King & Prince"),
            ("Hey! Say! JUMP", "Hey! Say! JUMP"), ("HeySayJUMP", "Hey! Say! JUMP"),
            ("timelesz", "timelesz"), ("タイムレス", "timelesz"),
            ("Kis-My-Ft2", "Kis-My-Ft2"), ("キスマイ", "Kis-My-Ft2"),
            ("嵐", "嵐"), ("ARASHI", "嵐"),
            ("よにのちゃんねる", "よにのちゃんねる"), ("よにの", "よにのちゃんねる"),
            ("横山会", "横山会"),
            ("KAT-TUN", "KAT-TUN"),
            ("KinKi Kids", "KinKi Kids"),
            ("NEWS", "NEWS"),
            ("V6", "V6"),
        ]:
            if keyword in combined:
                group = grp
                break

    # member_name自体がグループ名の場合
    if not group or group == 'STARTO':
        raw = (spot.get('member_name') or '').strip().lstrip('@＠')
        for pat, grp in GROUP_NORMALIZE.items():
            if pat.strip('()（）') in raw or raw in pat:
                group = grp
                break

    # 嵐・KAT-TUN のMV・CMタイトルから推定
    if not group or group == 'STARTO':
        arashi_songs = {'復活LOVE','Power of the Paradise','Power ofthe','君のうた','Monster',
                        'truth','Beautiful world','Happiness','One Love','Love so sweet',
                        'Troublemaker','忍びの国','JAL先得','BRAVE','Face Down',
                        '10-10 Anniversary Tour'}
        kattun_songs = {'Turning Up','Keep the faith','Real Face','signifie',
                        'Rescue','GOLD','RUN FOR YOU'}
        combined_text = f"{s.get('media_title') or ''} {s.get('spot_name') or ''} {s.get('member_name') or ''}"
        if any(song in combined_text for song in arashi_songs):
            group = '嵐'
        elif any(song in combined_text for song in kattun_songs):
            group = 'KAT-TUN'

    # スポット名・タレント名に木村拓哉
    if not group or group == 'STARTO':
        if '木村拓哉' in (s.get('member_name') or '') or '木村拓哉' in (s.get('spot_name') or ''):
            group = '木村拓哉'

    s['group_name'] = group or 'STARTO'

    # media_title クリーン：先頭のグループ名バナーを除去
    mt = (s.get('media_title') or '').strip()
    for g in KNOWN_GROUPS:
        if mt.startswith(g):
            mt = mt[len(g):].strip()
    s['media_title'] = mt or None

    # broadcast_date：年月日を含む日付のみ残す
    bd = (s.get('broadcast_date') or '').strip()
    m = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日)', bd)
    if m:
        s['broadcast_date'] = m.group(1)
    else:
        s['broadcast_date'] = None

    return s


def deduplicate(spots: list) -> list:
    """スポット名＋グループ名の組み合わせで重複排除"""
    seen = set()
    unique = []
    for s in spots:
        name = s.get('spot_name', '').strip()
        group = s.get('group_name', '')
        key = (name, group)
        if key not in seen:
            seen.add(key)
            unique.append(s)
    return unique


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='DBに書き込まず確認のみ')
    parser.add_argument('--no-geocode', action='store_true', help='ジオコーディングをスキップ')
    parser.add_argument('--limit', type=int, default=0)
    args = parser.parse_args()

    if not OCR_RESULTS_PATH.exists():
        print(f"ERROR: {OCR_RESULTS_PATH} が見つかりません。先にocr_import.pyを実行してください。")
        return

    with open(OCR_RESULTS_PATH, encoding='utf-8') as f:
        ocr_results = json.load(f)

    print(f"OCR結果: {len(ocr_results)}件")

    # クリーンアップ + グループ推定
    cleaned = []
    for r in ocr_results:
        c = clean_spot(r)
        if c:
            cleaned.append(c)
    print(f"クリーンアップ後: {len(cleaned)}件（元{len(ocr_results)}件）")

    from collections import Counter
    still_starto = [s for s in cleaned if s.get('group_name') == 'STARTO']
    print(f"グループ未解決（STARTO残）: {len(still_starto)}件")
    print("グループ分布:")
    for g, cnt in Counter(s.get('group_name') for s in cleaned).most_common():
        print(f"  {g}: {cnt}")

    # 重複排除
    unique = deduplicate(cleaned)
    print(f"重複排除後: {len(unique)}件")

    if args.limit:
        unique = unique[:args.limit]

    if args.dry_run:
        print("\n=== DRY RUN: サンプル10件 ===")
        for r in unique[:10]:
            print(f"  {r.get('spot_name')} / {r.get('member_name')} / {r.get('group_name')} / {r.get('address', 'アドレスなし')}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 既存のInstagramデータを削除
    cur.execute("DELETE FROM spots WHERE source_url LIKE '%instagram%'")
    deleted = cur.rowcount
    print(f"既存Instagramデータ削除: {deleted}件")
    conn.commit()

    inserted = 0
    geocode_ok = 0
    geocode_fallback = 0

    for i, spot in enumerate(unique):
        spot_name = (spot.get('spot_name') or '').strip()
        member_name = (spot.get('member_name') or '').strip()
        group_name = (spot.get('group_name') or 'STARTO').strip()
        group_names = spot.get('group_names')
        media_title = (spot.get('media_title') or '').strip()
        broadcast_date = clean_broadcast_date(spot.get('broadcast_date', ''))
        menu_items = (spot.get('menu_items') or '').strip() or None
        address = (spot.get('address') or '').strip() or None
        access = (spot.get('access') or '').strip() or None

        # アクセスを住所に含める（表示用）
        full_address = address
        if address and access:
            full_address = f"{address} {access}"

        media_type = get_media_type(media_title)

        print(f"[{i+1}/{len(unique)}] {spot_name} / {member_name} / {group_name}", flush=True)

        lat, lng = None, None
        if not args.no_geocode:
            # エリアヒントを住所・媒体タイトルから取得
            area_hint = ""
            combined = f"{address or ''} {media_title or ''}"
            for area in AREA_COORDS:
                if area in combined:
                    area_hint = area
                    break

            coords = geocode(spot_name, address or '', area_hint)
            if coords:
                lat, lng = coords
                geocode_ok += 1
                print(f"  Geocoded: {lat:.4f}, {lng:.4f}", flush=True)
            else:
                lat, lng = get_area_coords(f"{address or ''} {media_title or ''}")
                geocode_fallback += 1
                print(f"  Fallback coords: {lat:.4f}, {lng:.4f}", flush=True)

            time.sleep(1.1)
        else:
            lat, lng = get_area_coords(f"{address or ''} {media_title or ''}")
            geocode_fallback += 1

        cur.execute("""
            INSERT INTO spots
                (name, address, lat, lng, talent_name, group_name, group_names,
                 media_type, media_title, broadcast_date, menu_items,
                 source_url, pineapple_score, freshness_visual)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            spot_name,
            full_address,
            lat, lng,
            member_name or None,
            group_name,
            group_names,
            media_type,
            media_title or None,
            broadcast_date,
            menu_items,
            "https://www.instagram.com/pineapple.neki/",
            5,
            None,
        ))
        inserted += 1

        if inserted % 20 == 0:
            conn.commit()
            print(f"  [Committed {inserted} records]", flush=True)

    conn.commit()
    print(f"\n完了！")
    print(f"  挿入: {inserted}件")
    print(f"  Geocode成功: {geocode_ok}件 / フォールバック: {geocode_fallback}件")


if __name__ == '__main__':
    main()
