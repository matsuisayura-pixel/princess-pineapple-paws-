"""
Instagramキャプションから聖地情報をパースしてDBにインポート

キャプション形式:
  【タイトル】
  説明文...
  1️⃣ 店名
  説明（誰が、何を、どの番組）
  2️⃣ 店名
  ...
  #タグ #タグ
"""
import re
import json
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "data" / "pineapple_paws.db"
POSTS_JSON = Path(__file__).parent.parent / "instagram_posts_full.json"
REELS_JSON = Path(__file__).parent.parent / "instagram_reels_full.json"

# グループ検出キーワード
GROUP_KEYWORDS = [
    ("横山会",      ["SUPER EIGHT", "WEST.", "Aぇ! group", "なにわ男子"]),
    ("よにの",      ["嵐", "KAT-TUN", "Hey! Say! JUMP", "timelesz"]),
    ("Snow Man",    ["Snow Man"]), ("SnowMan", ["Snow Man"]), ("スノ", ["Snow Man"]),
    ("SixTONES",    ["SixTONES"]), ("スト",    ["SixTONES"]),
    ("なにわ男子",  ["なにわ男子"]), ("なにわ",  ["なにわ男子"]),
    ("Aぇ! group",  ["Aぇ! group"]), ("Aぇgroup", ["Aぇ! group"]), ("Aぇ",  ["Aぇ! group"]),
    ("WEST.",       ["WEST."]),
    ("Travis Japan",["Travis Japan"]), ("トラジャ", ["Travis Japan"]),
    ("King & Prince",["King & Prince"]), ("キンプリ", ["King & Prince"]),
    ("Kis-My-Ft2",  ["Kis-My-Ft2"]), ("キスマイ", ["Kis-My-Ft2"]),
    ("SUPER EIGHT", ["SUPER EIGHT"]), ("エイト",  ["SUPER EIGHT"]),
    ("timelesz",    ["timelesz"]),
    ("嵐",          ["嵐"]),
    ("KAT-TUN",     ["KAT-TUN"]),
    ("Hey! Say! JUMP", ["Hey! Say! JUMP"]), ("JUMP", ["Hey! Say! JUMP"]),
    ("KinKi Kids",  ["KinKi Kids"]),
    ("NEWS",        ["NEWS"]),
]

MEMBER_GROUP = {
    # Snow Man
    "岩本照": "Snow Man", "深澤辰哉": "Snow Man", "ラウール": "Snow Man",
    "渡辺翔太": "Snow Man", "阿部亮平": "Snow Man", "宮舘涼太": "Snow Man",
    "目黒蓮": "Snow Man", "向井康二": "Snow Man", "佐久間大介": "Snow Man",
    "翔太": "Snow Man", "ラウ": "Snow Man", "めめ": "Snow Man",
    # SixTONES
    "ジェシー": "SixTONES", "京本大我": "SixTONES", "松村北斗": "SixTONES",
    "田中樹": "SixTONES", "森本慎太郎": "SixTONES", "髙地優吾": "SixTONES",
    "京ちゃん": "SixTONES", "じゅり": "SixTONES",
    # なにわ男子
    "道枝駿佑": "なにわ男子", "高橋恭平": "なにわ男子", "西畑大吾": "なにわ男子",
    "大西流星": "なにわ男子", "藤原丈一郎": "なにわ男子", "長尾謙杜": "なにわ男子",
    "大橋和也": "なにわ男子", "丈くん": "なにわ男子", "流星": "なにわ男子",
    "りんたろー": "なにわ男子",
    # Aぇ! group
    "末澤誠也": "Aぇ! group", "佐野晶哉": "Aぇ! group", "福本大晴": "Aぇ! group",
    "草間リチャード敬太": "Aぇ! group", "小島健": "Aぇ! group", "中島颯太": "Aぇ! group",
    "こじけん": "Aぇ! group", "末澤": "Aぇ! group", "佐野": "Aぇ! group",
    # WEST.
    "桐山照史": "WEST.", "濵田崇裕": "WEST.", "小瀧望": "WEST.",
    "中間淳太": "WEST.", "藤井流星": "WEST.", "神山智洋": "WEST.",
    "重岡大毅": "WEST.", "重岡くん": "WEST.",
    # King & Prince
    "永瀬廉": "King & Prince", "高橋海人": "King & Prince",
    "廉くん": "King & Prince",
    # Travis Japan
    "松田元太": "Travis Japan", "吉澤閑也": "Travis Japan", "宮近海斗": "Travis Japan",
    "七五三掛龍也": "Travis Japan", "川島如恵留": "Travis Japan",
    "中村海人": "Travis Japan", "松倉海斗": "Travis Japan",
    # SUPER EIGHT
    "横山裕": "SUPER EIGHT", "村上信五": "SUPER EIGHT",
    "丸山隆平": "SUPER EIGHT", "安田章大": "SUPER EIGHT", "大倉忠義": "SUPER EIGHT",
    "丸ちゃん": "SUPER EIGHT",
    # timelesz
    "菊池風磨": "timelesz", "中島健人": "timelesz", "松島聡": "timelesz",
    "佐藤勝利": "timelesz", "橋本康也": "timelesz",
    "風磨くん": "timelesz", "勝利くん": "timelesz", "橋本くん": "timelesz",
    "山下くん": "timelesz",
    # 嵐
    "二宮和也": "嵐", "大野智": "嵐", "相葉雅紀": "嵐", "松本潤": "嵐", "櫻井翔": "嵐",
    "翔くん": "嵐", "潤くん": "嵐", "智くん": "嵐",
    # KAT-TUN
    "中丸雄一": "KAT-TUN", "亀梨和也": "KAT-TUN", "上田竜也": "KAT-TUN",
    # Hey! Say! JUMP
    "山田涼介": "Hey! Say! JUMP", "中島裕翔": "Hey! Say! JUMP", "知念侑李": "Hey! Say! JUMP",
    "有岡大貴": "Hey! Say! JUMP",
    # KinKi Kids
    "堂本光一": "KinKi Kids", "堂本剛": "KinKi Kids",
}

# 番組名を番組キーワードから検出
MEDIA_KEYWORDS = {
    "すのちゅーぶ": ("YouTube", "すのちゅーぶ"),
    "それスノ": ("YouTube", "それSnow Man にやらせてください"),
    "なにわTube": ("YouTube", "なにわ男子チャンネル"),
    "よにのちゃんねる": ("YouTube", "よにのちゃんねる"),
    "横山会": ("YouTube", "横山会"),
    "インスタライブ": ("インスタライブ", "インスタライブ"),
    "タイムレスマン": ("TV", "タイムレスマン"),
    "ドデスカ": ("TV", "ドデスカ！"),
    "アナザースカイ": ("TV", "アナザースカイ"),
    "ドラマ": ("TV", "ドラマ"),
    "MV": ("MV", "MV"),
    "YouTube": ("YouTube", "YouTube"),
}

# 番号絵文字パターン
NUMBER_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣"]


def parse_groups(text: str) -> list[str]:
    groups = []
    seen = set()
    for kw, group_list in GROUP_KEYWORDS:
        if kw in text:
            for g in group_list:
                if g not in seen:
                    groups.append(g)
                    seen.add(g)
    return groups


def parse_member(text: str) -> str | None:
    for name, group in MEMBER_GROUP.items():
        if name in text:
            return name
    return None


def parse_media(text: str):
    for kw, (mtype, mtitle) in MEDIA_KEYWORDS.items():
        if kw in text:
            return mtype, mtitle
    return "SNS", "パイナップルネキ"


def parse_spots_from_caption(caption: str, post_date: str, source: str) -> list[dict]:
    """キャプションから個別スポット情報をパース"""
    spots = []

    # タイトル行（最初の【...】）
    title_match = re.search(r'【(.+?)】', caption)
    post_title = title_match.group(1) if title_match else ""

    # ハッシュタグ行からグループを取得
    hashtags = " ".join(re.findall(r'#\S+', caption))
    post_groups = parse_groups(caption + " " + hashtags)

    # 番号絵文字でスポットを分割
    lines = caption.split('\n')
    current_spot = None
    current_desc = []

    for line in lines:
        line = line.strip()
        found_num = False
        for emoji in NUMBER_EMOJIS:
            if line.startswith(emoji):
                # 前のスポットを保存
                if current_spot:
                    spots.append({
                        "name": current_spot,
                        "desc": " ".join(current_desc),
                        "post_groups": post_groups,
                        "post_title": post_title,
                        "post_date": post_date,
                        "source": source,
                    })
                # 新しいスポット
                current_spot = line[len(emoji):].strip()
                current_desc = []
                found_num = True
                break

        if not found_num and current_spot and line and not line.startswith('#'):
            if line not in ["アクセスや詳細は画像をスワイプしてチェック📸",
                            "詳しいアクセスや詳細は画像をスワイプしてチェックしてね📸",
                            "詳しいアクセスや詳細は画像をスワイプしてチェック📸"]:
                current_desc.append(line)

    # 最後のスポット
    if current_spot:
        spots.append({
            "name": current_spot,
            "desc": " ".join(current_desc),
            "post_groups": post_groups,
            "post_title": post_title,
            "post_date": post_date,
            "source": source,
        })

    return spots


def parse_date(date_str: str) -> str:
    """「3月 15, 2026 2:35 AM」→ YYYY-MM-DD"""
    try:
        # 日本語月名を英語に変換
        months = {"1月": "January", "2月": "February", "3月": "March",
                  "4月": "April", "5月": "May", "6月": "June",
                  "7月": "July", "8月": "August", "9月": "September",
                  "10月": "October", "11月": "November", "12月": "December"}
        for jp, en in months.items():
            date_str = date_str.replace(jp, en)
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y %I:%M %p")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return date_str


def main():
    posts = []
    for src in [POSTS_JSON, REELS_JSON]:
        if src.exists():
            with open(src, encoding="utf-8") as f:
                posts.extend(json.load(f))
    print(f"投稿合計: {len(posts)}件")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 既存のInstagramデータを全削除して再インポート
    cur.execute("DELETE FROM spots WHERE source_url LIKE '%instagram%'")
    deleted = cur.rowcount
    print(f"既存Instagramデータ削除: {deleted}件")

    # 既存スポット名一覧（重複チェック用）
    cur.execute("SELECT name, group_name FROM spots")
    existing = {}
    for row in cur.fetchall():
        existing.setdefault(row["name"], []).append(row["group_name"])

    inserted = 0
    skipped = 0
    all_spots = []

    for post in posts:
        date = parse_date(post["date"])
        source = "https://www.instagram.com/pineapple.neki/"
        caption = post["caption"]

        spot_entries = parse_spots_from_caption(caption, date, source)

        for s in spot_entries:
            name = s["name"]
            desc = s["desc"]
            groups = s["post_groups"]
            post_title = s["post_title"]

            if not name or len(name) < 2:
                continue

            # メンバー解析
            member = parse_member(desc + " " + caption)
            # 番組解析
            media_type, media_title = parse_media(desc + " " + caption)

            # メニュー情報（説明文から抽出）
            menu_items = desc if desc else None

            primary_group = groups[0] if groups else "STARTO"

            # 重複チェック
            if name in existing:
                existing_groups = existing[name]
                if primary_group in existing_groups or any(g in existing_groups for g in groups):
                    skipped += 1
                    continue

            all_spots.append({
                "name": name,
                "group_name": primary_group,
                "group_names": json.dumps(groups, ensure_ascii=False),
                "talent_name": member,
                "media_type": media_type,
                "media_title": post_title,
                "broadcast_date": date,
                "menu_items": menu_items,
                "source_url": source,
            })

    # 座標は後でgeocoding（今は東京デフォルト）
    # 実際にはNominatimで取得するが、まず構造を確認
    print(f"\nパース結果: {len(all_spots)}スポット / スキップ: {skipped}件")

    sample_file = Path(__file__).parent.parent / "instagram_spots_parsed.json"
    with open(sample_file, "w", encoding="utf-8") as f:
        json.dump(all_spots, f, ensure_ascii=False, indent=2)

    # ======== エリア座標マップ（フォールバック） ========
    AREA_COORDS = {
        "札幌": (43.0642, 141.3469), "北海道": (43.0642, 141.3469),
        "東京": (35.6762, 139.6503), "都内": (35.6762, 139.6503),
        "新宿": (35.6938, 139.7036), "渋谷": (35.6580, 139.7016),
        "浅草": (35.7147, 139.7966), "六本木": (35.6628, 139.7317),
        "横浜": (35.4437, 139.6380), "川越": (35.9252, 139.4856),
        "千葉": (35.6074, 140.1065),
        "大阪": (34.6937, 135.5023), "梅田": (34.7026, 135.4975),
        "新大阪": (34.7334, 135.5003), "難波": (34.6686, 135.5026),
        "京都": (35.0116, 135.7681),
        "名古屋": (35.1815, 136.9066), "愛知": (35.1815, 136.9066),
        "神戸": (34.6901, 135.1956), "兵庫": (34.6901, 135.1956),
        "福岡": (33.5904, 130.4017),
        "仙台": (38.2688, 140.8721),
        "東海道": (35.0, 137.0), "静岡": (34.9769, 138.3831),
        "浜松": (34.7108, 137.7268),
        "三重": (34.7303, 136.5086), "伊勢": (34.4922, 136.7066),
        "松阪": (34.5786, 136.5269),
        "多摩": (35.6470, 139.4482),
        "川崎": (35.5311, 139.7029),
        "埼玉": (35.8574, 139.6489),
        "旭川": (43.7707, 142.3650),
    }

    def get_area_coords(title: str):
        for area, coords in AREA_COORDS.items():
            if area in title:
                return coords
        return (35.6762, 139.6503)  # デフォルト東京

    # ======== Nominatimジオコーディング ========
    import time, urllib.request, urllib.parse

    def geocode(name: str, area: str = "") -> tuple[float, float] | None:
        query = f"{name} {area}".strip()
        params = urllib.parse.urlencode({
            "q": query, "format": "json", "limit": 1,
            "accept-language": "ja", "countrycodes": "jp"
        })
        url = f"https://nominatim.openstreetmap.org/search?{params}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "pineapple-seichi/1.0"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception:
            pass
        return None

    # ======== DBへINSERT ========
    inserted = 0
    geocode_ok = 0
    geocode_fallback = 0

    for s in all_spots:
        area_for_geocode = ""
        for area in AREA_COORDS:
            if area in s.get("media_title", ""):
                area_for_geocode = area
                break

        # Nominatimでジオコーディング
        time.sleep(1.1)
        coords = geocode(s["name"], area_for_geocode)
        if coords:
            lat, lng = coords
            geocode_ok += 1
        else:
            lat, lng = get_area_coords(s.get("media_title", ""))
            geocode_fallback += 1

        cur.execute("""
            INSERT INTO spots
                (name, address, lat, lng, talent_name, group_name, group_names,
                 media_type, media_title, broadcast_date, menu_items,
                 source_url, pineapple_score, freshness_visual)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            s["name"], None, lat, lng,
            s.get("talent_name"), s["group_name"], s["group_names"],
            s["media_type"], s["media_title"], s["broadcast_date"],
            s.get("menu_items"), s["source_url"], 65, "ripe"
        ))
        inserted += 1
        if inserted % 10 == 0:
            print(f"  {inserted}/{len(all_spots)}...")

    conn.commit()
    conn.close()

    print(f"\n完了: {inserted}件追加 (Nominatim成功:{geocode_ok} / フォールバック:{geocode_fallback})")


if __name__ == "__main__":
    main()
