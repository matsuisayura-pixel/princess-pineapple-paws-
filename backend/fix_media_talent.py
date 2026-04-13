#!/usr/bin/env python3
"""
media_title と talent_name の一括修正
- media_title: OCRのまま入っている説明文から番組名だけを抽出
- talent_name: 同グループの複数メンバーが登場する場合は全員を記載
"""
import sqlite3, sys, re
sys.stdout.reconfigure(encoding='utf-8')

DB = 'backend/data/pineapple_paws.db'

# グループ → メンバー リスト
MEMBER_GROUP = {
    '渡辺翔太':'Snow Man','深澤辰哉':'Snow Man','阿部亮平':'Snow Man','向井康二':'Snow Man',
    '目黒蓮':'Snow Man','ラウール':'Snow Man','岩本照':'Snow Man','宮舘涼太':'Snow Man','佐久間大介':'Snow Man',
    '森本慎太郎':'SixTONES','田中樹':'SixTONES','京本大我':'SixTONES','髙地優吾':'SixTONES',
    'ジェシー':'SixTONES','松村北斗':'SixTONES',
    '大橋和也':'なにわ男子','藤原丈一郎':'なにわ男子','長尾謙杜':'なにわ男子',
    '大西流星':'なにわ男子','西畑大吾':'なにわ男子','道枝駿佑':'なにわ男子','高橋恭平':'なにわ男子',
    '小島健':'Aぇ! group','末澤誠也':'Aぇ! group','佐野晶哉':'Aぇ! group','福本大晴':'Aぇ! group',
    '正門良規':'Aぇ! group','草間リチャード敬太':'Aぇ! group',
    '相葉雅紀':'嵐','大野智':'嵐','櫻井翔':'嵐','二宮和也':'嵐','松本潤':'嵐',
    '重岡大毅':'WEST.','神山智洋':'WEST.','小瀧望':'WEST.','濱田崇裕':'WEST.',
    '桐山照史':'WEST.','中間淳太':'WEST.',
    '永瀬廉':'King & Prince','高橋海人':'King & Prince',
    '山田涼介':'Hey! Say! JUMP','知念侑李':'Hey! Say! JUMP','岡本圭人':'Hey! Say! JUMP',
    '中島裕翔':'Hey! Say! JUMP','有岡大貴':'Hey! Say! JUMP','薮宏太':'Hey! Say! JUMP',
    '八乙女光':'Hey! Say! JUMP','伊野尾慧':'Hey! Say! JUMP','髙木雄也':'Hey! Say! JUMP',
    '松田元太':'Travis Japan','川島如恵留':'Travis Japan','中村海人':'Travis Japan',
    '七五三掛龍也':'Travis Japan','吉澤閑也':'Travis Japan','宮近海斗':'Travis Japan',
    'カラニ・アキナ':'Travis Japan',
    '丸山隆平':'SUPER EIGHT','安田章大':'SUPER EIGHT','大倉忠義':'SUPER EIGHT',
    '村上信五':'SUPER EIGHT','横山裕':'SUPER EIGHT','錦戸亮':'SUPER EIGHT',
    '増田貴久':'NEWS','加藤シゲアキ':'NEWS','小山慶一郎':'NEWS',
    '堂本光一':'KinKi Kids','堂本剛':'KinKi Kids',
    '菊池風磨':'timelesz','松島聡':'timelesz','佐藤勝利':'timelesz',
    '中島健人':'中島健人','上田竜也':'上田竜也','内博貴':'内博貴','木村拓哉':'木村拓哉',
}

GROUPS = set(MEMBER_GROUP.values())
GROUP_RE = re.compile(r'\((' + '|'.join(re.escape(g) for g in GROUPS) + r')\)')

# 番組名の OCR 誤字修正
OCR_FIX = {
    '川': '!',
    'Heyl Say': 'Hey! Say',
    'l Say': '! Say',
}

def fix_ocr(text):
    for bad, good in OCR_FIX.items():
        text = text.replace(bad, good)
    return text

def extract_program_name(media):
    """「番組名」パターンから番組名を抽出"""
    if not media: return None
    m = re.search(r'[「『]([^」』]{2,40})[」』]', media)
    if m:
        prog = m.group(1).strip()
        prog = fix_ocr(prog)
        return prog
    return None

def find_same_group_members(text, primary_group):
    """テキストから同グループのメンバー名を全て抽出"""
    found = []
    for member, group in MEMBER_GROUP.items():
        if group == primary_group and member in text and member not in found:
            found.append(member)
    return found

def is_raw_ocr_description(media):
    """OCRそのままの説明文かどうか判定"""
    return bool(GROUP_RE.search(media or ''))

def build_clean_media_title(media, prog):
    """番組名が取れた場合はそれを、取れない場合はよりクリーンなタイトルを返す"""
    if prog:
        return prog
    if not media: return None
    # よにのちゃんねる系
    m = re.search(r'よにのちゃんねる\s*(#\d+)?', media)
    if m: return 'よにのちゃんねる' + ((' ' + m.group(1)) if m.group(1) else '')
    # ストチューブ系
    if 'ストチューブ' in media: return 'ストチューブ'
    # ジャにのちゃんねる系
    m = re.search(r'ジャにのちゃんねる\s*(#\d+)?', media)
    if m: return 'ジャにのちゃんねる' + ((' ' + m.group(1)) if m.group(1) else '')
    # 公式Instagram / SNS系はそのまま
    if '公式Instagram' in media or 'Instagram' in media: return None
    # その他: 最初の有意な部分を返す
    return None

def build_talent_name(current_talent, same_group_members):
    """複数メンバーを「・」区切りで結合"""
    if not same_group_members:
        return current_talent
    # current_talentが既にメンバー名なら結合リストに追加
    all_members = list(same_group_members)
    if current_talent and current_talent in MEMBER_GROUP and current_talent not in all_members:
        all_members.insert(0, current_talent)
    if len(all_members) == 0:
        return current_talent
    if len(all_members) == 1:
        return all_members[0]
    return '・'.join(all_members)

def main():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute('SELECT id, group_name, media_title, talent_name, menu_items, media_type FROM spots')
    rows = c.fetchall()

    media_fixed = 0
    talent_fixed = 0
    preview = []

    for sid, group, media, talent, menu, mtype in rows:
        new_media = None
        new_talent = None

        if is_raw_ocr_description(media):
            # media_title 修正
            prog = extract_program_name(media)
            clean = build_clean_media_title(media, prog)
            if clean:
                new_media = clean

            # talent_name 修正: 同グループメンバーを全員抽出
            combined = (media or '') + ' ' + (menu or '')
            same_members = find_same_group_members(combined, group)
            if same_members:
                candidate = build_talent_name(talent, same_members)
                if candidate and candidate != talent:
                    new_talent = candidate

        if new_media or new_talent:
            preview.append({
                'id': sid, 'group': group,
                'old_media': media, 'new_media': new_media,
                'old_talent': talent, 'new_talent': new_talent,
            })

    # プレビュー表示
    print(f'=== メディアタイトル修正候補: {sum(1 for p in preview if p["new_media"])}件 ===')
    for p in preview:
        if p['new_media']:
            print(f'  id={p["id"]} [{p["group"]}]')
            print(f'    旧: {(p["old_media"] or "")[:70]}')
            print(f'    新: {p["new_media"]}')

    print(f'\n=== タレント名修正候補: {sum(1 for p in preview if p["new_talent"])}件 ===')
    for p in preview:
        if p['new_talent']:
            print(f'  id={p["id"]} [{p["group"]}] {p["old_talent"]} → {p["new_talent"]}')

    # 確認なしで適用
    print('\n適用中...')
    for p in preview:
        if p['new_media']:
            c.execute('UPDATE spots SET media_title=? WHERE id=?', (p['new_media'], p['id']))
            media_fixed += 1
        if p['new_talent']:
            c.execute('UPDATE spots SET talent_name=? WHERE id=?', (p['new_talent'], p['id']))
            talent_fixed += 1

    conn.commit()
    print(f'\n完了: media_title={media_fixed}件, talent_name={talent_fixed}件 修正')
    conn.close()

if __name__ == '__main__':
    main()
