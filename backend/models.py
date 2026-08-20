from sqlalchemy import Column, Integer, String, Float, Text
from database import Base


class UserPost(Base):
    __tablename__ = "user_posts"

    id = Column(Integer, primary_key=True, index=True)
    spot_id = Column(Integer, nullable=False, index=True)
    image_path = Column(String, nullable=False)   # /uploads/filename.jpg
    media_title = Column(String)                   # 作品名・番組名（ユーザー入力）
    description = Column(Text)                     # 食べたもの・詳しい説明
    nickname = Column(String)                      # 省略時はNone → 表示は「匿名さん」
    created_at = Column(String)                    # ISO datetime string


class SpotSubmission(Base):
    """ユーザーから申請された新規聖地（審査待ち）"""
    __tablename__ = "spot_submissions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)     # スポット名（必須）
    address = Column(String)                   # 住所（任意）
    media_title = Column(String)              # 作品名・番組名
    description = Column(Text)               # 説明・食べたもの
    image_path = Column(String)              # /uploads/filename.jpg（任意）
    nickname = Column(String)                # 投稿者名
    created_at = Column(String)
    status = Column(String, default="pending")  # pending / approved / rejected


class Spot(Base):
    __tablename__ = "spots"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)          # 店名・スポット名
    address = Column(String)                        # 住所
    lat = Column(Float, nullable=False)             # 緯度
    lng = Column(Float, nullable=False)             # 経度
    talent_name = Column(String)                    # タレント名
    group_name = Column(String)                     # グループ名（主・表示用）
    group_names = Column(Text)                      # グループ名リスト JSON配列 例: ["Snow Man","なにわ男子"]
    media_type = Column(String)                     # TV / YouTube / 雑誌 / SNS
    media_title = Column(String)                    # 番組名・メディアタイトル
    broadcast_date = Column(String)                 # 放送・掲載日 (YYYY-MM-DD)
    menu_items = Column(Text)                       # 食べたメニュー
    access_info = Column(Text)                      # アクセス情報
    source_url = Column(String)                     # 情報ソースURL
    pineapple_score = Column(Integer, default=50)   # 鮮度スコア (0-100)
    freshness_visual = Column(String, default="ripe")  # fresh / ripe / dry
