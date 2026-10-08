import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# スポットDB（Git管理・毎日自動更新）
DB_PATH = DATA_DIR / "pineapple_paws.db"

# ユーザーデータDB（Fly Volume永続・投稿・申請）
USER_DATA_DIR = Path(os.getenv("USER_DATA_DIR", str(DATA_DIR)))
USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
USER_DB_PATH = USER_DATA_DIR / "user_data.db"

DEFAULT_SEARCH_RADIUS_M = 5000
MAX_NEARBY_RESULTS = 50

# Cloudflare R2（画像ストレージ）
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "pineapple-paws-uploads")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")
