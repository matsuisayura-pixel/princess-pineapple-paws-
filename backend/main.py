import shutil
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, Query, Depends, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

from config import DEFAULT_SEARCH_RADIUS_M, MAX_NEARBY_RESULTS
from database import init_db, get_db
from models import Spot, UserPost, SpotSubmission
from schemas import SpotOut, SpotWithDistance, TalentListResponse, UserPostOut, SpotSubmissionOut
from services.geo import haversine

UPLOADS_DIR = Path(__file__).parent / "uploads"
ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    UPLOADS_DIR.mkdir(exist_ok=True)
    init_db()
    yield


app = FastAPI(
    title="Princess Pineapple Paws API",
    description="STARTO聖地巡礼・現在地連動マップ",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")


@app.get("/health")
def health():
    return {"status": "ok", "app": "Princess Pineapple Paws"}


@app.get("/spots/nearby", response_model=list[SpotWithDistance])
def nearby_spots(
    lat: float = Query(..., description="現在地 緯度"),
    lng: float = Query(..., description="現在地 経度"),
    radius: float = Query(DEFAULT_SEARCH_RADIUS_M, description="検索半径 (メートル)"),
    talent: Optional[str] = Query(None),
    group: Optional[str] = Query(None),
    media: Optional[str] = Query(None),
    freshness: Optional[str] = Query(None, description="fresh / ripe / dry"),
    db: Session = Depends(get_db),
):
    query = db.query(Spot)
    if talent:
        query = query.filter(Spot.talent_name.ilike(f"%{talent}%"))
    if group:
        query = query.filter(
            Spot.group_name.ilike(f"%{group}%") |
            Spot.group_names.ilike(f"%{group}%")
        )
    if media:
        query = query.filter(Spot.media_type == media)
    if freshness:
        query = query.filter(Spot.freshness_visual == freshness)

    results: list[SpotWithDistance] = []
    for spot in query.all():
        dist = haversine(lat, lng, spot.lat, spot.lng)
        if dist <= radius:
            s = SpotWithDistance.model_validate(spot)
            s.distance_meters = round(dist)
            results.append(s)

    results.sort(key=lambda x: x.distance_meters or 0)
    return results[:MAX_NEARBY_RESULTS]


@app.get("/spots", response_model=list[SpotOut])
def all_spots(
    talent: Optional[str] = Query(None),
    group: Optional[str] = Query(None),
    media: Optional[str] = Query(None),
    freshness: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    query = db.query(Spot)
    if talent:
        query = query.filter(Spot.talent_name.ilike(f"%{talent}%"))
    if group:
        query = query.filter(
            Spot.group_name.ilike(f"%{group}%") |
            Spot.group_names.ilike(f"%{group}%")
        )
    if media:
        query = query.filter(Spot.media_type == media)
    if freshness:
        query = query.filter(Spot.freshness_visual == freshness)
    return query.all()


@app.get("/spots/{spot_id}", response_model=SpotOut)
def get_spot(spot_id: int, db: Session = Depends(get_db)):
    spot = db.query(Spot).filter(Spot.id == spot_id).first()
    if not spot:
        raise HTTPException(status_code=404, detail="Spot not found")
    return spot


# ===== ユーザー投稿 =====

@app.get("/spots/{spot_id}/posts", response_model=list[UserPostOut])
def get_spot_posts(spot_id: int, db: Session = Depends(get_db)):
    return (
        db.query(UserPost)
        .filter(UserPost.spot_id == spot_id)
        .order_by(UserPost.id.desc())
        .all()
    )


@app.post("/spots/{spot_id}/posts", response_model=UserPostOut)
async def create_spot_post(
    spot_id: int,
    file: UploadFile = File(...),
    comment: str = Form(""),
    nickname: str = Form(""),
    db: Session = Depends(get_db),
):
    if not db.query(Spot).filter(Spot.id == spot_id).first():
        raise HTTPException(status_code=404, detail="Spot not found")

    ext = Path(file.filename or "").suffix.lower()
    if ext not in ALLOWED_EXTS:
        raise HTTPException(status_code=400, detail="画像ファイル（jpg/png/gif/webp）のみアップロード可能です")

    filename = f"{uuid.uuid4().hex}{ext}"
    dest = UPLOADS_DIR / filename
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    post = UserPost(
        spot_id=spot_id,
        image_path=f"/uploads/{filename}",
        description=comment.strip() or None,   # commentをdescriptionに格納
        nickname=nickname.strip() or None,
        created_at=datetime.now().isoformat(),
    )
    db.add(post)
    db.commit()
    db.refresh(post)
    return post


# ===== 新規聖地申請 =====

@app.post("/spot-submissions", response_model=SpotSubmissionOut)
async def create_spot_submission(
    name: str = Form(...),
    address: str = Form(""),
    media_title: str = Form(""),
    description: str = Form(""),
    nickname: str = Form(""),
    file: Optional[UploadFile] = File(default=None),
    db: Session = Depends(get_db),
):
    image_path = None
    if file and file.filename:
        ext = Path(file.filename).suffix.lower()
        if ext not in ALLOWED_EXTS:
            raise HTTPException(status_code=400, detail="画像ファイル（jpg/png/gif/webp）のみアップロード可能です")
        filename = f"sub_{uuid.uuid4().hex}{ext}"
        dest = UPLOADS_DIR / filename
        with dest.open("wb") as f:
            shutil.copyfileobj(file.file, f)
        image_path = f"/uploads/{filename}"

    sub = SpotSubmission(
        name=name.strip(),
        address=address.strip() or None,
        media_title=media_title.strip() or None,
        description=description.strip() or None,
        image_path=image_path,
        nickname=nickname.strip() or None,
        created_at=datetime.now().isoformat(),
        status="pending",
    )
    db.add(sub)
    db.commit()
    db.refresh(sub)
    return sub


@app.get("/talents", response_model=TalentListResponse)
def list_talents(db: Session = Depends(get_db)):
    rows = db.query(Spot.talent_name, Spot.group_name).distinct().all()
    talents = sorted({r.talent_name for r in rows if r.talent_name})
    groups = sorted({r.group_name for r in rows if r.group_name})
    return TalentListResponse(talents=talents, groups=groups)


@app.get("/")
def index():
    frontend = Path(__file__).parent.parent / "frontend" / "index.html"
    return FileResponse(str(frontend))
