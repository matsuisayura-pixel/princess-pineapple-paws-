from pydantic import BaseModel
from typing import Optional


class SpotOut(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    lat: float
    lng: float
    talent_name: Optional[str] = None
    group_name: Optional[str] = None
    group_names: Optional[str] = None  # JSON配列文字列
    media_type: Optional[str] = None
    media_title: Optional[str] = None
    broadcast_date: Optional[str] = None
    menu_items: Optional[str] = None
    access_info: Optional[str] = None
    source_url: Optional[str] = None
    pineapple_score: Optional[int] = 50
    freshness_visual: Optional[str] = "ripe"

    model_config = {"from_attributes": True}


class SpotWithDistance(SpotOut):
    distance_meters: Optional[float] = None


class UserPostOut(BaseModel):
    id: int
    spot_id: int
    image_path: str
    media_title: Optional[str] = None
    description: Optional[str] = None
    nickname: Optional[str] = None
    created_at: str

    model_config = {"from_attributes": True}


class SpotSubmissionOut(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    media_title: Optional[str] = None
    description: Optional[str] = None
    image_path: Optional[str] = None
    nickname: Optional[str] = None
    created_at: str
    status: str = "pending"

    model_config = {"from_attributes": True}


class TalentListResponse(BaseModel):
    talents: list[str]
    groups: list[str]
