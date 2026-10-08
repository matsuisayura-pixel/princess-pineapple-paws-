from sqlalchemy import Column, Integer, String, Float, Text
from database import Base, UserBase


class UserPost(UserBase):
    __tablename__ = "user_posts"

    id = Column(Integer, primary_key=True, index=True)
    spot_id = Column(Integer, nullable=False, index=True)
    image_path = Column(String, nullable=False)
    media_title = Column(String)
    description = Column(Text)
    nickname = Column(String)
    created_at = Column(String)


class SpotSubmission(UserBase):
    __tablename__ = "spot_submissions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    address = Column(String)
    media_title = Column(String)
    description = Column(Text)
    image_path = Column(String)
    nickname = Column(String)
    created_at = Column(String)
    status = Column(String, default="pending")


class Spot(Base):
    __tablename__ = "spots"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    address = Column(String)
    lat = Column(Float, nullable=False)
    lng = Column(Float, nullable=False)
    talent_name = Column(String)
    group_name = Column(String)
    group_names = Column(Text)
    media_type = Column(String)
    media_title = Column(String)
    broadcast_date = Column(String)
    menu_items = Column(Text)
    access_info = Column(Text)
    source_url = Column(String)
    pineapple_score = Column(Integer, default=50)
    freshness_visual = Column(String, default="ripe")
