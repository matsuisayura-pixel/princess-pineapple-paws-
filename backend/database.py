from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from config import DB_PATH, USER_DB_PATH

# スポットDB（Git管理）
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ユーザーデータDB（Fly Volume永続）
user_engine = create_engine(f"sqlite:///{USER_DB_PATH}", connect_args={"check_same_thread": False})
UserSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=user_engine)


class Base(DeclarativeBase):
    pass


class UserBase(DeclarativeBase):
    pass


def init_db():
    from models import Spot  # noqa: F401
    Base.metadata.create_all(bind=engine)


def init_user_db():
    from models import UserPost, SpotSubmission  # noqa: F401
    UserBase.metadata.create_all(bind=user_engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_user_db():
    db = UserSessionLocal()
    try:
        yield db
    finally:
        db.close()
