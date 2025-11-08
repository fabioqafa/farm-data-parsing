# app/db.py
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Resolve to the project root (one level up from app/)
BASE_DIR = Path(__file__).resolve().parent.parent
DATABASE_URL = f"sqlite:///{(BASE_DIR / 'farms.db').as_posix()}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# FastAPI dep
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
