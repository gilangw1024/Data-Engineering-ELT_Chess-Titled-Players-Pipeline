"""
Transform Module - Chess Pipeline
Mengambil data mentah dari MongoDB (Bronze), melakukan validasi & cleaning,
lalu menyimpannya ke format Parquet untuk diteruskan ke Silver layer.
"""
import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pandas as pd
from pydantic import BaseModel, field_validator
from pymongo import MongoClient

# Memuat file .env jika dijalankan lokal
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic Schema (V2)
# ---------------------------------------------------------------------------
class ChessGameSchema(BaseModel):
    game_id: str = ""
    url: str = ""
    pgn: str = ""
    white_username: str = "unknown"
    black_username: str = "unknown"
    white_rating: int = 0
    black_rating: int = 0
    time_class: str = "blitz"
    rules: str = "chess"
    end_time: datetime
    rating_diff: int = 0
    outcome: str = "draw"
    duration_minutes: float = 0.0
    source_username: Optional[str] = None
    source_period: Optional[str] = None
    extraction_timestamp: Optional[datetime] = None

    @field_validator('time_class')
    @classmethod
    def validate_time_class(cls, v):
        return v.lower() if isinstance(v, str) else v

    @field_validator('end_time')
    @classmethod
    def validate_end_time(cls, v):
        if v > datetime.now():
            raise ValueError("End time cannot be in the future")
        return v


# ---------------------------------------------------------------------------
# MongoDB Helper
# ---------------------------------------------------------------------------
def get_mongo_client() -> MongoClient:
    """Membuat koneksi ke MongoDB tanpa hardcoded credentials."""
    mongo_uri = os.getenv('MONGO_URI')
    if not mongo_uri:
        user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
        password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
        host = os.getenv('MONGO_HOST')
        port = os.getenv('MONGO_PORT')
        
        if not all([user, password, host, port]):
            raise ValueError(
                "Kredensial MongoDB tidak lengkap! Pastikan variabel "
                "MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD, MONGO_HOST, dan MONGO_PORT "
                "sudah terisi di .env"
            )
            
        mongo_uri = f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin"
        
    return MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)


def fetch_raw_games(username: str, year: str, month: str) -> List[Dict]:
    """Ambil dokumen mentah dari koleksi MongoDB Bronze layer."""
    client = get_mongo_client()
    try:
        db = client[os.getenv('MONGO_INITDB_DATABASE', 'chess_raw_data')]
        query = {
            'source_username': username,
            'source_period': f"{year}-{month}"
        }
        games = list(db['raw_games'].find(query))
        logger.info(f"Fetched {len(games)} raw games from MongoDB")
        return games
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Cleaning & Validation
# ---------------------------------------------------------------------------
def clean_and_validate_games(raw_games: List[Dict]) -> Tuple[pd.DataFrame, List[Dict]]:
    """Validasi setiap dokumen mentah melawan ChessGameSchema."""
    valid = []
    invalid = []

    for game in raw_games:
        try:
            white = game.get('white', {}) or {}
            black = game.get('black', {}) or {}
            end_time_unix = game.get('end_time', 0)

            if not end_time_unix or end_time_unix == 0:
                raise ValueError("end_time_unix is missing or zero")

            validated = ChessGameSchema(
                game_id=str(game.get('game_id', '')),
                url=str(game.get('url', '')),
                pgn=str(game.get('pgn', '')),
                white_username=str(white.get('username', 'unknown')),
                black_username=str(black.get('username', 'unknown')),
                white_rating=int(white.get('rating', 0)),
                black_rating=int(black.get('rating', 0)),
                time_class=str(game.get('time_class', 'blitz')),
                rules=str(game.get('rules', 'chess')),
                end_time=datetime.fromtimestamp(end_time_unix),
                source_username=game.get('source_username'),
                source_period=game.get('source_period'),
                extraction_timestamp=game.get('extraction_timestamp'),
            )

            # Derived fields
            validated.rating_diff = validated.white_rating - validated.black_rating

            # FIX: API Bulanan Chess.com tidak punya field 'winner' di root.
            # Kita harus cek field 'result' di dalam objek 'white' dan 'black'.
            white_result = white.get('result', '').lower()
            black_result = black.get('result', '').lower()

            if white_result == 'win':
                validated.outcome = 'win'       # Putih menang
            elif black_result == 'win':
                validated.outcome = 'loss'      # Hitam menang (Putih kalah)
            else:
                validated.outcome = 'draw'      # Seri (stalemate, agreement, repetition, dll)

            # FIX: Perhitungan durasi dengan fallback ke time_control
            start_unix = game.get('start_time', 0)
            if start_unix is not None and start_unix > 0:
                duration_seconds = end_time_unix - start_unix
                validated.duration_minutes = round(duration_seconds / 60, 2)
            else:
                time_control = str(game.get('time_control', '0'))
                if '+' in time_control:
                    base_time = int(time_control.split('+')[0])
                    validated.duration_minutes = base_time / 60  # Konversi detik ke menit
                else:
                    validated.duration_minutes = 0.0

            valid.append(validated.model_dump())

        except Exception as e:
            logger.error(f"Invalid game {game.get('game_id')}: {e}")
            invalid.append({'game_id': game.get('game_id'), 'error': str(e)})

    return pd.DataFrame(valid), invalid


# ---------------------------------------------------------------------------
# Main Transform Entry Point
# ---------------------------------------------------------------------------
def transform_chess_data(username: str, year: str, month: str) -> Dict:
    """
    Orchestrate fetch -> clean -> validate -> save to Parquet.
    Mengembalikan path file parquet agar bisa diteruskan via XCom.
    """
    start = datetime.now()
    try:
        raw = fetch_raw_games(username, year, month)
        df, invalid = clean_and_validate_games(raw)

        # Tentukan folder staging di dalam Docker
        temp_dir = "/opt/airflow/staging"
        os.makedirs(temp_dir, exist_ok=True)
        
        file_name = f"transformed_{username}_{year}{month}.parquet"
        file_path = os.path.join(temp_dir, file_name)
        
        # Simpan DataFrame ke format Parquet jika datanya ada
        if not df.empty:
            df.to_parquet(file_path, index=False)
            logger.info(f"Saved transformed dataframe to parquet: {file_path}")

        duration = (datetime.now() - start).total_seconds()
        logger.info(
            f"Transform done in {duration:.2f}s | "
            f"Valid: {len(df)}, Invalid: {len(invalid)}"
        )

        return {
            'status': 'success',
            'validated_count': len(df),
            'invalid_count': len(invalid),
            'file_path': file_path if not df.empty else None,
            'metadata': {
                'username': username,
                'year': year,
                'month': month,
                'timestamp': datetime.now().isoformat(),
            },
        }
    except Exception as e:
        logger.error(f"Transform failed: {e}")
        raise