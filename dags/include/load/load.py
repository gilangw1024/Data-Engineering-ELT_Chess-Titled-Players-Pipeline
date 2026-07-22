"""
Load Module - Chess Pipeline
Fixed: Menggunakan SQLAlchemy Core insert (tanpa pandas.to_sql)
"""
import os
import logging
import uuid
from typing import Dict, List
from datetime import datetime

from sqlalchemy import create_engine, text, insert
import pandas as pd

logger = logging.getLogger(__name__)

def get_postgres_engine():
    """Membuat koneksi engine ke PostgreSQL."""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        host = os.getenv('POSTGRES_HOST')
        port = os.getenv('POSTGRES_PORT')
        dbname = os.getenv('POSTGRES_DB')
        db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(db_url, pool_pre_ping=True)


def load_to_silver(transform_result: Dict) -> Dict:
    """
    Membaca file Parquet dari hasil transform, lalu load ke tabel Silver.
    """
    file_path = transform_result.get('file_path')
    
    if not file_path or not os.path.exists(file_path):
        logger.error(f"File parquet tidak ditemukan di path: {file_path}")
        return {'status': 'error', 'reason': 'file_not_found', 'inserted': 0}

    logger.info(f"Membaca data dari: {file_path}")
    df = pd.read_parquet(file_path)
    logger.info(f"Berhasil membaca {len(df)} baris dari file parquet.")

    if df is None or df.empty:
        logger.warning("Dataframe kosong, skip load to silver")
        return {'status': 'skipped', 'inserted': 0}

    engine = get_postgres_engine()
    
    staging_table = f"staging_silver_games_{uuid.uuid4().hex[:8]}"

    try:
        with engine.begin() as conn:
            # 1. PASTIKAN SCHEMA DAN TABEL UTAMA ADA
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
            conn.execute(text("SET search_path TO analytics, public"))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analytics.silver_games (
                    game_id VARCHAR(64) PRIMARY KEY,
                    url VARCHAR(512),
                    pgn TEXT,
                    white_username VARCHAR(128) NOT NULL,
                    black_username VARCHAR(128) NOT NULL,
                    white_rating INTEGER DEFAULT 0,
                    black_rating INTEGER DEFAULT 0,
                    time_class VARCHAR(32) DEFAULT 'blitz',
                    rules VARCHAR(32) DEFAULT 'chess',
                    end_time TIMESTAMP,
                    rating_diff INTEGER DEFAULT 0,
                    outcome VARCHAR(16) DEFAULT 'draw',
                    duration_minutes NUMERIC(10,2) DEFAULT 0.0,
                    source_username VARCHAR(128),
                    source_period VARCHAR(16),
                    extraction_timestamp TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # 2. Buat tabel staging
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
            conn.execute(text(f"""
                CREATE TABLE {staging_table} (
                    game_id VARCHAR(64) PRIMARY KEY,
                    url VARCHAR(512),
                    pgn TEXT,
                    white_username VARCHAR(128) NOT NULL,
                    black_username VARCHAR(128) NOT NULL,
                    white_rating INTEGER DEFAULT 0,
                    black_rating INTEGER DEFAULT 0,
                    time_class VARCHAR(32) DEFAULT 'blitz',
                    rules VARCHAR(32) DEFAULT 'chess',
                    end_time TIMESTAMP,
                    rating_diff INTEGER DEFAULT 0,
                    outcome VARCHAR(16) DEFAULT 'draw',
                    duration_minutes NUMERIC(10,2) DEFAULT 0.0,
                    source_username VARCHAR(128),
                    source_period VARCHAR(16),
                    extraction_timestamp TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # 3. Insert data dari DataFrame ke staging table
            # FIX: Gunakan iterasi dan execute batch insert
            records_to_insert = []
            for _, row in df.iterrows():
                record = {
                    'game_id': str(row.get('game_id', '')),
                    'url': str(row.get('url', '')),
                    'pgn': str(row.get('pgn', '')),
                    'white_username': str(row.get('white_username', 'unknown')),
                    'black_username': str(row.get('black_username', 'unknown')),
                    'white_rating': int(row.get('white_rating', 0)),
                    'black_rating': int(row.get('black_rating', 0)),
                    'time_class': str(row.get('time_class', 'blitz')),
                    'rules': str(row.get('rules', 'chess')),
                    'end_time': row.get('end_time'),
                    'rating_diff': int(row.get('rating_diff', 0)),
                    'outcome': str(row.get('outcome', 'draw')),
                    'duration_minutes': float(row.get('duration_minutes', 0.0)),
                    'source_username': str(row.get('source_username', '')) if pd.notna(row.get('source_username')) else None,
                    'source_period': str(row.get('source_period', '')) if pd.notna(row.get('source_period')) else None,
                    'extraction_timestamp': row.get('extraction_timestamp'),
                }
                records_to_insert.append(record)
            
            # Batch insert ke staging table
            if records_to_insert:
                conn.execute(
                    text(f"""
                        INSERT INTO analytics.{staging_table} (
                            game_id, url, pgn, white_username, black_username,
                            white_rating, black_rating, time_class, rules, end_time,
                            rating_diff, outcome, duration_minutes, source_username,
                            source_period, extraction_timestamp
                        ) VALUES (
                            :game_id, :url, :pgn, :white_username, :black_username,
                            :white_rating, :black_rating, :time_class, :rules, :end_time,
                            :rating_diff, :outcome, :duration_minutes, :source_username,
                            :source_period, :extraction_timestamp
                        )
                    """),
                    records_to_insert
                )
            
            # 4. Upsert ke tabel utama
            conn.execute(text(f"""
                INSERT INTO analytics.silver_games (
                    game_id, url, pgn, white_username, black_username,
                    white_rating, black_rating, time_class, rules, end_time,
                    rating_diff, outcome, duration_minutes, source_username,
                    source_period, extraction_timestamp, updated_at
                )
                SELECT 
                    game_id, url, pgn, white_username, black_username,
                    white_rating, black_rating, time_class, rules, end_time,
                    rating_diff, outcome, duration_minutes, source_username,
                    source_period, extraction_timestamp, CURRENT_TIMESTAMP
                FROM analytics.{staging_table}
                ON CONFLICT (game_id) DO UPDATE SET
                    url = EXCLUDED.url, pgn = EXCLUDED.pgn,
                    white_username = EXCLUDED.white_username,
                    black_username = EXCLUDED.black_username,
                    white_rating = EXCLUDED.white_rating,
                    black_rating = EXCLUDED.black_rating,
                    time_class = EXCLUDED.time_class,
                    rules = EXCLUDED.rules,
                    end_time = EXCLUDED.end_time,
                    rating_diff = EXCLUDED.rating_diff,
                    outcome = EXCLUDED.outcome,
                    duration_minutes = EXCLUDED.duration_minutes,
                    source_username = EXCLUDED.source_username,
                    source_period = EXCLUDED.source_period,
                    extraction_timestamp = EXCLUDED.extraction_timestamp,
                    updated_at = CURRENT_TIMESTAMP
            """))
            
            # 5. Cleanup
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))

        logger.info(f"Successfully loaded {len(df)} records to Silver layer")
        return {'status': 'success', 'inserted': len(df)}
        
    except Exception as e:
        logger.error(f"Unexpected error during Silver load: {e}")
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        except Exception as cleanup_err:
            logger.error(f"Failed to cleanup staging table: {cleanup_err}")
        raise


def load_to_gold(**kwargs) -> Dict:
    """
    Mengagregasi data dari Silver ke Gold (player stats).
    """
    engine = get_postgres_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
            conn.execute(text("SET search_path TO analytics, public"))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analytics.gold_player_stats (
                    username VARCHAR(128) PRIMARY KEY,
                    total_games INTEGER DEFAULT 0,
                    avg_rating NUMERIC(10,2) DEFAULT 0.0,
                    win_rate NUMERIC(5,4) DEFAULT 0.0,
                    loss_rate NUMERIC(5,4) DEFAULT 0.0,
                    draw_rate NUMERIC(5,4) DEFAULT 0.0,
                    avg_duration_minutes NUMERIC(10,2) DEFAULT 0.0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))

            conn.execute(text("""
                INSERT INTO analytics.gold_player_stats (
                    username, total_games, avg_rating, win_rate, loss_rate,
                    draw_rate, avg_duration_minutes, last_updated
                )
                SELECT
                    username,
                    COUNT(*) AS total_games,
                    COALESCE(AVG(rating), 0) AS avg_rating,
                    COALESCE(SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0), 0) AS win_rate,
                    COALESCE(SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0), 0) AS loss_rate,
                    COALESCE(SUM(CASE WHEN outcome = 'draw' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0), 0) AS draw_rate,
                    COALESCE(AVG(NULLIF(duration_minutes, 0)), 0) AS avg_duration_minutes,
                    CURRENT_TIMESTAMP AS last_updated
                FROM (
                    SELECT white_username AS username, white_rating AS rating, outcome, duration_minutes
                    FROM analytics.silver_games WHERE white_username IS NOT NULL
                    UNION ALL
                    SELECT black_username AS username, black_rating AS rating,
                           CASE WHEN outcome = 'win' THEN 'loss' WHEN outcome = 'loss' THEN 'win' ELSE 'draw' END AS outcome,
                           duration_minutes
                    FROM analytics.silver_games WHERE black_username IS NOT NULL
                ) combined_games
                GROUP BY username
                ON CONFLICT (username) DO UPDATE SET
                    total_games = EXCLUDED.total_games,
                    avg_rating = EXCLUDED.avg_rating,
                    win_rate = EXCLUDED.win_rate,
                    loss_rate = EXCLUDED.loss_rate,
                    draw_rate = EXCLUDED.draw_rate,
                    avg_duration_minutes = EXCLUDED.avg_duration_minutes,
                    last_updated = EXCLUDED.last_updated
            """))
        logger.info("Gold layer aggregation completed successfully")
        return {'status': 'success'}
    except Exception as e:
        logger.error(f"Gold load failed: {e}")
        raise