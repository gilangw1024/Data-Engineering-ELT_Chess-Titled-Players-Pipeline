"""
Extract Module - Chess Pipeline
...
"""
import os
import logging
from datetime import datetime
from typing import Dict, List, Any

# Load .env early
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests
from pymongo import MongoClient

logger = logging.getLogger(__name__)


def get_mongo_client() -> MongoClient:
    """Membuat koneksi ke MongoDB menggunakan variabel lingkungan."""
    mongo_uri = os.getenv('MONGO_URI')
    
    if not mongo_uri:
        user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
        password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
        host = os.getenv('MONGO_HOST', 'mongodb')
        port = os.getenv('MONGO_PORT', '27017')
        
        if not all([user, password]):
            raise ValueError(
                "Kredensial MongoDB tidak lengkap! Pastikan variabel "
                "MONGO_INITDB_ROOT_USERNAME dan MONGO_INITDB_ROOT_PASSWORD sudah terisi."
            )
            
        mongo_uri = f"mongodb://{user}:{password}@{host}:{port}/?authSource=admin"
        
    return MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)


def fetch_games_from_api(username: str, year: str, month: str) -> List[Dict[str, Any]]:
    """Mengambil arsip bulanan pertandingan catur dari Chess.com API."""
    base_url = os.getenv('CHESS_API_BASE_URL', 'https://api.chess.com/pub')
    # MOnth format 2 digit (ex: '01', '12')
    formatted_month = month.zfill(2)
    url = f"{base_url}/player/{username}/games/{year}/{formatted_month}"
    
    headers = {
        'User-Agent': 'ChessMedallionPipeline/1.0 (Educational Project)'
    }
    
    logger.info(f"Fetching games from API: {url}")
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 404:
        logger.warning(f"No games found for {username} in {year}-{formatted_month} (404 Not Found)")
        return []
        
    response.raise_for_status()
    data = response.json()
    
    games = data.get('games', [])
    logger.info(f"Successfully fetched {len(games)} games from Chess.com API")
    return games


def extract_chess_games(username: str, year: str, month: str) -> Dict[str, Any]:
    """
    Orchestrator untuk proses Extract: Mengambil data dari API 
    dan melakukan insert mentah (raw/upsert) ke MongoDB Bronze Layer.
    """
    if not username or not year or not month:
        raise ValueError("username, year, dan month wajib diisi")

    start_time = datetime.now()
    formatted_month = month.zfill(2)
    period_str = f"{year}-{formatted_month}"
    
    try:
        raw_games = fetch_games_from_api(username, year, formatted_month)
        
        if not raw_games:
            return {
                'status': 'success',
                'inserted_count': 0,
                'metadata': {
                    'username': username,
                    'year': year,
                    'month': formatted_month,
                    'timestamp': datetime.now().isoformat()
                }
            }

        client = get_mongo_client()
        db_name = os.getenv('MONGO_INITDB_DATABASE', 'chess_raw_data')
        db = client[db_name]
        collection = db['raw_games']
        
        # Pastikan ada indeks unik untuk menghindari duplikasi data saat re-run
        collection.create_index([("url", 1)], unique=True, background=True)

        inserted_count = 0
        extraction_timestamp = datetime.now()

        for game in raw_games:
            # Tambahkan metadata tambahan untuk keperluan audit & pelacakan
            game['source_username'] = username
            game['source_period'] = period_str
            game['extraction_timestamp'] = extraction_timestamp
            
            # Buat ID unik berdasarkan URL game jika belum ada field game_id
            if 'url' in game:
                game['game_id'] = game['url'].split('/')[-1]

            try:
                # Upsert berdasarkan 'url' agar aman dari duplikasi
                collection.update_one(
                    {'url': game.get('url')},
                    {'$set': game},
                    upsert=True
                )
                inserted_count += 1
            except Exception as inner_err:
                logger.error(f"Failed to insert individual game into MongoDB: {inner_err}")

        client.close()
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Extraction completed in {duration:.2f}s. Inserted/Updated: {inserted_count} docs.")

        return {
            'status': 'success',
            'inserted_count': inserted_count,
            'metadata': {
                'username': username,
                'year': year,
                'month': formatted_month,
                'timestamp': extraction_timestamp.isoformat()
            }
        }

    except Exception as e:
        logger.error(f"Extraction failed for {username} ({period_str}): {e}")
        raise