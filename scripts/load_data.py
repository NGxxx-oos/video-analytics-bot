import json
import asyncio
import asyncpg
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


async def load_json_to_db(json_file_path: str):
    """Загрузка данных из JSON в PostgreSQL"""
    
    
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    
    print(f"📊 Загружено {len(data)} видео...")
    
    try:
        videos_inserted = 0
        snapshots_inserted = 0
        
        for video in data:
            
            await conn.execute("""
                INSERT INTO videos (id, creator_id, video_created_at, views_count, 
                                  likes_count, comments_count, reports_count, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (id) DO NOTHING
            """,
            video['id'],
            video['creator_id'],
            video['video_created_at'],
            video['views_count'],
            video['likes_count'],
            video['comments_count'],
            video['reports_count'],
            video['created_at'],
            video['updated_at'])
            
            videos_inserted += 1
            
            
            for snapshot in video.get('snapshots', []):
                await conn.execute("""
                    INSERT INTO video_snapshots 
                    (video_id, views_count, likes_count, comments_count, reports_count,
                     delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                     created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                video['id'],
                snapshot['views_count'],
                snapshot['likes_count'],
                snapshot['comments_count'],
                snapshot['reports_count'],
                snapshot.get('delta_views_count', 0),
                snapshot.get('delta_likes_count', 0),
                snapshot.get('delta_comments_count', 0),
                snapshot.get('delta_reports_count', 0),
                snapshot['created_at'],
                snapshot.get('updated_at', snapshot['created_at']))
                
                snapshots_inserted += 1
            
            # Прогресс
            if videos_inserted % 100 == 0:
                print(f"⏳ Обработано {videos_inserted} видео, {snapshots_inserted} снапшотов...")
        
        print(f"\n🎉 Загрузка завершена!")
        print(f"📽️ Видео: {videos_inserted}")
        print(f"📸 Снапшотов: {snapshots_inserted}")
        
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    json_file = "data/videos.json"
    if not os.path.exists(json_file):
        print(f"❌ Файл {json_file} не найден!")
        print("Убедитесь, что файл videos.json находится в папке data/")
    else:
        asyncio.run(load_json_to_db(json_file))