import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()


async def init_database():
    """Создание таблиц в базе данных"""
    
    
    with open('scripts/create_tables.sql', 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    
    try:
        
        await conn.execute(sql_script)
        print("✅ Таблицы успешно созданы!")
        
        
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        print(f"✅ Создано таблиц: {len(tables)}")
        for table in tables:
            print(f"  - {table['table_name']}")
            
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(init_database())