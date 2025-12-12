import asyncpg
from typing import Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Создание пула подключений"""
        self.pool = await asyncpg.create_pool(
            self.connection_url,
            min_size=1,
            max_size=10,
            command_timeout=60
        )
        logger.info("Database pool created")
    
    async def execute_query(self, sql: str) -> Any:
        """Выполнение SQL запроса"""
        if not self.pool:
            await self.connect()
        
        async with self.pool.acquire() as connection:
            try:
                
                sql_lower = sql.lower().strip()
                dangerous_keywords = ['drop', 'delete', 'truncate', 'update', 'insert']
                
                if any(keyword in sql_lower for keyword in dangerous_keywords):
                    
                    if not (sql_lower.startswith('delete from') or 
                           sql_lower.startswith('update ') or 
                           sql_lower.startswith('insert into')):
                        raise ValueError("Запрещенная операция")
                
                
                if sql_lower.startswith("select"):
                    result = await connection.fetch(sql)
                    if result and len(result) > 0:
                        return result
                    else:
                        return None
                else:
                    result = await connection.execute(sql)
                    return result
                    
            except Exception as e:
                logger.error(f"Database error: {str(e)}. SQL: {sql}")
                raise
    
    async def close(self):
        """Закрытие пула подключений"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")