from typing import List, Optional
from snowflake.connector.cursor import SnowflakeCursor

class DatabaseManager:
    def __init__(self, connection):
        self.conn = connection
    
    def execute_queries(self, queries: List[str]) -> None:
        if not queries:
            return
            
        cursor = self.conn.cursor()
        try:
            for query in queries:
                if query.strip():  # Skip empty queries
                    try:
                        print(f"Executing query: {query}")
                        cursor.execute(query)
                        print("Success")
                    except Exception as e:
                        raise(e)
        finally:
            cursor.close()
            
    def execute_query(self, query: str) -> Optional[SnowflakeCursor]:
        if not query.strip():
            return None
            
        cursor = self.conn.cursor()
        try:
            print(f"Executing query: {query}")
            cursor.execute(query)
            print("Success")
            return cursor
        except Exception as e:
            print(f"Failed")
            cursor.close()
            raise(e)