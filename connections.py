from snowflake.connector import connect
from typing import Union
import os

class ConnectionManager:
    def __init__(self, connection_type: str = "snowflake"):
        self.connection_type = connection_type
        self.conn = None
        
    def get_connection(self):
        if self.conn is None:
            if self.connection_type == "snowflake":
                self.conn = connect(
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    user=os.getenv("SNOWFLAKE_USER"),
                    password=os.getenv("SNOWFLAKE_PASSWORD"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    role=os.getenv("SNOWFLAKE_ROLE")
                )
            elif self.connection_type == "universql":
                # TO BE IMPLEMENTED
                pass
            
        return self.conn

    def close_connection(self):
        if self.conn:
            self.conn.close()
            self.conn = None