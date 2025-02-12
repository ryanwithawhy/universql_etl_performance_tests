from database import DatabaseManager
from env_setup import EnvironmentSetup
from bronze import BronzeLayerManager
from silver import SilverLayerManager
from gold import GoldLayerManager
from connections import ConnectionManager
import os
from dotenv import load_dotenv

load_dotenv()

class TPCDIManager:
    def __init__(self):
        load_dotenv()
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.s3_uri = os.getenv("S3_URI")
        self.external_volume = os.getenv("EXTERNAL_VOLUME")
        self.base_location = os.getenv("BASE_LOCATION")
        
        # Initialize components
        self.db = DatabaseManager(None)  # Add connection
        self.env_setup = EnvironmentSetup()
        self.bronze = BronzeLayerManager(self.external_volume, self.base_location)
        self.silver = SilverLayerManager()
        self.gold = GoldLayerManager(self.external_volume, self.base_location)
    
        self.conn_manager = ConnectionManager("snowflake")
        connection = self.conn_manager.get_connection()
        
        # Initialize components with connection
        self.db = DatabaseManager(connection)    
    
    def setup_env(self):
        print("""Setting up database and file formats""")
        queries = []
        queries.extend(self.env_setup.create_database_and_schemas())
        queries.extend(self.env_setup.create_file_formats())
        self.db.execute_queries(queries)
    
    def create_tables(self):
        """Create all layer tables"""
        queries = []
        queries.extend(self.gold.create_tables())
        queries.extend(self.silver.create_tables())
        queries.extend(self.bronze.create_tables())
        self.db.execute_queries(queries)

    def execute_batch(self, batch_num):
        queries = []
        historical = False
        if batch_num == 1:
            historical = True
        queries.extend(self.bronze.load_staging_data(self.s3_uri, self.aws_access_key, self.aws_secret_key, batch_num, historical))
        self.db.execute_queries(queries)
  
def main():
    manager = TPCDIManager()
    manager.setup_env()
    manager.create_tables()
    for batch in (1,2):
        manager.execute_batch(batch)

if __name__ == "__main__":
    main()