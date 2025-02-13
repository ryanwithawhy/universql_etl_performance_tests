class EnvironmentSetup:
    def __init__(self):
        pass

    def create_database_and_schemas(self) -> list[str]:
        ddls = []
        ddls.append("CREATE DATABASE IF NOT EXISTS TPC_DI;")
        ddls.append("CREATE SCHEMA IF NOT EXISTS TPC_DI.STAGING;")  
        ddls.append("CREATE SCHEMA IF NOT EXISTS TPC_DI.HISTORICAL;")  
        ddls.append("CREATE SCHEMA IF NOT EXISTS TPC_DI.LOAD;")  
        return ddls

    def create_file_formats(self) -> list[str]:
        file_format_ddls = []

        file_format_ddls.append("""
        CREATE OR REPLACE FILE FORMAT TPC_DI.STAGING.CSV_PIPE
            TYPE = 'CSV'
            FIELD_DELIMITER = '|'
            SKIP_HEADER = 0
            NULL_IF = ('');
        """)

        file_format_ddls.append("""
        CREATE OR REPLACE FILE FORMAT TPC_DI.STAGING.CSV_COMMA
            TYPE = 'CSV' 
            FIELD_DELIMITER = ','
            SKIP_HEADER = 0
            NULL_IF = ('');
        """)

        file_format_ddls.append("""
        CREATE OR REPLACE FILE FORMAT TPC_DI.STAGING.XML
            TYPE = 'XML';
        """)

        file_format_ddls.append(r"""
        CREATE OR REPLACE FILE FORMAT TPC_DI.STAGING.TEXT
            TYPE = 'CSV'
            FIELD_DELIMITER = NONE
            RECORD_DELIMITER = '\n';
        """)
        
        file_format_ddls.append("""
        CREATE OR REPLACE FILE FORMAT TPC_DI.STAGING.JSON
            TYPE = 'JSON'
            STRIP_OUTER_ARRAY = TRUE
            ALLOW_DUPLICATE = FALSE;
        """)
        
        return file_format_ddls







# ddls = create_gold_tables("iceberg_external_volume", "tpc-di") + create_file_formats()
# for ddl in create_file_formats():
#     print(ddl)

# for ddl in create_bronze_tables():
#     print(ddl)

# for ddl in create_silver_tables():
#     print(ddl)
    
# for ddl in create_gold_tables("iceberg_external_volume", "tpc-di"):
#     print(ddl)