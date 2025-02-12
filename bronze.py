class BronzeLayerManager:
    def __init__(self, external_volume: str, base_location: str):
        self.external_volume = external_volume
        self.base_location = base_location
        
    def create_tables(self) -> list[str]:
        ddls = []
        
        # Account staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.ACCOUNT (
            CDC_FLAG STRING,
            CDC_DSN NUMBER(12,0),
            CA_ID NUMBER(11,0) NOT NULL,
            CA_B_ID NUMBER(11,0) NOT NULL,
            CA_C_ID NUMBER(11,0) NOT NULL,
            CA_NAME STRING,
            CA_TAX_ST NUMBER(1,0),
            CA_ST_ID STRING
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/account';
        """)
        
        # Account historical table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.HISTORICAL.ACCOUNT (
            CA_ID NUMBER(11,0) NOT NULL,
            CA_B_ID NUMBER(11,0) NOT NULL,
            CA_C_ID NUMBER(11,0) NOT NULL,
            CA_NAME STRING,
            CA_TAX_ST NUMBER(1,0),
            CA_ST_ID STRING
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/historical_account';
        """)

        # BatchDate staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.BATCHDATE (
            BatchDate DATE NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/batchdate';
        """)

        # CashTransaction staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.CASHTRANSACTION (
            CDC_FLAG STRING,
            CDC_DSN NUMBER(12,0),
            CT_CA_ID NUMBER(11,0) NOT NULL,
            CT_DTS TIMESTAMP NOT NULL,
            CT_AMT NUMBER(10,2) NOT NULL,
            CT_NAME STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/cashtransaction';
        """)
        
        # CashTransaction historical table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.HISTORICAL.CASHTRANSACTION (
            CT_CA_ID NUMBER(11,0) NOT NULL,
            CT_DTS TIMESTAMP NOT NULL,
            CT_AMT NUMBER(10,2) NOT NULL,
            CT_NAME STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/historical_cashtransaction';
        """)

        # Customer staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.CUSTOMER (
            CDC_FLAG STRING,
            CDC_DSN NUMBER(12,0),
            C_ID NUMBER(11,0) NOT NULL,
            C_TAX_ID STRING NOT NULL,
            C_ST_ID STRING,
            C_L_NAME STRING NOT NULL,
            C_F_NAME STRING NOT NULL,
            C_M_NAME STRING,
            C_GNDR STRING,
            C_TIER NUMBER(1,0),
            C_DOB DATE NOT NULL,
            C_ADLINE1 STRING NOT NULL,
            C_ADLINE2 STRING,
            C_ZIPCODE STRING NOT NULL,
            C_CITY STRING NOT NULL,
            C_STATE_PROV STRING NOT NULL,
            C_CTRY STRING,
            C_CTRY_1 STRING,
            C_AREA_1 STRING,
            C_LOCAL_1 STRING,
            C_EXT_1 STRING,
            C_CTRY_2 STRING,
            C_AREA_2 STRING,
            C_LOCAL_2 STRING,
            C_EXT_2 STRING,
            C_CTRY_3 STRING,
            C_AREA_3 STRING,
            C_LOCAL_3 STRING,
            C_EXT_3 STRING,
            C_EMAIL_1 STRING,
            C_EMAIL_2 STRING,
            C_LCL_TX_ID STRING NOT NULL,
            C_NAT_TX_ID STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/customer';
        """)
        
        # Customer historical table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.HISTORICAL.CUSTOMER (
            C_ID NUMBER(11,0) NOT NULL,
            C_TAX_ID STRING NOT NULL,
            C_ST_ID STRING,
            C_L_NAME STRING NOT NULL,
            C_F_NAME STRING NOT NULL,
            C_M_NAME STRING,
            C_GNDR STRING,
            C_TIER NUMBER(1,0),
            C_DOB DATE NOT NULL,
            C_ADLINE1 STRING NOT NULL,
            C_ADLINE2 STRING,
            C_ZIPCODE STRING NOT NULL,
            C_CITY STRING NOT NULL,
            C_STATE_PROV STRING NOT NULL,
            C_CTRY STRING,
            C_CTRY_1 STRING,
            C_AREA_1 STRING,
            C_LOCAL_1 STRING,
            C_EXT_1 STRING,
            C_CTRY_2 STRING,
            C_AREA_2 STRING,
            C_LOCAL_2 STRING,
            C_EXT_2 STRING,
            C_CTRY_3 STRING,
            C_AREA_3 STRING,
            C_LOCAL_3 STRING,
            C_EXT_3 STRING,
            C_EMAIL_1 STRING,
            C_EMAIL_2 STRING,
            C_LCL_TX_ID STRING NOT NULL,
            C_NAT_TX_ID STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/historical_customer';
        """)

        # CustomerMgmt staging table (XML format)
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.CUSTOMERMGMT (
            xml_content OBJECT
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/customermgmt';
        """)

        # DailyMarket staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.DAILYMARKET (
            CDC_FLAG STRING,
            CDC_DSN NUMBER(12,0),
            DM_DATE DATE NOT NULL,
            DM_S_SYMB STRING NOT NULL,
            DM_CLOSE NUMBER(8,2) NOT NULL,
            DM_HIGH NUMBER(8,2) NOT NULL,
            DM_LOW NUMBER(8,2) NOT NULL,
            DM_VOL NUMBER(12,0) NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/dailymarket';
        """)
        
        # DailyMarket historical table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.HISTORICAL.DAILYMARKET (
            DM_DATE DATE NOT NULL,
            DM_S_SYMB STRING NOT NULL,
            DM_CLOSE NUMBER(8,2) NOT NULL,
            DM_HIGH NUMBER(8,2) NOT NULL,
            DM_LOW NUMBER(8,2) NOT NULL,
            DM_VOL NUMBER(12,0) NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/historical_dailymarket';
        """)

        # Date staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.DATE (
            SK_DateID NUMBER(11,0) NOT NULL,
            DateValue DATE NOT NULL,
            DateDesc STRING NOT NULL,
            CalendarYearID NUMBER(4,0) NOT NULL,
            CalendarYearDesc STRING NOT NULL,
            CalendarQtrID NUMBER(5,0) NOT NULL,
            CalendarQtrDesc STRING NOT NULL,
            CalendarMonthID NUMBER(6,0) NOT NULL,
            CalendarMonthDesc STRING NOT NULL,
            CalendarWeekID NUMBER(6,0) NOT NULL,
            CalendarWeekDesc STRING NOT NULL,
            DayOfWeekNum NUMBER(1,0) NOT NULL,
            DayOfWeekDesc STRING NOT NULL,
            FiscalYearID NUMBER(4,0) NOT NULL,
            FiscalYearDesc STRING NOT NULL,
            FiscalQtrID NUMBER(5,0) NOT NULL,
            FiscalQtrDesc STRING NOT NULL,
            HolidayFlag BOOLEAN
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/date';
        """)

        # FINWIRE staging table (fixed width format)
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.FINWIRE (
            LineNumber INTEGER,  -- To maintain input order 
            RawText STRING NOT NULL  -- Max length needed for any record type (CMP/SEC/FIN)
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/finwire';
        """)

        # HoldingHistory staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.HOLDINGHISTORY (
            CDC_FLAG STRING,
            CDC_DSN NUMBER(12,0),
            HH_H_T_ID NUMBER(15,0) NOT NULL,
            HH_T_ID NUMBER(15,0) NOT NULL,
            HH_BEFORE_QTY NUMBER(6,0) NOT NULL,
            HH_AFTER_QTY NUMBER(6,0) NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/holdinghistory';
        """)

        # HoldingHistory historical table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.HISTORICAL.HOLDINGHISTORY (
            HH_H_T_ID NUMBER(15,0) NOT NULL,
            HH_T_ID NUMBER(15,0) NOT NULL,
            HH_BEFORE_QTY NUMBER(6,0) NOT NULL,
            HH_AFTER_QTY NUMBER(6,0) NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/historical_holdinghistory';
        """)

        # HR staging table (CSV format)
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.HR (
            EmployeeID NUMBER(11,0) NOT NULL,
            ManagerID NUMBER(11,0) NOT NULL,
            EmployeeFirstName STRING NOT NULL,
            EmployeeLastName STRING NOT NULL,
            EmployeeMI STRING,
            EmployeeJobCode NUMBER(3,0),
            EmployeeBranch STRING,
            EmployeeOffice STRING,
            EmployeePhone STRING
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/hr';
        """)

        # Industry staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.INDUSTRY (
            IN_ID STRING NOT NULL,
            IN_NAME STRING NOT NULL,
            IN_SC_ID STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/industry';
        """)

        # Prospect staging table (CSV format)
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.PROSPECT (
            AgencyID STRING NOT NULL,
            LastName STRING NOT NULL,
            FirstName STRING NOT NULL,
            MiddleInitial STRING,
            Gender STRING,
            AddressLine1 STRING,
            AddressLine2 STRING,
            PostalCode STRING,
            City STRING NOT NULL,
            State STRING NOT NULL,
            Country STRING,
            Phone STRING,
            Income NUMBER(9,0),
            NumberCars NUMBER(2,0),
            NumberChildren NUMBER(2,0),
            MaritalStatus STRING,
            Age NUMBER(3,0),
            CreditRating NUMBER(4,0),
            OwnOrRentFlag STRING,
            Employer STRING,
            NumberCreditCards NUMBER(2,0),
            NetWorth NUMBER(12,0)
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/prospect';
        """)

        # StatusType staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.STATUSTYPE (
            ST_ID STRING NOT NULL,
            ST_NAME STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/statustype';
        """)

        # TaxRate staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.TAXRATE (
            TX_ID STRING NOT NULL,
            TX_NAME STRING NOT NULL,
            TX_RATE NUMBER(6,5) NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/taxrate';
        """)

        # Time staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.TIME (
            SK_TimeID NUMBER(11,0) NOT NULL,
            TimeValue TIME NOT NULL,
            HourID NUMBER(2,0) NOT NULL,
            HourDesc STRING NOT NULL,
            MinuteID NUMBER(2,0) NOT NULL,
            MinuteDesc STRING NOT NULL,
            SecondID NUMBER(2,0) NOT NULL,
            SecondDesc STRING NOT NULL,
            MarketHoursFlag BOOLEAN,
            OfficeHoursFlag BOOLEAN
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/time';
        """)

        # TradeHistory staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.TRADEHISTORY (
            TH_T_ID NUMBER(15,0) NOT NULL,
            TH_DTS TIMESTAMP NOT NULL,
            TH_ST_ID STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/tradehistory';
        """)

        # Trade staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.TRADE (
            CDC_FLAG STRING,
            CDC_DSN NUMBER(12,0),
            T_ID NUMBER(15,0) NOT NULL,
            T_DTS TIMESTAMP NOT NULL,
            T_ST_ID STRING NOT NULL,
            T_TT_ID STRING NOT NULL,
            T_IS_CASH BOOLEAN NOT NULL,
            T_S_SYMB STRING NOT NULL,
            T_QTY NUMBER(6,0) NOT NULL,
            T_BID_PRICE NUMBER(8,2) NOT NULL,
            T_CA_ID NUMBER(11,0) NOT NULL,
            T_EXEC_NAME STRING NOT NULL,
            T_TRADE_PRICE NUMBER(8,2),
            T_CHRG NUMBER(10,2),
            T_COMM NUMBER(10,2),
            T_TAX NUMBER(10,2)
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/trade';
        """)

        # Trade historical table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.HISTORICAL.TRADE (
            T_ID NUMBER(15,0) NOT NULL,
            T_DTS TIMESTAMP NOT NULL,
            T_ST_ID STRING NOT NULL,
            T_TT_ID STRING NOT NULL,
            T_IS_CASH BOOLEAN NOT NULL,
            T_S_SYMB STRING NOT NULL,
            T_QTY NUMBER(6,0) NOT NULL,
            T_BID_PRICE NUMBER(8,2) NOT NULL,
            T_CA_ID NUMBER(11,0) NOT NULL,
            T_EXEC_NAME STRING NOT NULL,
            T_TRADE_PRICE NUMBER(8,2),
            T_CHRG NUMBER(10,2),
            T_COMM NUMBER(10,2),
            T_TAX NUMBER(10,2)
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/historical_trade';
        """)

        # TradeType staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.TRADETYPE (
            TT_ID STRING NOT NULL,
            TT_NAME STRING NOT NULL,
            TT_IS_SELL NUMBER(1,0) NOT NULL,
            TT_IS_MRKT NUMBER(1,0) NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/tradetype';
        """)

        # WatchHistory staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.WATCHHISTORY (
            CDC_FLAG STRING,
            CDC_DSN NUMBER(12,0),
            W_C_ID NUMBER(11,0) NOT NULL,
            W_S_SYMB STRING NOT NULL,
            W_DTS TIMESTAMP NOT NULL,
            W_ACTION STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/watchhistory';
        """)

        # WatchHistory historical table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.HISTORICAL.WATCHHISTORY (
            W_C_ID NUMBER(11,0) NOT NULL,
            W_S_SYMB STRING NOT NULL,
            W_DTS TIMESTAMP NOT NULL,
            W_ACTION STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/historical_watchhistory';
        """)

        # Audit staging table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.AUDIT_FILE (
            DataSet STRING NOT NULL,
            BatchID NUMBER(5,0),
            Date DATE,
            Attribute STRING NOT NULL,
            Value NUMBER(15,0),
            DValue NUMBER(15,5)
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/bronze/audit_file';
        """)
        
        return ddls

    def load_staging_data(self, s3_base_url: str, aws_key_id: str, 
                         aws_secret_key: str, batch_num: int, historical = False) -> list[str]:
        """
        Loads TPC-DI data from S3 into Snowflake staging tables for a specified batch.
        
        Args:
            s3_base_url (str): Base S3 URL where the data files are stored
            aws_key_id (str): AWS access key ID
            aws_secret_key (str): AWS secret access key
            batch_num (int): Batch number to load (1 for historical, 2-3 for incremental)
            historical (bool): Whether this is a historical load or not
        """
        
        cdc_table_schema = "STAGING"
        
        if historical == True:
            cdc_table_schema = "HISTORICAL"
        
        # Define the credentials string for COPY commands
        credentials = f"""
        CREDENTIALS = (
            AWS_KEY_ID='{aws_key_id}'
            AWS_SECRET_KEY='{aws_secret_key}'
        )
        """
        
        # Base path for this batch
        batch_path = f"{s3_base_url.rstrip('/')}/Batch{batch_num}"
        
        # Dictionary of copy commands for each table and its corresponding file
        copy_commands = {
            'Account': f"""
                COPY INTO TPC_DI.{cdc_table_schema}.ACCOUNT
                FROM '{batch_path}/Account.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                    EMPTY_FIELD_AS_NULL = TRUE
                )
                {credentials}
            """,
            
            'BatchDate': f"""
                COPY INTO TPC_DI.STAGING.BATCHDATE
                FROM '{batch_path}/BatchDate.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'CashTransaction': f"""
                COPY INTO TPC_DI.{cdc_table_schema}.CASHTRANSACTION
                FROM '{batch_path}/CashTransaction.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'Customer': f"""
                COPY INTO TPC_DI.{cdc_table_schema}.CUSTOMER
                FROM '{batch_path}/Customer.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                    EMPTY_FIELD_AS_NULL = TRUE
                )
                {credentials}
            """,
            
            'CustomerMgmt': f"""
                COPY INTO TPC_DI.STAGING.CUSTOMERMGMT
                FROM '{batch_path}/CustomerMgmt.xml'
                FILE_FORMAT = (
                    TYPE = 'XML'
                )
                {credentials}
            """,
            
            'DailyMarket': f"""
                COPY INTO TPC_DI.{cdc_table_schema}.DAILYMARKET
                FROM '{batch_path}/DailyMarket.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'Date': f"""
                COPY INTO TPC_DI.STAGING.DATE
                FROM '{batch_path}/Date.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'FINWIRE': f"""
                COPY INTO TPC_DI.STAGING.FINWIRE
                FROM '{batch_path}/FINWIRE*.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    RECORD_DELIMITER = '\\n'
                    FIELD_DELIMITER = NONE
                    PARSE_HEADER = FALSE
                )
                {credentials}
            """,
            
            'HoldingHistory': f"""
                COPY INTO TPC_DI.{cdc_table_schema}.HOLDINGHISTORY
                FROM '{batch_path}/HoldingHistory.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'HR': f"""
                COPY INTO TPC_DI.STAGING.HR
                FROM '{batch_path}/HR.csv'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = ','
                    NULL_IF = ''
                    EMPTY_FIELD_AS_NULL = TRUE
                )
                {credentials}
            """,
            
            'Industry': f"""
                COPY INTO TPC_DI.STAGING.INDUSTRY
                FROM '{batch_path}/Industry.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'Prospect': f"""
                COPY INTO TPC_DI.STAGING.PROSPECT
                FROM '{batch_path}/Prospect.csv'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = ','
                    NULL_IF = ''
                    EMPTY_FIELD_AS_NULL = TRUE
                )
                {credentials}
            """,
            
            'StatusType': f"""
                COPY INTO TPC_DI.STAGING.STATUSTYPE
                FROM '{batch_path}/StatusType.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'TaxRate': f"""
                COPY INTO TPC_DI.STAGING.TAXRATE
                FROM '{batch_path}/TaxRate.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'Time': f"""
                COPY INTO TPC_DI.STAGING.TIME
                FROM '{batch_path}/Time.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'Trade': f"""
                COPY INTO TPC_DI.{cdc_table_schema}.TRADE
                FROM '{batch_path}/Trade.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                    EMPTY_FIELD_AS_NULL = TRUE
                )
                {credentials}
            """,
            
            'TradeHistory': f"""
                COPY INTO TPC_DI.STAGING.TRADEHISTORY
                FROM '{batch_path}/TradeHistory.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'TradeType': f"""
                COPY INTO TPC_DI.STAGING.TRADETYPE
                FROM '{batch_path}/TradeType.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """,
            
            'WatchHistory': f"""
                COPY INTO TPC_DI.{cdc_table_schema}.WATCHHISTORY
                FROM '{batch_path}/WatchHistory.txt'
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_DELIMITER = '|'
                    NULL_IF = ''
                )
                {credentials}
            """
        }
        
        # Execute all COPY commands
        queries = []
        for table, copy_cmd in copy_commands.items():
            queries.append(copy_cmd)
        
        return queries