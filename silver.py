class SilverLayerManager:
    def __init__(self, external_volume: str, base_location: str):
        self.external_volume = external_volume
        self.base_location = base_location
    
    def create_tables(self, persist:bool = False) -> list[str]:
        # Silver layer for FINWIRE parsed records
        ddls = []
        
        if persist == True:
            return self.create_iceberg_tables()
        
        ddls.append("""
        CREATE OR REPLACE TEMPORARY TABLE TPC_DI.STAGING.SILVER_FINWIRE_CMP (
            PTS TIMESTAMP NOT NULL,
            CompanyName VARCHAR(60) NOT NULL,
            CIK VARCHAR(10) NOT NULL,
            Status CHAR(4) NOT NULL,
            IndustryID CHAR(2) NOT NULL,
            SPRating CHAR(4),
            FoundingDate DATE,
            AddrLine1 VARCHAR(80) NOT NULL,
            AddrLine2 VARCHAR(80),
            PostalCode VARCHAR(12) NOT NULL,
            City VARCHAR(25) NOT NULL,
            StateProvince VARCHAR(20) NOT NULL,
            Country VARCHAR(24),
            CEOName VARCHAR(46) NOT NULL,
            Description VARCHAR(150) NOT NULL
        );
        """)

        ddls.append("""
        CREATE OR REPLACE TEMPORARY TABLE TPC_DI.STAGING.SILVER_FINWIRE_SEC (
            PTS TIMESTAMP NOT NULL,
            Symbol VARCHAR(15) NOT NULL,
            IssueType VARCHAR(6) NOT NULL,
            Status CHAR(4) NOT NULL,
            Name VARCHAR(70) NOT NULL,
            ExID VARCHAR(6) NOT NULL,
            ShOut NUMBER(12) NOT NULL,
            FirstTradeDate DATE NOT NULL,
            FirstTradeExchg DATE NOT NULL,
            Dividend NUMBER(10,2) NOT NULL,
            CoNameOrCIK VARCHAR(60) NOT NULL
        );
        """)

        ddls.append("""
        CREATE OR REPLACE TEMPORARY TABLE TPC_DI.STAGING.SILVER_FINWIRE_FIN (
            PTS TIMESTAMP NOT NULL,
            Year NUMBER(4) NOT NULL,
            Quarter CHAR(1) NOT NULL,
            QtrStartDate DATE NOT NULL,
            PostingDate DATE NOT NULL,
            Revenue NUMBER(15,2) NOT NULL,
            Earnings NUMBER(15,2) NOT NULL,
            EPS NUMBER(10,2) NOT NULL,
            DilutedEPS NUMBER(10,2) NOT NULL,
            Margin NUMBER(10,2) NOT NULL,
            Inventory NUMBER(15,2) NOT NULL,
            Assets NUMBER(15,2) NOT NULL,
            Liabilities NUMBER(15,2) NOT NULL,
            ShOut NUMBER(12) NOT NULL,
            DilutedShOut NUMBER(12) NOT NULL,
            CoNameOrCIK VARCHAR(60) NOT NULL
        );
        """)

        # Silver layer for parsed CustomerMgmt XML 
        ddls.append("""
        CREATE OR REPLACE TEMPORARY TABLE TPC_DI.STAGING.SILVER_CUSTOMER_MGMT (
            ActionType VARCHAR(9) NOT NULL,
            ActionTS TIMESTAMP NOT NULL,
            C_ID NUMBER(11) NOT NULL,
            C_TAX_ID VARCHAR(20),
            C_GNDR CHAR(1),
            C_TIER NUMBER(1),
            C_DOB DATE,
            C_L_NAME VARCHAR(25),
            C_F_NAME VARCHAR(20),
            C_M_NAME CHAR(1),
            C_ADLINE1 VARCHAR(80),
            C_ADLINE2 VARCHAR(80),
            C_ZIPCODE VARCHAR(12),
            C_CITY VARCHAR(25),
            C_STATE_PROV VARCHAR(20),
            C_CTRY VARCHAR(24),
            C_PRIM_EMAIL VARCHAR(50),
            C_ALT_EMAIL VARCHAR(50),
            C_PHONE_1 VARCHAR(30),
            C_PHONE_2 VARCHAR(30),
            C_PHONE_3 VARCHAR(30),
            C_LCL_TX_ID CHAR(4),
            C_NAT_TX_ID CHAR(4),
            CA_ID NUMBER(11),
            CA_TAX_ST NUMBER(1),
            CA_B_ID NUMBER(11),
            CA_NAME VARCHAR(50)
        );
        """)

        # Silver layer for DimCustomer with history tracking
        ddls.append("""
        CREATE OR REPLACE TEMPORARY TABLE TPC_DI.STAGING.SILVER_DIM_CUSTOMER (
            SK_CustomerID NUMBER(11) NOT NULL,
            CustomerID NUMBER(11) NOT NULL,
            TaxID VARCHAR(20) NOT NULL,
            Status VARCHAR(10) NOT NULL,
            LastName VARCHAR(30) NOT NULL,
            FirstName VARCHAR(30) NOT NULL,
            MiddleInitial CHAR(1),
            Gender CHAR(1),
            Tier NUMBER(1),
            DOB DATE NOT NULL,
            AddressLine1 VARCHAR(80) NOT NULL,
            AddressLine2 VARCHAR(80),
            PostalCode VARCHAR(12) NOT NULL,
            City VARCHAR(25) NOT NULL,
            StateProv VARCHAR(20) NOT NULL,
            Country VARCHAR(24),
            Phone1 VARCHAR(30),
            Phone2 VARCHAR(30),
            Phone3 VARCHAR(30),
            Email1 VARCHAR(50),
            Email2 VARCHAR(50),
            NationalTaxRateDesc VARCHAR(50),
            NationalTaxRate NUMBER(6,5),
            LocalTaxRateDesc VARCHAR(50),
            LocalTaxRate NUMBER(6,5),
            AgencyID VARCHAR(30),
            CreditRating NUMBER(5),
            NetWorth NUMBER(10),
            MarketingNameplate VARCHAR(100),
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMBER(5) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        );
        """)

        # Silver layer for DimAccount with history tracking
        ddls.append("""
        CREATE OR REPLACE TEMPORARY TABLE TPC_DI.STAGING.SILVER_DIM_ACCOUNT (
            SK_AccountID NUMBER(11) NOT NULL,
            AccountID NUMBER(11) NOT NULL,
            SK_BrokerID NUMBER(11) NOT NULL,
            SK_CustomerID NUMBER(11) NOT NULL,
            Status VARCHAR(10) NOT NULL,
            AccountDesc VARCHAR(50),
            TaxStatus NUMBER(1),
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMBER(5) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        );""")

        # Silver layer for DimSecurity with history tracking
        ddls.append("""
        CREATE OR REPLACE TEMPORARY TABLE TPC_DI.STAGING.SILVER_DIM_SECURITY (
            SK_SecurityID NUMBER(11) NOT NULL,
            Symbol VARCHAR(15) NOT NULL,
            Issue VARCHAR(6) NOT NULL,
            Status VARCHAR(10) NOT NULL,
            Name VARCHAR(70) NOT NULL,
            ExchangeID VARCHAR(6) NOT NULL,
            SK_CompanyID NUMBER(11) NOT NULL,
            SharesOutstanding NUMBER(12) NOT NULL,
            FirstTrade DATE NOT NULL,
            FirstTradeOnExchange DATE NOT NULL,
            Dividend NUMBER(10,2) NOT NULL,
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMBER(5) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        );""")

        # Silver layer for DimCompany with history tracking
        ddls.append("""
        CREATE OR REPLACE TEMPORARY TABLE TPC_DI.STAGING.SILVER_DIM_COMPANY (
            SK_CompanyID NUMBER(11) NOT NULL,
            CompanyID NUMBER(11) NOT NULL,
            Status VARCHAR(10) NOT NULL,
            Name VARCHAR(60) NOT NULL,
            Industry VARCHAR(50) NOT NULL,
            SPRating VARCHAR(4),
            isLowGrade BOOLEAN,
            CEO VARCHAR(100) NOT NULL,
            AddressLine1 VARCHAR(80),
            AddressLine2 VARCHAR(80),
            PostalCode VARCHAR(12) NOT NULL,
            City VARCHAR(25) NOT NULL,
            StateProv VARCHAR(20) NOT NULL,
            Country VARCHAR(24),
            Description VARCHAR(150) NOT NULL,
            FoundingDate DATE,
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMBER(5) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        );""")
        
        return ddls        
    
    def create_iceberg_tables(self)  -> list[str]:
        
        ddls = []
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.SILVER_FINWIRE_CMP (
            PTS TIMESTAMP NOT NULL,
            CompanyName STRING NOT NULL,
            CIK STRING NOT NULL,
            Status STRING NOT NULL,
            IndustryID STRING NOT NULL,
            SPRating STRING,
            FoundingDate DATE,
            AddrLine1 STRING NOT NULL,
            AddrLine2 STRING,
            PostalCode STRING NOT NULL,
            City STRING NOT NULL,
            StateProvince STRING NOT NULL,
            Country STRING,
            CEOName STRING NOT NULL,
            Description STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/silver/silver_finwire_cmp';
        """)

        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.SILVER_FINWIRE_SEC (
            PTS TIMESTAMP NOT NULL,
            Symbol STRING NOT NULL,
            IssueType STRING NOT NULL,
            Status STRING NOT NULL,
            Name STRING NOT NULL,
            ExID STRING NOT NULL,
            ShOut NUMBER(12,0) NOT NULL,
            FirstTradeDate DATE NOT NULL,
            FirstTradeExchg DATE NOT NULL,
            Dividend NUMBER(10,2) NOT NULL,
            CoNameOrCIK STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/silver/silver_finwire_sec';
        """)

        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.SILVER_FINWIRE_FIN (
            PTS TIMESTAMP NOT NULL,
            Year NUMBER(4,0) NOT NULL,
            Quarter STRING NOT NULL,
            QtrStartDate DATE NOT NULL,
            PostingDate DATE NOT NULL,
            Revenue NUMBER(15,2) NOT NULL,
            Earnings NUMBER(15,2) NOT NULL,
            EPS NUMBER(10,2) NOT NULL,
            DilutedEPS NUMBER(10,2) NOT NULL,
            Margin NUMBER(10,2) NOT NULL,
            Inventory NUMBER(15,2) NOT NULL,
            Assets NUMBER(15,2) NOT NULL,
            Liabilities NUMBER(15,2) NOT NULL,
            ShOut NUMBER(12,0) NOT NULL,
            DilutedShOut NUMBER(12,0) NOT NULL,
            CoNameOrCIK STRING NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/silver/silver_finwire_fin';
        """)

        # Silver layer for parsed CustomerMgmt XML 
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.SILVER_CUSTOMER_MGMT (
            ActionType STRING NOT NULL,
            ActionTS TIMESTAMP NOT NULL,
            C_ID NUMBER(11,0) NOT NULL,
            C_TAX_ID STRING,
            C_GNDR STRING,
            C_TIER NUMBER(1,0),
            C_DOB DATE,
            C_L_NAME STRING,
            C_F_NAME STRING,
            C_M_NAME STRING,
            C_ADLINE1 STRING,
            C_ADLINE2 STRING,
            C_ZIPCODE STRING,
            C_CITY STRING,
            C_STATE_PROV STRING,
            C_CTRY STRING,
            C_PRIM_EMAIL STRING,
            C_ALT_EMAIL STRING,
            C_PHONE_1 STRING,
            C_PHONE_2 STRING,
            C_PHONE_3 STRING,
            C_LCL_TX_ID STRING,
            C_NAT_TX_ID STRING,
            CA_ID NUMBER(11,0),
            CA_TAX_ST NUMBER(1,0),
            CA_B_ID NUMBER(11,0),
            CA_NAME STRING
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/silver/silver_customer_mgt';
        """)

        # Silver layer for DimCustomer with history tracking
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.SILVER_DIM_CUSTOMER (
            SK_CustomerID NUMBER(11,0) NOT NULL,
            CustomerID NUMBER(11,0) NOT NULL,
            TaxID STRING NOT NULL,
            Status STRING NOT NULL,
            LastName STRING NOT NULL,
            FirstName STRING NOT NULL,
            MiddleInitial STRING,
            Gender STRING,
            Tier NUMBER(1,0),
            DOB DATE NOT NULL,
            AddressLine1 STRING NOT NULL,
            AddressLine2 STRING,
            PostalCode STRING NOT NULL,
            City STRING NOT NULL,
            StateProv STRING NOT NULL,
            Country STRING,
            Phone1 STRING,
            Phone2 STRING,
            Phone3 STRING,
            Email1 STRING,
            Email2 STRING,
            NationalTaxRateDesc STRING,
            NationalTaxRate NUMBER(6,5),
            LocalTaxRateDesc STRING,
            LocalTaxRate NUMBER(6,5),
            AgencyID STRING,
            CreditRating NUMBER(5,0),
            NetWorth NUMBER(10,0),
            MarketingNameplate STRING,
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMBER(5,0) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/silver/silver_dim_customer';
        """)

        # Silver layer for DimAccount with history tracking
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.SILVER_DIM_ACCOUNT (
            SK_AccountID NUMBER(11,0) NOT NULL,
            AccountID NUMBER(11,0) NOT NULL,
            SK_BrokerID NUMBER(11,0) NOT NULL,
            SK_CustomerID NUMBER(11,0) NOT NULL,
            Status STRING NOT NULL,
            AccountDesc STRING,
            TaxStatus NUMBER(1,0),
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMBER(5,0) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/silver/silver_dim_account';""")

        # Silver layer for DimSecurity with history tracking
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.SILVER_DIM_SECURITY (
            SK_SecurityID NUMBER(11,0) NOT NULL,
            Symbol STRING NOT NULL,
            Issue STRING NOT NULL,
            Status STRING NOT NULL,
            Name STRING NOT NULL,
            ExchangeID STRING NOT NULL,
            SK_CompanyID NUMBER(11,0) NOT NULL,
            SharesOutstanding NUMBER(12,0) NOT NULL,
            FirstTrade DATE NOT NULL,
            FirstTradeOnExchange DATE NOT NULL,
            Dividend NUMBER(10,2) NOT NULL,
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMBER(5,0) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/silver/silver_dim_security';""")

        # Silver layer for DimCompany with history tracking
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.STAGING.SILVER_DIM_COMPANY (
            SK_CompanyID NUMBER(11,0) NOT NULL,
            CompanyID NUMBER(11,0) NOT NULL,
            Status STRING NOT NULL,
            Name STRING NOT NULL,
            Industry STRING NOT NULL,
            SPRating STRING,
            isLowGrade BOOLEAN,
            CEO STRING NOT NULL,
            AddressLine1 STRING,
            AddressLine2 STRING,
            PostalCode STRING NOT NULL,
            City STRING NOT NULL,
            StateProv STRING NOT NULL,
            Country STRING,
            Description STRING NOT NULL,
            FoundingDate DATE,
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMBER(5,0) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/silver/silver_dim_company';""")
        
        return ddls
    
    def transform_staged_data(self, historical:bool = False) -> list[str]:
        queries = self.process_finwire_records() + self.process_customer_mgmt()
        if historical == True:
            queries.append(self.process_dim_customer_updates())
        return self.process_dim_customer_updates()
    
    
    def process_finwire_records(self):
        
        queries = [
            "TRUNCATE TPC_DI.STAGING.SILVER_FINWIRE_CMP",
            "TRUNCATE TPC_DI.STAGING.SILVER_FINWIRE_SEC",
            "TRUNCATE TPC_DI.STAGING.SILVER_FINWIRE_FIN"
        ]
        
        queries.append("""INSERT INTO TPC_DI.STAGING.SILVER_FINWIRE_CMP
        SELECT 
            TO_TIMESTAMP(SUBSTRING(RawText, 1, 15), 'YYYYMMDD-HH24MISS') as PTS,
            TRIM(SUBSTRING(RawText, 19, 60)) as CompanyName,
            TRIM(SUBSTRING(RawText, 79, 10)) as CIK,
            TRIM(SUBSTRING(RawText, 89, 4)) as Status,
            TRIM(SUBSTRING(RawText, 93, 2)) as IndustryID,
            NULLIF(TRIM(SUBSTRING(RawText, 95, 4)), '') as SPRating,
            CASE 
                WHEN NULLIF(TRIM(SUBSTRING(RawText, 99, 8)), '') IS NOT NULL 
                THEN TO_DATE(SUBSTRING(RawText, 99, 8), 'YYYYMMDD')
            END as FoundingDate,
            TRIM(SUBSTRING(RawText, 107, 80)) as AddrLine1,
            NULLIF(TRIM(SUBSTRING(RawText, 187, 80)), '') as AddrLine2,
            TRIM(SUBSTRING(RawText, 267, 12)) as PostalCode,
            TRIM(SUBSTRING(RawText, 279, 25)) as City,
            TRIM(SUBSTRING(RawText, 304, 20)) as StateProvince,
            NULLIF(TRIM(SUBSTRING(RawText, 324, 24)), '') as Country,
            TRIM(SUBSTRING(RawText, 348, 46)) as CEOName,
            TRIM(SUBSTRING(RawText, 394, 150)) as Description
        FROM TPC_DI.STAGING.FINWIRE
        WHERE SUBSTRING(RawText, 16, 3) = 'CMP';""")

        # Parse SEC records
        queries.append("""INSERT INTO TPC_DI.STAGING.SILVER_FINWIRE_SEC
        SELECT 
            TO_TIMESTAMP(SUBSTRING(RawText, 1, 15), 'YYYYMMDD-HH24MISS') as PTS,
            TRIM(SUBSTRING(RawText, 19, 15)) as Symbol,
            TRIM(SUBSTRING(RawText, 34, 6)) as IssueType,
            TRIM(SUBSTRING(RawText, 40, 4)) as Status,
            TRIM(SUBSTRING(RawText, 44, 70)) as Name,
            TRIM(SUBSTRING(RawText, 114, 6)) as ExID,
            CAST(TRIM(SUBSTRING(RawText, 120, 13)) AS NUMBER(12,0)) as ShOut,
            TO_DATE(SUBSTRING(RawText, 133, 8), 'YYYYMMDD') as FirstTradeDate,
            TO_DATE(SUBSTRING(RawText, 141, 8), 'YYYYMMDD') as FirstTradeExchg,
            CAST(TRIM(SUBSTRING(RawText, 149, 12)) AS NUMBER(10,2)) as Dividend,
            TRIM(SUBSTRING(RawText, 161, 60)) as CoNameOrCIK
        FROM TPC_DI.STAGING.FINWIRE
        WHERE SUBSTRING(RawText, 16, 3) = 'SEC';""")

        # Parse FIN records
        queries.append("""INSERT INTO TPC_DI.STAGING.SILVER_FINWIRE_FIN
        SELECT 
            TO_TIMESTAMP(SUBSTRING(RawText, 1, 15), 'YYYYMMDD-HH24MISS') as PTS,
            CAST(SUBSTRING(RawText, 19, 4) AS NUMBER(4)) as Year,
            SUBSTRING(RawText, 23, 1) as Quarter,
            TO_DATE(SUBSTRING(RawText, 24, 8), 'YYYYMMDD') as QtrStartDate,
            TO_DATE(SUBSTRING(RawText, 32, 8), 'YYYYMMDD') as PostingDate,
            CAST(TRIM(SUBSTRING(RawText, 40, 17)) AS NUMBER(15,2)) as Revenue,
            CAST(TRIM(SUBSTRING(RawText, 57, 17)) AS NUMBER(15,2)) as Earnings,
            CAST(TRIM(SUBSTRING(RawText, 74, 12)) AS NUMBER(10,2)) as EPS,
            CAST(TRIM(SUBSTRING(RawText, 86, 12)) AS NUMBER(10,2)) as DilutedEPS,
            CAST(TRIM(SUBSTRING(RawText, 98, 12)) AS NUMBER(10,2)) as Margin,
            CAST(TRIM(SUBSTRING(RawText, 110, 17)) AS NUMBER(15,2)) as Inventory,
            CAST(TRIM(SUBSTRING(RawText, 127, 17)) AS NUMBER(15,2)) as Assets,
            CAST(TRIM(SUBSTRING(RawText, 144, 17)) AS NUMBER(15,2)) as Liabilities,
            CAST(TRIM(SUBSTRING(RawText, 161, 13)) AS NUMBER(12,0)) as ShOut,
            CAST(TRIM(SUBSTRING(RawText, 174, 13)) AS NUMBER(12,0)) as DilutedShOut,
            TRIM(SUBSTRING(RawText, 187, 60)) as CoNameOrCIK
        FROM TPC_DI.STAGING.FINWIRE
        WHERE SUBSTRING(RawText, 16, 3) = 'FIN';""")
        
        return queries
    
    # Note: Since we converted to JSON, we can directly query the JSON structure
    def process_customer_mgmt(self):
        return [
            """
            INSERT INTO TPC_DI.STAGING.SILVER_CUSTOMER_MGMT
            SELECT
                ActionType,
                TO_TIMESTAMP(ActionTS) as ActionTS,
                C_ID,
                C_TAX_ID,
                C_GNDR,
                C_TIER,
                CASE 
                    WHEN C_DOB IS NOT NULL THEN TO_DATE(C_DOB)
                END as C_DOB,
                C_L_NAME,
                C_F_NAME,
                C_M_NAME,
                C_ADLINE1,
                C_ADLINE2,
                C_ZIPCODE,
                C_CITY,
                C_STATE_PROV,
                C_CTRY,
                C_PRIM_EMAIL,
                C_ALT_EMAIL,
                C_PHONE_1,
                C_PHONE_2,
                C_PHONE_3,
                C_LCL_TX_ID,
                C_NAT_TX_ID,
                accounts.CA_ID,
                accounts.CA_TAX_ST,
                accounts.CA_B_ID,
                accounts.CA_NAME
            FROM TPC_DI.STAGING.CUSTOMERMGMT customers
            LEFT JOIN TPC_DI.STAGING.ACCOUNT accounts
                ON customers.C_ID = accounts.CA_C_ID
                WHERE C_ID is not NULL;
            """]
        
    def process_dim_customer_updates(self):
        return [
        """
        INSERT INTO TPC_DI.STAGING.SILVER_DIM_CUSTOMER
        WITH new_customers AS (
            -- Get only new/update records not currently in DimCustomer
            SELECT 
                cm.*,
                t1.TX_NAME as NationalTaxRateDesc,
                t1.TX_RATE as NationalTaxRate,
                t2.TX_NAME as LocalTaxRateDesc, 
                t2.TX_RATE as LocalTaxRate,
                p.AgencyID,
                p.CreditRating,
                p.NetWorth,
                p.Income,
                p.NumberChildren,
                p.NumberCreditCards,
                p.Age,
                p.NumberCars
            FROM TPC_DI.STAGING.SILVER_CUSTOMER_MGMT cm
            LEFT JOIN TPC_DI.STAGING.TAXRATE t1 ON cm.C_NAT_TX_ID = t1.TX_ID
            LEFT JOIN TPC_DI.STAGING.TAXRATE t2 ON cm.C_LCL_TX_ID = t2.TX_ID
            LEFT JOIN TPC_DI.STAGING.PROSPECT p ON 
                UPPER(cm.C_L_NAME) = UPPER(p.LastName) AND
                UPPER(cm.C_F_NAME) = UPPER(p.FirstName) AND
                UPPER(COALESCE(cm.C_ADLINE1,'')) = UPPER(COALESCE(p.AddressLine1,'')) AND
                UPPER(COALESCE(cm.C_ADLINE2,'')) = UPPER(COALESCE(p.AddressLine2,'')) AND
                UPPER(COALESCE(cm.C_ZIPCODE,'')) = UPPER(COALESCE(p.PostalCode,''))
            WHERE cm.ActionType IN ('NEW', 'UPDCUST', 'INACT')
            AND NOT EXISTS (
                SELECT 1 FROM TPC_DI.STAGING.SILVER_DIM_CUSTOMER dc 
                WHERE dc.CustomerID = cm.C_ID AND dc.IsCurrent = TRUE
            )
        )
        SELECT 
            -- Generate SK inline using existing max + row_number
            COALESCE((SELECT MAX(SK_CustomerID) FROM TPC_DI.STAGING.SILVER_DIM_CUSTOMER), 0) + 
            ROW_NUMBER() OVER (ORDER BY C_ID) as SK_CustomerID,
            C_ID as CustomerID,
            C_TAX_ID as TaxID,
            CASE 
                WHEN ActionType = 'INACT' THEN 'INACTIVE'
                ELSE 'ACTIVE'
            END as Status,
            C_L_NAME as LastName,
            C_F_NAME as FirstName,
            C_M_NAME as MiddleInitial,
            COALESCE(UPPER(C_GNDR), 'U') as Gender,
            C_TIER as Tier,
            C_DOB as DOB,
            C_ADLINE1 as AddressLine1,
            C_ADLINE2 as AddressLine2,
            C_ZIPCODE as PostalCode,
            C_CITY as City,
            C_STATE_PROV as StateProv,
            C_CTRY as Country,
            -- Phone formatting following spec rules
            CASE 
                WHEN C_PHONE_1 IS NOT NULL THEN
                    CONCAT(
                        CASE WHEN SPLIT_PART(C_PHONE_1, '|', 1) != '' THEN '+' || SPLIT_PART(C_PHONE_1, '|', 1) || ' ' ELSE '' END,
                        CASE WHEN SPLIT_PART(C_PHONE_1, '|', 2) != '' THEN '(' || SPLIT_PART(C_PHONE_1, '|', 2) || ') ' ELSE '' END,
                        SPLIT_PART(C_PHONE_1, '|', 3),
                        CASE WHEN SPLIT_PART(C_PHONE_1, '|', 4) != '' THEN ' ' || SPLIT_PART(C_PHONE_1, '|', 4) ELSE '' END
                    )
            END as Phone1,
            CASE 
                WHEN C_PHONE_2 IS NOT NULL THEN
                    CONCAT(
                        CASE WHEN SPLIT_PART(C_PHONE_2, '|', 1) != '' THEN '+' || SPLIT_PART(C_PHONE_2, '|', 1) || ' ' ELSE '' END,
                        CASE WHEN SPLIT_PART(C_PHONE_2, '|', 2) != '' THEN '(' || SPLIT_PART(C_PHONE_2, '|', 2) || ') ' ELSE '' END,
                        SPLIT_PART(C_PHONE_2, '|', 3),
                        CASE WHEN SPLIT_PART(C_PHONE_2, '|', 4) != '' THEN ' ' || SPLIT_PART(C_PHONE_2, '|', 4) ELSE '' END
                    )
            END as Phone2,
            CASE 
                WHEN C_PHONE_3 IS NOT NULL THEN
                    CONCAT(
                        CASE WHEN SPLIT_PART(C_PHONE_3, '|', 1) != '' THEN '+' || SPLIT_PART(C_PHONE_3, '|', 1) || ' ' ELSE '' END,
                        CASE WHEN SPLIT_PART(C_PHONE_3, '|', 2) != '' THEN '(' || SPLIT_PART(C_PHONE_3, '|', 2) || ') ' ELSE '' END,
                        SPLIT_PART(C_PHONE_3, '|', 3),
                        CASE WHEN SPLIT_PART(C_PHONE_3, '|', 4) != '' THEN ' ' || SPLIT_PART(C_PHONE_3, '|', 4) ELSE '' END
                    )
            END as Phone3,
            C_PRIM_EMAIL as Email1,
            C_ALT_EMAIL as Email2,
            NationalTaxRateDesc,
            NationalTaxRate,
            LocalTaxRateDesc,
            LocalTaxRate,
            AgencyID,
            CreditRating,
            NetWorth,
            -- Marketing Nameplate logic
            CASE
                WHEN NetWorth > 1000000 OR Income > 200000 THEN 
                    CASE 
                        WHEN Age < 25 AND NetWorth > 1000000 THEN 'HighValue+Inherited'
                        ELSE 'HighValue'
                    END
                WHEN NumberChildren > 3 OR NumberCreditCards > 5 THEN 'Expenses'
                WHEN Age > 45 THEN 'Boomer'
                WHEN Income < 50000 OR CreditRating < 600 OR NetWorth < 100000 THEN 'MoneyAlert'
                WHEN NumberCars > 3 OR NumberCreditCards > 7 THEN 'Spender'
                WHEN Age < 25 AND NetWorth > 1000000 THEN 'Inherited'
                ELSE NULL
            END as MarketingNameplate,
            TRUE as IsCurrent,
            1 as BatchID, -- Assuming this is first batch
            DATE(ActionTS) as EffectiveDate,
            '9999-12-31' as EndDate
        FROM new_customers;
        """,
        """
        -- Update existing records to not be current
        UPDATE TPC_DI.STAGING.SILVER_DIM_CUSTOMER dc
        SET 
            IsCurrent = FALSE,
            EndDate = DATE(cm.ActionTS)
        FROM TPC_DI.STAGING.SILVER_CUSTOMER_MGMT cm
        WHERE dc.CustomerID = cm.C_ID 
        AND dc.IsCurrent = TRUE
        AND cm.ActionType IN ('UPDCUST', 'INACT');
        """]