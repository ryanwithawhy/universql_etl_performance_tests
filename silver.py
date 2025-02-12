class SilverLayerManager:
    def __init__(self):
        pass
    
    def create_tables(self) -> list[str]:
        # Silver layer for FINWIRE parsed records
        ddls = []
        
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
