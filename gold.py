class GoldLayerManager:
    def __init__(self, external_volume: str, base_location: str):
        self.external_volume = external_volume
        self.base_location = base_location

    def create_tables(self) -> list[str]:
        """
        Generate DDL statements for TPC-DI gold/fact Iceberg tables
        Args:
            external_volume: Name of external volume
            base_location: Base location for table files
        Returns:
            List of DDL SQL statements
        """
        ddls = []
        
        # DimTrade - hybrid dimension/fact table
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.LOAD.DimTrade (
            TradeID NUMBER(15,0) NOT NULL,           
            SK_BrokerID NUMBER(11,0),                 
            SK_CreateDateID NUMBER(11,0) NOT NULL,   
            SK_CreateTimeID NUMBER(11,0) NOT NULL,   
            SK_CloseDateID NUMBER(11,0),             
            SK_CloseTimeID NUMBER(11,0),             
            Status STRING NOT NULL,
            Type STRING NOT NULL,
            CashFlag BOOLEAN NOT NULL,
            SK_SecurityID NUMBER(11,0) NOT NULL,     
            SK_CompanyID NUMBER(11,0) NOT NULL,      
            Quantity NUMBER(6,0) NOT NULL,           
            BidPrice NUMBER(8,2) NOT NULL,           
            SK_CustomerID NUMBER(11,0) NOT NULL,     
            SK_AccountID NUMBER(11,0) NOT NULL,      
            ExecutedBy STRING NOT NULL,
            TradePrice NUMBER(8,2),                  
            Fee NUMBER(10,2),                        
            Commission NUMBER(10,2),                 
            Tax NUMBER(10,2),                        
            BatchID NUMBER(5,0) NOT NULL
        )
        CLUSTER BY (SK_CreateDateID)
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/dimtrade';
        """)

        # FactCashBalances
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.LOAD.FactCashBalances (
            SK_CustomerID NUMBER(11,0) NOT NULL,     
            SK_AccountID NUMBER(11,0) NOT NULL,      
            SK_DateID NUMBER(11,0) NOT NULL,         
            Cash NUMBER(12,2) NOT NULL,              
            BatchID NUMBER(5,0) NOT NULL
        )
        CLUSTER BY (SK_DateID)
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/factcashbalances';
        """)

        # FactHoldings
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.LOAD.FactHoldings (
            TradeID NUMBER(15,0) NOT NULL,           
            CurrentTradeID NUMBER(15,0) NOT NULL,    
            SK_CustomerID NUMBER(11,0) NOT NULL,     
            SK_AccountID NUMBER(11,0) NOT NULL,      
            SK_SecurityID NUMBER(11,0) NOT NULL,     
            SK_CompanyID NUMBER(11,0) NOT NULL,      
            SK_DateID NUMBER(11,0) NOT NULL,         
            SK_TimeID NUMBER(11,0) NOT NULL,         
            CurrentPrice NUMBER(8,2) NOT NULL,       
            CurrentHolding NUMBER(6,0) NOT NULL,     
            BatchID NUMBER(5,0) NOT NULL
        )
        CLUSTER BY (SK_DateID)
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/factholdings';
        """)

        # FactMarketHistory
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.LOAD.FactMarketHistory (
            SK_SecurityID NUMBER(11,0) NOT NULL,    
            SK_CompanyID NUMBER(11,0) NOT NULL,     
            SK_DateID NUMBER(11,0) NOT NULL,        
            PERatio NUMBER(10,2),                   
            Yield NUMBER(5,2) NOT NULL,
            FiftyTwoWeekHigh NUMBER(8,2) NOT NULL,   
            SK_FiftyTwoWeekHighDate NUMBER(11,0) NOT NULL,
            FiftyTwoWeekLow NUMBER(8,2) NOT NULL,    
            SK_FiftyTwoWeekLowDate NUMBER(11,0) NOT NULL,
            ClosePrice NUMBER(8,2) NOT NULL,         
            DayHigh NUMBER(8,2) NOT NULL,            
            DayLow NUMBER(8,2) NOT NULL,             
            Volume NUMBER(12,0) NOT NULL,            
            BatchID NUMBER(5,0) NOT NULL
        )
        CLUSTER BY (SK_DateID)
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/factmarkethistory';
        """)

        # FactWatches
        ddls.append(f"""
        CREATE OR REPLACE ICEBERG TABLE TPC_DI.LOAD.FactWatches (
            SK_CustomerID NUMBER(11,0) NOT NULL,    
            SK_SecurityID NUMBER(11,0) NOT NULL,    
            SK_DateID_DatePlaced NUMBER(11,0) NOT NULL,
            SK_DateID_DateRemoved NUMBER(11,0),     
            BatchID NUMBER(5,0) NOT NULL
        )
        EXTERNAL_VOLUME = '{self.external_volume}'
        CATALOG = 'SNOWFLAKE'
        BASE_LOCATION = '{self.base_location}/factwatches';
        """)

        return ddls