"""
Database Manager
Handles SQLite database operations for MCA data
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path: str = "data/mca_master.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
        self.initialize_database()
    
    def get_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(str(self.db_path))
        return self.conn
    
    def initialize_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                cin TEXT PRIMARY KEY,
                company_name TEXT,
                company_class TEXT,
                company_category TEXT,
                company_sub_category TEXT,
                date_of_incorporation TEXT,
                authorized_capital REAL,
                paidup_capital REAL,
                company_status TEXT,
                principal_business_activity TEXT,
                registered_office_address TEXT,
                roc_code TEXT,
                state TEXT,
                data_quality_score REAL,
                last_updated TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cin TEXT,
                company_name TEXT,
                change_type TEXT,
                field_changed TEXT,
                old_value TEXT,
                new_value TEXT,
                date TEXT,
                state TEXT,
                status TEXT,
                FOREIGN KEY (cin) REFERENCES companies(cin)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS enriched_data (
                cin TEXT PRIMARY KEY,
                industry TEXT,
                sector TEXT,
                directors TEXT,
                gstin TEXT,
                gst_status TEXT,
                compliance_status TEXT,
                source TEXT,
                source_urls TEXT,
                enrichment_date TEXT,
                FOREIGN KEY (cin) REFERENCES companies(cin)
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_company_status 
            ON companies(company_status)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_state 
            ON companies(state)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_change_type 
            ON changes(change_type)
        """)
        
        conn.commit()
        logger.info("Database initialized successfully")
    
    def insert_companies(self, df: pd.DataFrame):
        conn = self.get_connection()
        
        columns_map = {
            'CIN': 'cin',
            'COMPANY_NAME': 'company_name',
            'COMPANY_CLASS': 'company_class',
            'COMPANY_CATEGORY': 'company_category',
            'COMPANY_SUB_CATEGORY': 'company_sub_category',
            'DATE_OF_INCORPORATION': 'date_of_incorporation',
            'AUTHORIZED_CAPITAL': 'authorized_capital',
            'PAIDUP_CAPITAL': 'paidup_capital',
            'COMPANY_STATUS': 'company_status',
            'PRINCIPAL_BUSINESS_ACTIVITY': 'principal_business_activity',
            'REGISTERED_OFFICE_ADDRESS': 'registered_office_address',
            'ROC_CODE': 'roc_code',
            'STATE': 'state',
            'DATA_QUALITY_SCORE': 'data_quality_score',
            'LAST_UPDATED': 'last_updated'
        }
        
        df_renamed = df.rename(columns=columns_map)
        
        df_renamed.to_sql('companies', conn, if_exists='replace', index=False)
        
        logger.info(f"Inserted {len(df)} companies into database")
    
    def insert_changes(self, df: pd.DataFrame):
        conn = self.get_connection()
        
        columns_map = {
            'CIN': 'cin',
            'COMPANY_NAME': 'company_name',
            'CHANGE_TYPE': 'change_type',
            'FIELD_CHANGED': 'field_changed',
            'OLD_VALUE': 'old_value',
            'NEW_VALUE': 'new_value',
            'DATE': 'date',
            'STATE': 'state',
            'STATUS': 'status'
        }
        
        df_renamed = df.rename(columns=columns_map)
        
        df_renamed.to_sql('changes', conn, if_exists='append', index=False)
        
        logger.info(f"Inserted {len(df)} changes into database")
    
    def insert_enriched_data(self, df: pd.DataFrame):
        conn = self.get_connection()
        
        columns_map = {
            'CIN': 'cin',
            'INDUSTRY': 'industry',
            'SECTOR': 'sector',
            'DIRECTORS': 'directors',
            'GSTIN': 'gstin',
            'GST_STATUS': 'gst_status',
            'COMPLIANCE_STATUS': 'compliance_status',
            'SOURCE': 'source',
            'SOURCE_URLS': 'source_urls',
            'ENRICHMENT_DATE': 'enrichment_date'
        }
        
        df_renamed = df.rename(columns=columns_map)
        
        df_renamed.to_sql('enriched_data', conn, if_exists='replace', index=False)
        
        logger.info(f"Inserted {len(df)} enriched records into database")
    
    def search_company(self, query: str) -> pd.DataFrame:
        conn = self.get_connection()
        
        sql = """
            SELECT * FROM companies 
            WHERE cin LIKE ? OR company_name LIKE ?
            LIMIT 100
        """
        
        search_term = f"%{query}%"
        df = pd.read_sql_query(sql, conn, params=(search_term, search_term))
        
        return df
    
    def get_company_by_cin(self, cin: str) -> Optional[Dict]:
        conn = self.get_connection()
        
        sql = "SELECT * FROM companies WHERE cin = ?"
        df = pd.read_sql_query(sql, conn, params=(cin,))
        
        if df.empty:
            return None
        
        return df.iloc[0].to_dict()
    
    def get_changes_by_date(self, date: str) -> pd.DataFrame:
        conn = self.get_connection()
        
        sql = "SELECT * FROM changes WHERE date = ?"
        df = pd.read_sql_query(sql, conn, params=(date,))
        
        return df
    
    def get_statistics(self) -> Dict:
        conn = self.get_connection()
        
        stats = {}
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM companies")
        stats['total_companies'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM companies WHERE company_status = 'Active'")
        stats['active_companies'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT state) FROM companies")
        stats['states_covered'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM changes")
        stats['total_changes'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM enriched_data")
        stats['enriched_count'] = cursor.fetchone()[0]
        
        return stats
    
    def get_companies_by_state(self, state: str) -> pd.DataFrame:
        conn = self.get_connection()
        
        sql = "SELECT * FROM companies WHERE state = ?"
        df = pd.read_sql_query(sql, conn, params=(state,))
        
        return df
    
    def get_companies_by_status(self, status: str) -> pd.DataFrame:
        conn = self.get_connection()
        
        sql = "SELECT * FROM companies WHERE company_status = ?"
        df = pd.read_sql_query(sql, conn, params=(status,))
        
        return df
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")

def main():
    db = DatabaseManager()
    
    processed_dir = Path("data/processed")
    master_file = processed_dir / "current_master.csv"
    
    if master_file.exists():
        print("Loading master data into database...")
        master_df = pd.read_csv(master_file)
        db.insert_companies(master_df)
        
        stats = db.get_statistics()
        print("\n=== Database Statistics ===")
        for key, value in stats.items():
            print(f"{key}: {value}")
    
    change_logs_dir = Path("data/change_logs")
    change_files = list(change_logs_dir.glob("change_log_*.csv"))
    
    if change_files:
        print(f"\nLoading {len(change_files)} change logs into database...")
        for file in change_files:
            changes_df = pd.read_csv(file)
            db.insert_changes(changes_df)
    
    enriched_dir = Path("data/enriched")
    enriched_file = enriched_dir / "current_enriched.csv"
    
    if enriched_file.exists():
        print("\nLoading enriched data into database...")
        enriched_df = pd.read_csv(enriched_file)
        db.insert_enriched_data(enriched_df)
    
    print("\n Database setup complete!")
    
    db.close()

if __name__ == "__main__":
    main()