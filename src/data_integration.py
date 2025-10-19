"""
MCA Data Integration Module
Handles merging, cleaning, and standardization of state-wise MCA data
"""

import pandas as pd
import numpy as np
import os
import logging
from pathlib import Path
from typing import List, Dict, Optional
import hashlib
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MCADataIntegrator:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.snapshots_dir = self.data_dir / "snapshots"
        
        for dir_path in [self.raw_dir, self.processed_dir, self.snapshots_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        self.column_mapping = {
            'CORPORATE_IDENTIFICATION_NUMBER': 'CIN',
            'CIN': 'CIN',
            'COMPANY_NAME': 'COMPANY_NAME',
            'COMPANY_CLASS': 'COMPANY_CLASS',
            'COMPANY_CATEGORY': 'COMPANY_CATEGORY',
            'COMPANY_SUB_CATEGORY': 'COMPANY_SUB_CATEGORY',
            'DATE_OF_REGISTRATION': 'DATE_OF_INCORPORATION',
            'DATE_OF_INCORPORATION': 'DATE_OF_INCORPORATION',
            'AUTHORIZED_CAPITAL': 'AUTHORIZED_CAPITAL',
            'PAIDUP_CAPITAL': 'PAIDUP_CAPITAL',
            'COMPANY_STATUS': 'COMPANY_STATUS',
            'PRINCIPAL_BUSINESS_ACTIVITY': 'PRINCIPAL_BUSINESS_ACTIVITY',
            'REGISTERED_OFFICE_ADDRESS': 'REGISTERED_OFFICE_ADDRESS',
            'ROC_CODE': 'ROC_CODE',
            'STATE': 'STATE'
        }
    
    def load_state_data(self, state: str, file_path: Optional[str] = None) -> pd.DataFrame:
        try:
            if file_path:
                df = pd.read_csv(file_path, low_memory=False)
            else:
                state_normalized = state.lower().replace(' ', '_')
                csv_files = list(self.raw_dir.glob("*.csv"))
                
                logger.info(f"Looking for {state} data...")
                logger.info(f"Available files: {[f.name for f in csv_files]}")
                
                state_file = None
                for csv_file in csv_files:
                    if state_normalized in csv_file.name.lower():
                        state_file = csv_file
                        break
                
                if not state_file:
                    logger.warning(f"No file found for state: {state}")
                    return pd.DataFrame()
                
                logger.info(f"Loading file: {state_file.name}")
                df = pd.read_csv(state_file, low_memory=False)
            
            if 'STATE' not in df.columns:
                df['STATE'] = state
            
            logger.info(f"Loaded {len(df)} records for {state}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading data for {state}: {e}")
            return pd.DataFrame()
    
    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.upper().str.strip()
        
        for old_name, new_name in self.column_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})
        
        required_columns = list(set(self.column_mapping.values()))
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan
        
        return df[required_columns]
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates(subset=['CIN'], keep='last')
        
        df['CIN'] = df['CIN'].astype(str).str.strip()
        
        df = df[df['CIN'].str.len() == 21]
        
        df['COMPANY_NAME'] = df['COMPANY_NAME'].astype(str).str.strip().str.upper()
        
        date_columns = ['DATE_OF_INCORPORATION']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        
        capital_columns = ['AUTHORIZED_CAPITAL', 'PAIDUP_CAPITAL']
        for col in capital_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['COMPANY_STATUS'] = df['COMPANY_STATUS'].astype(str).str.strip().str.upper()
        
        df['DATA_QUALITY_SCORE'] = df.notna().sum(axis=1) / len(df.columns)
        
        df['LAST_UPDATED'] = datetime.now()
        
        logger.info(f"Cleaned data: {len(df)} records remaining")
        return df
    
    def merge_state_data(self, states: List[str]) -> pd.DataFrame:
        all_data = []
        
        for state in states:
            logger.info(f"Processing {state}...")
            df = self.load_state_data(state)
            
            if df.empty:
                continue
            
            df = self.standardize_columns(df)
            df = self.clean_data(df)
            all_data.append(df)
        
        if not all_data:
            logger.error("No data loaded from any state")
            return pd.DataFrame()
        
        master_df = pd.concat(all_data, ignore_index=True)
        
        master_df = master_df.drop_duplicates(subset=['CIN'], keep='last')
        
        master_df['RECORD_ID'] = master_df['CIN'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest()[:10]
        )
        
        master_df['INTEGRATION_DATE'] = datetime.now()
        master_df['SOURCE'] = 'MCA_MASTER'
        
        logger.info(f"Master dataset created: {len(master_df)} total records")
        return master_df
    
    def save_master_dataset(self, df: pd.DataFrame, filename: str = None) -> str:
        if filename is None:
            filename = f"mca_master_{datetime.now().strftime('%Y%m%d')}.csv"
        
        filepath = self.processed_dir / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Master dataset saved to: {filepath}")
        
        current_master = self.processed_dir / "current_master.csv"
        df.to_csv(current_master, index=False)
        
        return str(filepath)
    
    def create_snapshot(self, df: pd.DataFrame, snapshot_date: str = None) -> str:
        if snapshot_date is None:
            snapshot_date = datetime.now().strftime('%Y%m%d')
        
        snapshot_file = self.snapshots_dir / f"snapshot_{snapshot_date}.csv"
        df.to_csv(snapshot_file, index=False)
        logger.info(f"Snapshot created: {snapshot_file}")
        
        return str(snapshot_file)
    
    def validate_data(self, df: pd.DataFrame) -> Dict:
        stats = {
            'total_records': len(df),
            'unique_cins': df['CIN'].nunique(),
            'states_covered': df['STATE'].nunique(),
            'date_range': {
                'earliest': str(df['DATE_OF_INCORPORATION'].min()),
                'latest': str(df['DATE_OF_INCORPORATION'].max())
            },
            'status_distribution': df['COMPANY_STATUS'].value_counts().to_dict(),
            'missing_values': df.isnull().sum().to_dict(),
            'data_quality': {
                'avg_score': float(df['DATA_QUALITY_SCORE'].mean()),
                'high_quality': int(len(df[df['DATA_QUALITY_SCORE'] > 0.8])),
                'low_quality': int(len(df[df['DATA_QUALITY_SCORE'] < 0.5]))
            }
        }
        
        return stats

def main():
    integrator = MCADataIntegrator()
    
    states = ['Maharashtra', 'Gujarat', 'Delhi', 'Tamil Nadu', 'Karnataka']
    
    master_df = integrator.merge_state_data(states)
    
    if not master_df.empty:
        filepath = integrator.save_master_dataset(master_df)
        
        integrator.create_snapshot(master_df)
        
        stats = integrator.validate_data(master_df)
        
        print("\n=== Data Integration Complete ===")
        print(f"Total Records: {stats['total_records']}")
        print(f"Unique CINs: {stats['unique_cins']}")
        print(f"States Covered: {stats['states_covered']}")
        print(f"Average Data Quality Score: {stats['data_quality']['avg_score']:.2f}")
        print(f"Master Dataset: {filepath}")
    else:
        print("Error: No data was integrated")

if __name__ == "__main__":
    main()