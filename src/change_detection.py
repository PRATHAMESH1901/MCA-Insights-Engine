"""
MCA Change Detection Module
Detects and logs daily changes in company data
"""

import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChangeDetector:
    def __init__(self, snapshots_dir: str = "data/snapshots", logs_dir: str = "data/change_logs"):
        self.snapshots_dir = Path(snapshots_dir)
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        self.tracked_fields = [
            'COMPANY_NAME', 'COMPANY_CLASS', 'COMPANY_STATUS',
            'AUTHORIZED_CAPITAL', 'PAIDUP_CAPITAL', 
            'PRINCIPAL_BUSINESS_ACTIVITY', 'REGISTERED_OFFICE_ADDRESS'
        ]
    
    def load_snapshot(self, date_str: str) -> pd.DataFrame:
        """Load a snapshot for a specific date"""
        snapshot_file = self.snapshots_dir / f"snapshot_{date_str}.csv"
        
        if not snapshot_file.exists():
            logger.warning(f"Snapshot not found: {snapshot_file}")
            return pd.DataFrame()
        
        df = pd.read_csv(snapshot_file, low_memory=False)
        logger.info(f"Loaded snapshot: {date_str} ({len(df)} records)")
        return df
    
    def detect_changes(self, old_df: pd.DataFrame, new_df: pd.DataFrame, 
                      change_date: str = None) -> Dict[str, pd.DataFrame]:
        """Detect all types of changes between two snapshots"""
        if change_date is None:
            change_date = datetime.now().strftime('%Y-%m-%d')
        
        changes = {
            'new_incorporations': pd.DataFrame(),
            'deregistrations': pd.DataFrame(),
            'field_updates': pd.DataFrame()
        }
        
        old_df['CIN'] = old_df['CIN'].astype(str)
        new_df['CIN'] = new_df['CIN'].astype(str)
        
        old_cins = set(old_df['CIN'].unique())
        new_cins = set(new_df['CIN'].unique())
        
        new_incorporations = new_cins - old_cins
        if new_incorporations:
            changes['new_incorporations'] = new_df[new_df['CIN'].isin(new_incorporations)].copy()
            logger.info(f"Found {len(new_incorporations)} new incorporations")
        
        deregistered = old_cins - new_cins
        if deregistered:
            changes['deregistrations'] = old_df[old_df['CIN'].isin(deregistered)].copy()
            logger.info(f"Found {len(deregistered)} deregistrations")
        
        common_cins = old_cins & new_cins
        field_changes = []
        
        for cin in common_cins:
            old_record = old_df[old_df['CIN'] == cin].iloc[0]
            new_record = new_df[new_df['CIN'] == cin].iloc[0]
            
            for field in self.tracked_fields:
                if field in old_record and field in new_record:
                    old_value = str(old_record[field])
                    new_value = str(new_record[field])
                    
                    if old_value != new_value and not (pd.isna(old_value) and pd.isna(new_value)):
                        field_changes.append({
                            'CIN': cin,
                            'COMPANY_NAME': new_record.get('COMPANY_NAME', ''),
                            'CHANGE_TYPE': 'FIELD_UPDATE',
                            'FIELD_CHANGED': field,
                            'OLD_VALUE': old_value,
                            'NEW_VALUE': new_value,
                            'DATE': change_date
                        })
        
        if field_changes:
            changes['field_updates'] = pd.DataFrame(field_changes)
            logger.info(f"Found {len(field_changes)} field updates")
        
        return changes
    
    def create_change_log(self, changes: Dict[str, pd.DataFrame], 
                         log_date: str = None) -> Tuple[str, str]:
        """Create structured change logs in CSV and JSON format"""
        if log_date is None:
            log_date = datetime.now().strftime('%Y%m%d')
        
        all_changes = []

        for _, row in changes.get('new_incorporations', pd.DataFrame()).iterrows():
            all_changes.append({
                'CIN': row['CIN'],
                'COMPANY_NAME': row.get('COMPANY_NAME', ''),
                'CHANGE_TYPE': 'NEW_INCORPORATION',
                'FIELD_CHANGED': 'ALL',
                'OLD_VALUE': None,
                'NEW_VALUE': 'INCORPORATED',
                'DATE': log_date,
                'STATE': row.get('STATE', ''),
                'STATUS': row.get('COMPANY_STATUS', '')
            })
        
        for _, row in changes.get('deregistrations', pd.DataFrame()).iterrows():
            all_changes.append({
                'CIN': row['CIN'],
                'COMPANY_NAME': row.get('COMPANY_NAME', ''),
                'CHANGE_TYPE': 'DEREGISTRATION',
                'FIELD_CHANGED': 'COMPANY_STATUS',
                'OLD_VALUE': row.get('COMPANY_STATUS', ''),
                'NEW_VALUE': 'DEREGISTERED',
                'DATE': log_date,
                'STATE': row.get('STATE', ''),
                'STATUS': 'DEREGISTERED'
            })

        for _, row in changes.get('field_updates', pd.DataFrame()).iterrows():
            all_changes.append(row.to_dict())
        
        if all_changes:
            changes_df = pd.DataFrame(all_changes)
            csv_file = self.logs_dir / f"change_log_{log_date}.csv"
            changes_df.to_csv(csv_file, index=False)
            
            json_file = self.logs_dir / f"change_log_{log_date}.json"
            with open(json_file, 'w') as f:
                json.dump(all_changes, f, indent=2, default=str)
            
            logger.info(f"Change logs saved: {csv_file} and {json_file}")
            return str(csv_file), str(json_file)
        
        return "", ""
    
    def simulate_daily_changes(self, base_df: pd.DataFrame, 
                              num_days: int = 3) -> List[pd.DataFrame]:
        """Simulate daily changes for testing purposes"""
        snapshots = []
        current_df = base_df.copy()
        
        for day in range(num_days):
            modified_df = current_df.copy()

            num_new = np.random.randint(5, 11)
            new_companies = []
            
            for i in range(num_new):
                new_cin = f"U{np.random.randint(10000, 99999)}MH2024PTC{np.random.randint(100000, 999999)}"
                new_companies.append({
                    'CIN': new_cin,
                    'COMPANY_NAME': f"TEST COMPANY {day}_{i} PRIVATE LIMITED",
                    'COMPANY_CLASS': 'Private',
                    'COMPANY_STATUS': 'Active',
                    'AUTHORIZED_CAPITAL': np.random.randint(100000, 10000000),
                    'PAIDUP_CAPITAL': np.random.randint(50000, 5000000),
                    'STATE': np.random.choice(['Maharashtra', 'Gujarat', 'Delhi']),
                    'DATE_OF_INCORPORATION': datetime.now() - timedelta(days=np.random.randint(1, 30)),
                    'ROC_CODE': 'ROC-MUMBAI',
                    'DATA_QUALITY_SCORE': 0.9,
                    'LAST_UPDATED': datetime.now()
                })
            
            new_df = pd.DataFrame(new_companies)
            modified_df = pd.concat([modified_df, new_df], ignore_index=True)
            
            num_status_changes = np.random.randint(2, 6)
            status_change_indices = np.random.choice(
                modified_df.index, 
                size=min(num_status_changes, len(modified_df)), 
                replace=False
            )
            
            for idx in status_change_indices:
                current_status = modified_df.loc[idx, 'COMPANY_STATUS']
                if current_status == 'Active':
                    modified_df.loc[idx, 'COMPANY_STATUS'] = np.random.choice(['Strike Off', 'Under Liquidation'])
                
            num_capital_changes = np.random.randint(3, 8)
            capital_change_indices = np.random.choice(
                modified_df.index,
                size=min(num_capital_changes, len(modified_df)),
                replace=False
            )
            
            for idx in capital_change_indices:
                modified_df.loc[idx, 'AUTHORIZED_CAPITAL'] *= np.random.uniform(1.1, 2.0)
                modified_df.loc[idx, 'PAIDUP_CAPITAL'] *= np.random.uniform(1.05, 1.5)
            
            snapshot_date = (datetime.now() + timedelta(days=day)).strftime('%Y%m%d')
            snapshot_file = self.snapshots_dir / f"snapshot_{snapshot_date}.csv"
            modified_df.to_csv(snapshot_file, index=False)
            
            snapshots.append(modified_df)
            current_df = modified_df
            
            logger.info(f"Created snapshot for day {day + 1}: {len(modified_df)} records")
        
        return snapshots
    
    def generate_change_summary(self, change_log_file: str) -> Dict:
        """Generate a summary of changes from a log file"""
        df = pd.read_csv(change_log_file)
        
        summary = {
            'date': df['DATE'].iloc[0] if not df.empty else '',
            'total_changes': len(df),
            'new_incorporations': len(df[df['CHANGE_TYPE'] == 'NEW_INCORPORATION']),
            'deregistrations': len(df[df['CHANGE_TYPE'] == 'DEREGISTRATION']),
            'field_updates': len(df[df['CHANGE_TYPE'] == 'FIELD_UPDATE']),
            'affected_states': df['STATE'].dropna().unique().tolist() if 'STATE' in df.columns else [],
            'field_change_breakdown': {}
        }
        
        if 'FIELD_CHANGED' in df.columns:
            field_updates = df[df['CHANGE_TYPE'] == 'FIELD_UPDATE']
            if not field_updates.empty:
                summary['field_change_breakdown'] = field_updates['FIELD_CHANGED'].value_counts().to_dict()
        
        return summary
    
    def track_consecutive_days(self, num_days: int = 3) -> List[Dict]:
        """Track changes across consecutive days"""
        summaries = []
        snapshot_files = sorted(self.snapshots_dir.glob("snapshot_*.csv"))
        
        if len(snapshot_files) < 2:
            logger.warning("Not enough snapshots for change detection")
            return summaries
        
        for i in range(min(num_days, len(snapshot_files) - 1)):
            old_date = snapshot_files[i].stem.replace('snapshot_', '')
            new_date = snapshot_files[i + 1].stem.replace('snapshot_', '')
            
            old_df = self.load_snapshot(old_date)
            new_df = self.load_snapshot(new_date)
            
            if not old_df.empty and not new_df.empty:
                changes = self.detect_changes(old_df, new_df, new_date)
                csv_file, json_file = self.create_change_log(changes, new_date)
                
                if csv_file:
                    summary = self.generate_change_summary(csv_file)
                    summaries.append(summary)
                    
                    print(f"\n=== Change Summary for {new_date} ===")
                    print(f"New Incorporations: {summary['new_incorporations']}")
                    print(f"Deregistrations: {summary['deregistrations']}")
                    print(f"Field Updates: {summary['field_updates']}")
        
        return summaries

import numpy as np  

def main():
    """Main execution function"""
    detector = ChangeDetector()
    
    processed_dir = Path("data/processed")
    master_file = processed_dir / "current_master.csv"
    
    if master_file.exists():
        master_df = pd.read_csv(master_file, low_memory=False)

        initial_snapshot = detector.snapshots_dir / "snapshot_20240101.csv"
        if not initial_snapshot.exists():
            master_df.to_csv(initial_snapshot, index=False)
        
        print("Simulating daily changes...")
        snapshots = detector.simulate_daily_changes(master_df, num_days=3)
        
        print("\nTracking changes across days...")
        summaries = detector.track_consecutive_days(num_days=3)
        
        if summaries:
            summary_file = detector.logs_dir / "change_summary.json"
            with open(summary_file, 'w') as f:
                json.dump(summaries, f, indent=2, default=str)
            print(f"\nChange tracking complete. Summary saved to: {summary_file}")
    else:
        print("Error: Master dataset not found. Please run data_integration.py first.")

if __name__ == "__main__":
    main()