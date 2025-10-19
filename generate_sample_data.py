"""
Sample MCA Data Generator
Creates realistic sample CSV files for testing
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import random

class SampleDataGenerator:
    def __init__(self):
        self.states = {
            'Maharashtra': 'MH',
            'Gujarat': 'GJ',
            'Delhi': 'DL',
            'Tamil Nadu': 'TN',
            'Karnataka': 'KA'
        }
        
        self.company_types = ['PTC', 'PLC', 'OPC']
        
        self.company_classes = ['Private', 'Public', 'One Person Company']
        
        self.company_categories = [
            'Company limited by shares',
            'Company limited by guarantee',
            'Unlimited Company'
        ]
        
        self.company_statuses = [
            'Active',
            'Strike Off',
            'Amalgamated',
            'Under Liquidation',
            'Dissolved'
        ]
        
        self.business_activities = [
            'MANUFACTURING (FOOD PRODUCTS)',
            'MANUFACTURING (TEXTILES)',
            'MANUFACTURING (CHEMICALS)',
            'WHOLESALE TRADE',
            'RETAIL TRADE',
            'COMPUTER PROGRAMMING',
            'INFORMATION SERVICE ACTIVITIES',
            'FINANCIAL SERVICE ACTIVITIES',
            'REAL ESTATE ACTIVITIES',
            'PROFESSIONAL, SCIENTIFIC AND TECHNICAL ACTIVITIES',
            'ADMINISTRATIVE AND SUPPORT SERVICE ACTIVITIES',
            'EDUCATION',
            'HUMAN HEALTH AND SOCIAL WORK',
            'CONSTRUCTION',
            'ACCOMMODATION AND FOOD SERVICE'
        ]
        
        self.company_name_suffixes = [
            'PRIVATE LIMITED',
            'LIMITED',
            'OPC PRIVATE LIMITED'
        ]
        
        self.company_name_words = [
            'TECH', 'SOLUTIONS', 'INDUSTRIES', 'ENTERPRISES', 'SYSTEMS',
            'GLOBAL', 'INTERNATIONAL', 'INDIA', 'SERVICES', 'TRADING',
            'INNOVATIONS', 'VENTURES', 'CORPORATIONS', 'TECHNOLOGIES',
            'CONSULTANCY', 'EXPORTS', 'IMPORTS', 'MANUFACTURING',
            'DEVELOPERS', 'CONSTRUCTIONS', 'FOODS', 'TEXTILES',
            'CHEMICALS', 'PHARMA', 'HEALTHCARE', 'EDUCATION',
            'FINANCE', 'CAPITAL', 'INVESTMENTS', 'REALTY'
        ]
    
    def generate_cin(self, state_code, year, company_type):
        listing = random.choice(['U', 'L'])
        industry_code = random.randint(10000, 99999)
        sequence = random.randint(100000, 999999)
        
        return f"{listing}{industry_code}{state_code}{year}{company_type}{sequence}"
    
    def generate_company_name(self):
        num_words = random.randint(1, 3)
        name_words = random.sample(self.company_name_words, num_words)
        suffix = random.choice(self.company_name_suffixes)
        
        return ' '.join(name_words) + ' ' + suffix
    
    def generate_date(self, start_year=2000, end_year=2024):
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
        
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        
        return start_date + timedelta(days=random_days)
    
    def generate_address(self, state):
        buildings = ['A-101', 'B-202', 'C-303', 'D-404', 'E-505']
        streets = ['MG Road', 'Park Street', 'Anna Salai', 'Brigade Road', 'Link Road']
        areas = ['Andheri', 'Borivali', 'Whitefield', 'Adyar', 'Satellite']
        
        building = random.choice(buildings)
        street = random.choice(streets)
        area = random.choice(areas)
        pincode = random.randint(400001, 560099)
        
        return f"{building}, {street}, {area}, {state} - {pincode}"
    
    def generate_roc_code(self, state):
        roc_map = {
            'Maharashtra': 'ROC-MUMBAI',
            'Gujarat': 'ROC-AHMEDABAD',
            'Delhi': 'ROC-DELHI',
            'Tamil Nadu': 'ROC-CHENNAI',
            'Karnataka': 'ROC-BANGALORE'
        }
        return roc_map.get(state, 'ROC-MUMBAI')
    
    def generate_state_data(self, state, num_companies=1000):
        state_code = self.states[state]
        companies = []
        
        print(f"Generating {num_companies} companies for {state}...")
        
        for i in range(num_companies):
            year = random.randint(2000, 2024)
            company_type = random.choice(self.company_types)
            
            company = {
                'CORPORATE_IDENTIFICATION_NUMBER': self.generate_cin(state_code, year, company_type),
                'COMPANY_NAME': self.generate_company_name(),
                'COMPANY_CLASS': random.choice(self.company_classes),
                'COMPANY_CATEGORY': random.choice(self.company_categories),
                'COMPANY_SUB_CATEGORY': random.choice(['Non-govt company', 'State Govt company']),
                'DATE_OF_REGISTRATION': self.generate_date(year, year).strftime('%d/%m/%Y'),
                'AUTHORIZED_CAPITAL': random.randint(100000, 50000000),
                'PAIDUP_CAPITAL': random.randint(50000, 25000000),
                'COMPANY_STATUS': random.choice(self.company_statuses) if random.random() > 0.1 else 'Active',
                'PRINCIPAL_BUSINESS_ACTIVITY': random.choice(self.business_activities),
                'REGISTERED_OFFICE_ADDRESS': self.generate_address(state),
                'ROC_CODE': self.generate_roc_code(state),
            }
            
            companies.append(company)
        
        df = pd.DataFrame(companies)
        return df
    
    def generate_all_states(self, num_companies_per_state=1000):
        raw_dir = Path("data/raw")
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        for state in self.states.keys():
            df = self.generate_state_data(state, num_companies_per_state)
            
            filename = raw_dir / f"company_master_{state.lower().replace(' ', '_')}.csv"
            df.to_csv(filename, index=False)
            
            print(f" Created: {filename} ({len(df)} records)")
        
        print(f"\n Sample data generation complete!")
        print(f"Total files created: {len(self.states)}")
        print(f"Total companies: {num_companies_per_state * len(self.states)}")

def main():
    print("="*60)
    print("MCA Sample Data Generator")
    print("="*60)
    print()
    
    generator = SampleDataGenerator()
    
    num_companies = input("Enter number of companies per state (default 1000): ").strip()
    num_companies = int(num_companies) if num_companies else 1000
    
    print(f"\nGenerating {num_companies} companies per state...")
    print("This may take a minute...\n")
    
    generator.generate_all_states(num_companies)
    
    print("\n Files created in: data/raw/")

if __name__ == "__main__":
    main()