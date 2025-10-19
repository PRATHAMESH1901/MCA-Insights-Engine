"""
AI Insights Engine
Provides AI-powered chat interface and automated summary generation
"""

import pandas as pd
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AIInsightsEngine:
    def __init__(self):
        self.summaries_dir = Path("data/summaries")
        self.summaries_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_daily_summary(self, changes_df: pd.DataFrame, date_str: str = None) -> str:
        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')
        
        if changes_df.empty:
            return "No changes recorded for this date."
        
        new_inc = len(changes_df[changes_df['CHANGE_TYPE'] == 'NEW_INCORPORATION'])
        dereg = len(changes_df[changes_df['CHANGE_TYPE'] == 'DEREGISTRATION'])
        updates = len(changes_df[changes_df['CHANGE_TYPE'] == 'FIELD_UPDATE'])
        
        summary = f"""
=== MCA Daily Change Summary ===
Date: {date_str}

 Overall Statistics:
- New Incorporations: {new_inc}
- Deregistrations: {dereg}
- Field Updates: {updates}
- Total Changes: {len(changes_df)}

"""
        
        if 'STATE' in changes_df.columns:
            state_dist = changes_df['STATE'].value_counts().head(5)
            summary += "\n Top States by Changes:\n"
            for state, count in state_dist.items():
                summary += f"  - {state}: {count}\n"
        
        if 'FIELD_CHANGED' in changes_df.columns:
            field_updates = changes_df[changes_df['CHANGE_TYPE'] == 'FIELD_UPDATE']
            if not field_updates.empty:
                field_dist = field_updates['FIELD_CHANGED'].value_counts().head(5)
                summary += "\n Most Updated Fields:\n"
                for field, count in field_dist.items():
                    summary += f"  - {field}: {count}\n"
        
        summary += f"\n Summary generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        summary_file = self.summaries_dir / f"daily_summary_{date_str}.txt"
        with open(summary_file, 'w') as f:
            f.write(summary)
        
        json_summary = {
            'date': date_str,
            'new_incorporations': int(new_inc),
            'deregistrations': int(dereg),
            'field_updates': int(updates),
            'total_changes': len(changes_df),
            'generated_at': datetime.now().isoformat()
        }
        
        json_file = self.summaries_dir / f"daily_summary_{date_str}.json"
        with open(json_file, 'w') as f:
            json.dump(json_summary, f, indent=2)
        
        logger.info(f"Daily summary generated: {summary_file}")
        return summary
    
    def chat_query(self, query: str, master_df: pd.DataFrame, changes_df: pd.DataFrame) -> str:
        query_lower = query.lower()
        
        if "new incorporation" in query_lower:
            return self._handle_new_incorporations(query_lower, changes_df)
        
        elif "struck off" in query_lower or "deregister" in query_lower:
            return self._handle_deregistrations(query_lower, changes_df)
        
        elif "maharashtra" in query_lower or "gujarat" in query_lower or "delhi" in query_lower or "tamil nadu" in query_lower or "karnataka" in query_lower:
            return self._handle_state_query(query_lower, master_df, changes_df)
        
        elif "capital" in query_lower and ("above" in query_lower or "greater" in query_lower or "more than" in query_lower):
            return self._handle_capital_query(query_lower, master_df)
        
        elif "manufacturing" in query_lower or "sector" in query_lower or "industry" in query_lower:
            return self._handle_sector_query(query_lower, master_df)
        
        elif "total" in query_lower or "how many" in query_lower or "count" in query_lower:
            return self._handle_count_query(query_lower, master_df, changes_df)
        
        elif "status" in query_lower:
            return self._handle_status_query(query_lower, master_df)
        
        else:
            return self._generate_generic_response(master_df, changes_df)
    
    def _handle_new_incorporations(self, query: str, changes_df: pd.DataFrame) -> str:
        if changes_df.empty:
            return "No change data available."
        
        new_inc = changes_df[changes_df['CHANGE_TYPE'] == 'NEW_INCORPORATION']
        
        state = None
        for s in ['maharashtra', 'gujarat', 'delhi', 'tamil nadu', 'karnataka']:
            if s in query:
                state = s.title()
                break
        
        if state and 'STATE' in new_inc.columns:
            new_inc = new_inc[new_inc['STATE'].str.lower() == state.lower()]
        
        if new_inc.empty:
            return f"No new incorporations found{f' in {state}' if state else ''}."
        
        response = f"Found **{len(new_inc)}** new incorporations{f' in {state}' if state else ''}.\n\n"
        response += "**Recent incorporations:**\n"
        
        for idx, row in new_inc.head(10).iterrows():
            response += f"- {row['COMPANY_NAME']} (CIN: {row['CIN']})\n"
        
        if len(new_inc) > 10:
            response += f"\n... and {len(new_inc) - 10} more"
        
        return response
    
    def _handle_deregistrations(self, query: str, changes_df: pd.DataFrame) -> str:
        if changes_df.empty:
            return "No change data available."
        
        dereg = changes_df[changes_df['CHANGE_TYPE'] == 'DEREGISTRATION']
        
        if "last month" in query:
            response = f"Based on available data, **{len(dereg)}** companies were deregistered.\n\n"
        else:
            response = f"Found **{len(dereg)}** deregistrations.\n\n"
        
        if not dereg.empty:
            response += "**Recently deregistered companies:**\n"
            for idx, row in dereg.head(10).iterrows():
                response += f"- {row['COMPANY_NAME']} (CIN: {row['CIN']})\n"
        
        return response
    
    def _handle_state_query(self, query: str, master_df: pd.DataFrame, changes_df: pd.DataFrame) -> str:
        state_map = {
            'maharashtra': 'Maharashtra',
            'gujarat': 'Gujarat',
            'delhi': 'Delhi',
            'tamil nadu': 'Tamil Nadu',
            'karnataka': 'Karnataka'
        }
        
        state = None
        for key, value in state_map.items():
            if key in query:
                state = value
                break
        
        if not state:
            return "Please specify a valid state."
        
        if "new" in query or "incorporation" in query:
            new_inc = changes_df[
                (changes_df['CHANGE_TYPE'] == 'NEW_INCORPORATION') &
                (changes_df['STATE'] == state)
            ]
            
            response = f"**New Incorporations in {state}:** {len(new_inc)}\n\n"
            
            if not new_inc.empty:
                response += "**Recent companies:**\n"
                for idx, row in new_inc.head(10).iterrows():
                    response += f"- {row['COMPANY_NAME']}\n"
            
            return response
        
        else:
            state_companies = master_df[master_df['STATE'] == state]
            response = f"**Companies in {state}:**\n"
            response += f"- Total: {len(state_companies)}\n"
            
            if 'COMPANY_STATUS' in state_companies.columns:
                status_dist = state_companies['COMPANY_STATUS'].value_counts()
                response += "\n**By Status:**\n"
                for status, count in status_dist.head(5).items():
                    response += f"- {status}: {count}\n"
            
            return response
    
    def _handle_capital_query(self, query: str, master_df: pd.DataFrame) -> str:
        capital_match = re.search(r'(\d+)\s*(lakh|crore)?', query)
        
        if capital_match:
            amount = int(capital_match.group(1))
            unit = capital_match.group(2) if capital_match.group(2) else 'lakh'
            
            if unit == 'lakh':
                amount_inr = amount * 100000
            elif unit == 'crore':
                amount_inr = amount * 10000000
            else:
                amount_inr = amount
            
            filtered = master_df[master_df['AUTHORIZED_CAPITAL'] > amount_inr]
            
            if "manufacturing" in query:
                filtered = filtered[
                    filtered['PRINCIPAL_BUSINESS_ACTIVITY'].str.contains('MANUFACTURING', na=False, case=False)
                ]
            
            response = f"Found **{len(filtered)}** companies with authorized capital above ₹{amount} {unit}.\n\n"
            
            if not filtered.empty:
                response += "**Sample companies:**\n"
                for idx, row in filtered.head(10).iterrows():
                    capital = row['AUTHORIZED_CAPITAL']
                    response += f"- {row['COMPANY_NAME']}: ₹{capital:,.0f}\n"
            
            return response
        
        return "Please specify an amount (e.g., 'above 10 lakh' or 'above 1 crore')."
    
    def _handle_sector_query(self, query: str, master_df: pd.DataFrame) -> str:
        if "manufacturing" in query:
            sector_companies = master_df[
                master_df['PRINCIPAL_BUSINESS_ACTIVITY'].str.contains('MANUFACTURING', na=False, case=False)
            ]
            sector_name = "Manufacturing"
        else:
            return "Please specify a sector (e.g., manufacturing)."
        
        response = f"**{sector_name} Sector Analysis:**\n"
        response += f"- Total Companies: {len(sector_companies)}\n"
        
        if 'COMPANY_STATUS' in sector_companies.columns:
            active = len(sector_companies[sector_companies['COMPANY_STATUS'] == 'Active'])
            response += f"- Active Companies: {active}\n"
        
        if 'AUTHORIZED_CAPITAL' in sector_companies.columns:
            avg_capital = sector_companies['AUTHORIZED_CAPITAL'].mean()
            response += f"- Average Authorized Capital: ₹{avg_capital:,.0f}\n"
        
        return response
    
    def _handle_count_query(self, query: str, master_df: pd.DataFrame, changes_df: pd.DataFrame) -> str:
        if "total" in query and "companies" in query:
            return f"Total companies in database: **{len(master_df)}**"
        
        elif "active" in query:
            active = len(master_df[master_df['COMPANY_STATUS'] == 'Active'])
            return f"Total active companies: **{active}**"
        
        elif "change" in query:
            return f"Total changes recorded: **{len(changes_df)}**"
        
        return f"Total companies: **{len(master_df)}**"
    
    def _handle_status_query(self, query: str, master_df: pd.DataFrame) -> str:
        if 'COMPANY_STATUS' not in master_df.columns:
            return "Status information not available."
        
        status_dist = master_df['COMPANY_STATUS'].value_counts()
        
        response = "**Company Status Distribution:**\n\n"
        for status, count in status_dist.items():
            percentage = (count / len(master_df)) * 100
            response += f"- {status}: {count} ({percentage:.1f}%)\n"
        
        return response
    
    def _generate_generic_response(self, master_df: pd.DataFrame, changes_df: pd.DataFrame) -> str:
        response = "**MCA Data Overview:**\n\n"
        response += f" Total Companies: {len(master_df)}\n"
        
        if not changes_df.empty:
            response += f" Recent Changes: {len(changes_df)}\n"
        
        if 'STATE' in master_df.columns:
            response += f" States Covered: {master_df['STATE'].nunique()}\n"
        
        response += "\n**You can ask me:**\n"
        response += "- 'Show new incorporations in Maharashtra'\n"
        response += "- 'How many companies were struck off?'\n"
        response += "- 'List companies with capital above 10 lakh'\n"
        response += "- 'What is the total number of active companies?'\n"
        
        return response
    
    def batch_generate_summaries(self, change_logs_dir: Path) -> List[str]:
        summaries = []
        change_files = sorted(change_logs_dir.glob("change_log_*.csv"))
        
        for file in change_files:
            date_str = file.stem.replace('change_log_', '')
            changes_df = pd.read_csv(file)
            summary = self.generate_daily_summary(changes_df, date_str)
            summaries.append(summary)
            logger.info(f"Generated summary for {date_str}")
        
        return summaries

def main():
    ai_engine = AIInsightsEngine()
    
    change_logs_dir = Path("data/change_logs")
    if change_logs_dir.exists():
        print("Generating AI summaries for all change logs...")
        summaries = ai_engine.batch_generate_summaries(change_logs_dir)
        print(f"\nGenerated {len(summaries)} summaries")
        
        if summaries:
            print("\n" + "="*50)
            print("Latest Summary:")
            print("="*50)
            print(summaries[-1])
    else:
        print("No change logs found. Please run change detection first.")

if __name__ == "__main__":
    main()