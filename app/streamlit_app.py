"""
MCA Insights Engine - Main Streamlit Dashboard
Interactive dashboard with search, analytics, and AI chatbot
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ai_insights import AIInsightsEngine
from src.database import DatabaseManager

st.set_page_config(
    page_title="MCA Insights Engine",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    .highlight {
        background-color: #ffe4b5;
        padding: 0.2rem 0.4rem;
        border-radius: 0.2rem;
    }
    </style>
""", unsafe_allow_html=True)

class MCADashboard:
    def __init__(self):
        self.data_dir = Path("data")
        self.processed_dir = self.data_dir / "processed"
        self.change_logs_dir = self.data_dir / "change_logs"
        self.enriched_dir = self.data_dir / "enriched"
        
        self.ai_engine = AIInsightsEngine()
        self.db_manager = DatabaseManager()
        
        self.load_data()
    
    def load_data(self):
        master_file = self.processed_dir / "current_master.csv"
        if master_file.exists():
            self.master_df = pd.read_csv(master_file, low_memory=False)
            if 'DATE_OF_INCORPORATION' in self.master_df.columns:
                self.master_df['DATE_OF_INCORPORATION'] = pd.to_datetime(
                    self.master_df['DATE_OF_INCORPORATION'], errors='coerce'
                )
        else:
            self.master_df = pd.DataFrame()
        
        enriched_file = self.enriched_dir / "current_enriched.csv"
        if enriched_file.exists():
            self.enriched_df = pd.read_csv(enriched_file)
        else:
            self.enriched_df = pd.DataFrame()
        
        change_files = list(self.change_logs_dir.glob("change_log_*.csv"))
        if change_files:
            latest_changes = sorted(change_files)[-1]
            self.changes_df = pd.read_csv(latest_changes)
        else:
            self.changes_df = pd.DataFrame()
    
    def render_header(self):
        st.markdown('<h1 class="main-header">🏢 MCA Insights Engine</h1>', unsafe_allow_html=True)
        st.markdown("---")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Companies",
                f"{len(self.master_df):,}" if not self.master_df.empty else "0"
            )
        
        with col2:
            active_count = len(self.master_df[self.master_df['COMPANY_STATUS'] == 'Active']) if not self.master_df.empty else 0
            st.metric("Active Companies", f"{active_count:,}")
        
        with col3:
            states_count = self.master_df['STATE'].nunique() if not self.master_df.empty else 0
            st.metric("States Covered", states_count)
        
        with col4:
            today_changes = len(self.changes_df) if not self.changes_df.empty else 0
            st.metric("Recent Changes", today_changes)
        
        with col5:
            enriched_count = len(self.enriched_df) if not self.enriched_df.empty else 0
            st.metric("Enriched Companies", enriched_count)
    
    def render_sidebar(self):
        st.sidebar.title("Navigation")
        
        page = st.sidebar.selectbox(
            "Select Page",
            ["Overview", "Company Search", "Change Analytics", "AI Chat", "API Documentation"]
        )
        
        st.sidebar.markdown("---")
        
        st.sidebar.subheader("Global Filters")
        
        if not self.master_df.empty and 'STATE' in self.master_df.columns:
            states = ['All'] + sorted(self.master_df['STATE'].dropna().unique().tolist())
            selected_state = st.sidebar.selectbox("State", states)
        else:
            selected_state = 'All'
        
        if not self.master_df.empty and 'COMPANY_STATUS' in self.master_df.columns:
            statuses = ['All'] + sorted(self.master_df['COMPANY_STATUS'].dropna().unique().tolist())
            selected_status = st.sidebar.selectbox("Company Status", statuses)
        else:
            selected_status = 'All'
        
        if not self.master_df.empty and 'DATE_OF_INCORPORATION' in self.master_df.columns:
            years = self.master_df['DATE_OF_INCORPORATION'].dt.year.dropna()
            if not years.empty:
                year_range = st.sidebar.slider(
                    "Incorporation Year",
                    int(years.min()),
                    int(years.max()),
                    (int(years.min()), int(years.max()))
                )
            else:
                year_range = (2000, 2024)
        else:
            year_range = (2000, 2024)
        
        return page, selected_state, selected_status, year_range
    
    def apply_filters(self, df, filters):
        state, status, year_range = filters
        filtered_df = df.copy()
        
        if state != 'All':
            filtered_df = filtered_df[filtered_df['STATE'] == state]
        
        if status != 'All':
            filtered_df = filtered_df[filtered_df['COMPANY_STATUS'] == status]
        
        if 'DATE_OF_INCORPORATION' in filtered_df.columns:
            filtered_df = filtered_df[
                (filtered_df['DATE_OF_INCORPORATION'].dt.year >= year_range[0]) &
                (filtered_df['DATE_OF_INCORPORATION'].dt.year <= year_range[1])
            ]
        
        return filtered_df
    
    def render_overview(self, filters):
        st.header("📊 Dashboard Overview")
        
        if self.master_df.empty:
            st.warning("No data available. Please run data integration first.")
            return
        
        filtered_df = self.apply_filters(self.master_df, filters)
        
        col1, col2 = st.columns(2)
        
        with col1:
            state_dist = filtered_df['STATE'].value_counts().head(10)
            fig = px.bar(
                x=state_dist.values,
                y=state_dist.index,
                orientation='h',
                title="Companies by State",
                labels={'x': 'Count', 'y': 'State'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            status_dist = filtered_df['COMPANY_STATUS'].value_counts()
            fig = px.pie(
                values=status_dist.values,
                names=status_dist.index,
                title="Company Status Distribution"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        if 'DATE_OF_INCORPORATION' in filtered_df.columns:
            yearly_inc = filtered_df.groupby(
                filtered_df['DATE_OF_INCORPORATION'].dt.year
            ).size().reset_index(name='count')
            
            fig = px.line(
                yearly_inc,
                x='DATE_OF_INCORPORATION',
                y='count',
                title="Company Incorporations Over Time",
                labels={'DATE_OF_INCORPORATION': 'Year', 'count': 'Number of Incorporations'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        if not self.changes_df.empty:
            st.subheader("📝 Recent Changes")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                new_inc = len(self.changes_df[self.changes_df['CHANGE_TYPE'] == 'NEW_INCORPORATION'])
                st.info(f"**New Incorporations:** {new_inc}")
            
            with col2:
                dereg = len(self.changes_df[self.changes_df['CHANGE_TYPE'] == 'DEREGISTRATION'])
                st.warning(f"**Deregistrations:** {dereg}")
            
            with col3:
                updates = len(self.changes_df[self.changes_df['CHANGE_TYPE'] == 'FIELD_UPDATE'])
                st.success(f"**Field Updates:** {updates}")
            
            st.dataframe(
                self.changes_df.head(10),
                use_container_width=True
            )
    
    def search_companies(self, query):
        query_upper = query.upper()
        
        cin_match = self.master_df[self.master_df['CIN'].str.contains(query_upper, na=False)]
        name_match = self.master_df[self.master_df['COMPANY_NAME'].str.contains(query_upper, na=False)]
        
        results = pd.concat([cin_match, name_match]).drop_duplicates()
        return results
    
    def render_search(self, filters):
        st.header("🔍 Company Search")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            search_query = st.text_input(
                "Search by CIN or Company Name",
                placeholder="Enter CIN (e.g., U74900MH2009PTC123456) or Company Name"
            )
        
        with col2:
            search_button = st.button("Search", type="primary", use_container_width=True)
        
        if search_button and search_query:
            results = self.search_companies(search_query)
            
            if not results.empty:
                st.success(f"Found {len(results)} matching companies")
                
                for idx, company in results.iterrows():
                    with st.expander(f"{company['COMPANY_NAME']} ({company['CIN']})"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**CIN:** {company['CIN']}")
                            st.write(f"**Status:** {company['COMPANY_STATUS']}")
                            st.write(f"**State:** {company.get('STATE', 'N/A')}")
                            st.write(f"**Incorporation Date:** {company.get('DATE_OF_INCORPORATION', 'N/A')}")
                        
                        with col2:
                            st.write(f"**Authorized Capital:** ₹{company.get('AUTHORIZED_CAPITAL', 0):,.0f}")
                            st.write(f"**Paid-up Capital:** ₹{company.get('PAIDUP_CAPITAL', 0):,.0f}")
                            st.write(f"**ROC:** {company.get('ROC_CODE', 'N/A')}")
                        
                        if not self.enriched_df.empty and company['CIN'] in self.enriched_df['CIN'].values:
                            enriched = self.enriched_df[self.enriched_df['CIN'] == company['CIN']].iloc[0]
                            st.markdown("### 📊 Enriched Information")
                            st.write(f"**Industry:** {enriched.get('INDUSTRY', 'N/A')}")
                            st.write(f"**Sector:** {enriched.get('SECTOR', 'N/A')}")
                            if 'DIRECTORS' in enriched and enriched['DIRECTORS']:
                                st.write(f"**Directors:** {enriched['DIRECTORS']}")
            else:
                st.warning("No companies found matching your search criteria")
    
    def render_analytics(self, filters):
        st.header("📈 Change Analytics")
        
        if self.changes_df.empty:
            st.warning("No change data available. Please run change detection first.")
            return
        
        change_dist = self.changes_df['CHANGE_TYPE'].value_counts()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                x=change_dist.index,
                y=change_dist.values,
                title="Changes by Type",
                labels={'x': 'Change Type', 'y': 'Count'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'FIELD_CHANGED' in self.changes_df.columns:
                field_changes = self.changes_df[
                    self.changes_df['CHANGE_TYPE'] == 'FIELD_UPDATE'
                ]['FIELD_CHANGED'].value_counts()
                
                fig = px.pie(
                    values=field_changes.values,
                    names=field_changes.index,
                    title="Field Updates Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Recent Changes Timeline")
        
        change_files = sorted(self.change_logs_dir.glob("change_log_*.csv"))
        if change_files:
            timeline_data = []
            for file in change_files[-5:]:
                df = pd.read_csv(file)
                date = file.stem.replace('change_log_', '')
                timeline_data.append({
                    'Date': date,
                    'New Incorporations': len(df[df['CHANGE_TYPE'] == 'NEW_INCORPORATION']),
                    'Deregistrations': len(df[df['CHANGE_TYPE'] == 'DEREGISTRATION']),
                    'Field Updates': len(df[df['CHANGE_TYPE'] == 'FIELD_UPDATE'])
                })
            
            timeline_df = pd.DataFrame(timeline_data)
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=timeline_df['Date'], y=timeline_df['New Incorporations'],
                                     mode='lines+markers', name='New Incorporations'))
            fig.add_trace(go.Scatter(x=timeline_df['Date'], y=timeline_df['Deregistrations'],
                                     mode='lines+markers', name='Deregistrations'))
            fig.add_trace(go.Scatter(x=timeline_df['Date'], y=timeline_df['Field Updates'],
                                     mode='lines+markers', name='Field Updates'))
            
            fig.update_layout(title='Change Timeline', xaxis_title='Date', yaxis_title='Count')
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Latest Changes")
        st.dataframe(self.changes_df.head(20), use_container_width=True)
    
    def render_chat(self, filters):
        st.header("💬 AI Chat Assistant")
        
        st.markdown("""
        Ask questions about the MCA data in natural language. Examples:
        - "Show new incorporations in Maharashtra"
        - "How many companies were struck off last month?"
        - "List companies with authorized capital above Rs.10 lakh"
        """)
        
        if "messages" not in st.session_state:
            st.session_state.messages = []
        
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
        
        if prompt := st.chat_input("Ask about MCA data..."):
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            
            with st.chat_message("assistant"):
                response = self.ai_engine.chat_query(prompt, self.master_df, self.changes_df)
                st.markdown(response)
            
            st.session_state.messages.append({"role": "assistant", "content": response})
    
    def render_api_docs(self):
        st.header("📚 API Documentation")
        
        st.markdown("""
        ### REST API Endpoints
        
        #### 1. Search Company
        ```
        GET /api/search_company
        Parameters:
            - query (string): CIN or company name
            - state (optional): Filter by state
            - status (optional): Filter by status
        ```
        
        #### 2. Get Company Details
        ```
        GET /api/company/<cin>
        Returns: Complete company information including enriched data
        ```
        
        #### 3. Get Recent Changes
        ```
        GET /api/changes
        Parameters:
            - date (optional): Specific date (YYYYMMDD)
            - type (optional): Change type filter
        ```
        
        #### 4. Get Statistics
        ```
        GET /api/stats
        Returns: Overall statistics and metrics
        ```
        
        ### Example Usage
        
        **Python:**
        ```python
        import requests
        
        response = requests.get('http://localhost:5000/api/search_company',
                               params={'query': 'U74900MH2009PTC123456'})
        data = response.json()
        ```
        
        **cURL:**
        ```bash
        curl "http://localhost:5000/api/search_company?query=INFOSYS"
        ```
        """)
    
    def run(self):
        self.render_header()
        page, state, status, year_range = self.render_sidebar()
        filters = (state, status, year_range)
        
        if page == "Overview":
            self.render_overview(filters)
        elif page == "Company Search":
            self.render_search(filters)
        elif page == "Change Analytics":
            self.render_analytics(filters)
        elif page == "AI Chat":
            self.render_chat(filters)
        elif page == "API Documentation":
            self.render_api_docs()

def main():
    dashboard = MCADashboard()
    dashboard.run()

if __name__ == "__main__":
    main()