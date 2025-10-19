echo "========================================="
echo "MCA Insights Engine - Setup Script"
echo "========================================="

echo -e "\n Step 1: Creating project structure..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p data/snapshots
mkdir -p data/enriched
mkdir -p data/change_logs
mkdir -p data/summaries
mkdir -p src
mkdir -p app/pages
mkdir -p config
mkdir -p tests
mkdir -p logs

echo -e "\n Directories created successfully!"

echo -e "\n Step 2: Creating virtual environment..."
python3 -m venv venv

echo -e "\n Virtual environment created!"

echo -e "\n Step 3: Activating virtual environment..."
source venv/bin/activate

echo -e "\n Virtual environment activated!"

echo -e "\n Step 4: Installing requirements..."
pip install --upgrade pip
pip install pandas numpy streamlit plotly requests beautifulsoup4 \
    selenium sqlalchemy python-dotenv pyyaml faker schedule \
    flask flask-restful lxml openpyxl

echo -e "\n All packages installed!"

echo -e "\n Step 5: Creating configuration files..."

cat > .env << 'EOF'
OPENAI_API_KEY=your_openai_api_key_here
MCA_API_KEY=your_mca_api_key_if_available
DATABASE_URL=sqlite:///data/mca_master.db
EOF

cat > config/config.yaml << 'EOF'
database:
  path: "data/mca_master.db"
  
data_sources:
  states:
    - Maharashtra
    - Gujarat
    - Delhi
    - Tamil Nadu
    - Karnataka
  
enrichment:
  sources:
    - name: "ZaubaCorp"
      url: "https://www.zaubacorp.com/company/"
    - name: "API Setu"
      url: "https://apisetu.gov.in/api/"
      
ai:
  model: "gpt-3.5-turbo"
  temperature: 0.7
  
app:
  port: 8501
  debug: true
EOF

echo -e "\n Configuration files created!"

echo -e "\n Step 6: Creating __init__.py files..."
touch src/__init__.py
touch app/__init__.py
touch app/pages/__init__.py
touch tests/__init__.py

echo -e "\n __init__.py files created!"

echo -e "\n Setup complete!"
echo -e "\n Next steps:"
echo "1. Place your CSV files in data/raw/ directory"
echo "2. Run: python src/data_integration.py"
echo "3. Run: python src/change_detection.py"
echo "4. Run: python src/web_enrichment.py"
echo "5. Run: python src/ai_insights.py"
echo "6. Run: python src/database.py"
echo "7. Start dashboard: streamlit run app/streamlit_app.py"
echo "8. Start API: python app/api.py"
echo -e "\n========================================="