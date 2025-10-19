echo "========================================="
echo "MCA Insights Engine - Pipeline Execution"
echo "========================================="

source venv/bin/activate

echo -e "\n Step 1: Data Integration..."
python src/data_integration.py
if [ $? -eq 0 ]; then
    echo " Data integration complete!"
else
    echo " Data integration failed!"
    exit 1
fi

echo -e "\n Step 2: Change Detection..."
python src/change_detection.py
if [ $? -eq 0 ]; then
    echo " Change detection complete!"
else
    echo " Change detection failed!"
    exit 1
fi

echo -e "\n Step 3: Web Enrichment..."
python src/web_enrichment.py
if [ $? -eq 0 ]; then
    echo " Web enrichment complete!"
else
    echo " Web enrichment failed!"
    exit 1
fi

echo -e "\n Step 4: AI Insights Generation..."
python src/ai_insights.py
if [ $? -eq 0 ]; then
    echo " AI insights generated!"
else
    echo " AI insights generation failed!"
    exit 1
fi

echo -e "\n Step 5: Database Population..."
python src/database.py
if [ $? -eq 0 ]; then
    echo " Database populated!"
else
    echo " Database population failed!"
    exit 1
fi

echo -e "\n Pipeline execution complete!"
echo -e "\n You can now:"
echo "1. Start dashboard: streamlit run app/streamlit_app.py"
echo "2. Start API: python app/api.py"
echo -e "\n========================================="