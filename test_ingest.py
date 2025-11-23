import pytest
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'ingest'))

def test_ingest_script_structure():
    
    try:
        from load_data import main, create_indexes, load_collection
        assert callable(main)
        assert callable(create_indexes)
        assert callable(load_collection)
    except ImportError:
        pytest.skip("Ingestion script not available for testing")

def test_csv_urls_accessible():
    import requests
    base_url = "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/samples/"
    test_files = ["books.csv", "ratings.csv", "tags.csv", "book_tags.csv", "to_read.csv"]
    
    for file in test_files:
        response = requests.head(base_url + file)
        assert response.status_code == 200, f"CSV file {file} is not accessible"