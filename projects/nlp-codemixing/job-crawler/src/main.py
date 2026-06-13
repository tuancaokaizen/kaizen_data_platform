import sys
import os

# Add projects/nlp-codemixing to system path to import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.db_connector import get_db_connection
from utils.text_cleaner import clean_text

def run_crawler():
    print("Starting crawler job...")
    raw_text = "   <div>Hello, this is a code-mixed Vietnamese-English text block.</div>   "
    cleaned = clean_text(raw_text)
    
    print(f"Cleaned Text: {cleaned}")
    try:
        conn = get_db_connection(config_path="config.ini")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawled_jobs (
                id SERIAL PRIMARY KEY,
                content TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("INSERT INTO crawled_jobs (content) VALUES (%s)", (cleaned,))
        conn.commit()
        cursor.close()
        conn.close()
        print("Crawler job completed successfully and stored data in PostgreSQL.")
    except Exception as e:
        print(f"Crawler job warning/error: {e}. Outputting locally instead.")

if __name__ == "__main__":
    run_crawler()
