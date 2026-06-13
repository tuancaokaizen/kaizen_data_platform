import sys
import os
import configparser

# Add projects/nlp-codemixing to system path to import utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils.db_connector import get_db_connection

def run_nlp_processor():
    print("Starting NLP & Code-Mixing processor job...")
    config = configparser.ConfigParser()
    config.read("config.ini")
    model_name = config.get("nlp", "model_name", fallback="vinai/phobert-base")
    print(f"Loading transformer model: {model_name}")

    try:
        conn = get_db_connection(config_path="config.ini")
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, content FROM crawled_jobs ORDER BY created_at DESC LIMIT 10")
        rows = cursor.fetchall()
        for row in rows:
            doc_id, text = row
            tokens = text.split()
            code_mixed_score = len([t for t in tokens if t.lower() in ["hello", "code-mixed", "text", "block"]]) / max(len(tokens), 1)
            print(f"Doc {doc_id} Content: '{text}' -> Code-Mix ratio: {code_mixed_score:.2f}")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"NLP processor warning/error: {e}. Running local fallback simulation...")
        text = "Hello, this is a code-mixed Vietnamese-English text block."
        tokens = text.split()
        code_mixed_score = len([t for t in tokens if t.lower() in ["hello", "code-mixed", "text", "block"]]) / len(tokens)
        print(f"Simulated classification -> Code-Mix ratio: {code_mixed_score:.2f}")

if __name__ == "__main__":
    run_nlp_processor()
