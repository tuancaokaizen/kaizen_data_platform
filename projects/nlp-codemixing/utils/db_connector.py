import psycopg2
import configparser

def get_db_connection(config_path="config.ini"):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    conn = psycopg2.connect(
        host=config.get("postgres", "host", fallback="localhost"),
        port=config.get("postgres", "port", fallback="5432"),
        database=config.get("postgres", "database", fallback="kaizen_db"),
        user=config.get("postgres", "username", fallback="postgres"),
        password=config.get("postgres", "password", fallback="postgres")
    )
    return conn
