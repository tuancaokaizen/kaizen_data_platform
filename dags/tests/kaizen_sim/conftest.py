import os
import sys

# Import jobs.* the same way the Airflow DAGs do / Import jobs.* giống cách DAG Airflow làm
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
