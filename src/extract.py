import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# Load credentials:
# Load credentials
password = os.getenv("PASSWORD")
db_name = os.getenv("DB_NAME")
host = "127.0.0.1"
port = "3306"
user = "root"

# Define the connection string
db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"


try:
    print(db_name)
    engine = create_engine(db_url)
    ## create the query:
    query = "SELECT * FROM bank_marketing"

    # This line triggers the connection attempt
    data = pd.read_sql_query(query, engine)

    # If connection succeeds:
    data.to_csv("data/raw_data.csv", index=False)

    
except Exception as e:
    print(f"Connection Failed! Error: {e}")


