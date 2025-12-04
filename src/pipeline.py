from prefect import task,flow

@task(
  name="data extraction",
  task_name="extraction",
  description="Extract the data from the databases"
)
def data_extraction():
  """
  Extract the messy and raw data from the databases
  """
  import os
  from dotenv import load_dotenv
  
  load_dotenv()
  ##connect to the db:
  
  password = os.getenv("PASSWORD")
  db_name = os.getenv("DB_NAME")
  host = "127.0.0.1"
  port = "3306"
  user = "root"
  
  db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"

  ## write sql query or script
  query = "SELECT * FROM bank_marketing"
  data = pd.read_sql_query(query, engine)
  
  return data
