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


@task(
  name="transformation",
  task_name="data_transformation",
  description="Handling nulls and making sure that the data is clean and consistent"
)
def data_transformation(messy_data:pd.DataFrame)->pd.DataFrame:
  """
  Transform the messy data and make it ready for any data workflow or consumering
  """
  import pandas as pd
  import numpy as np
  from datetime import datetime

  ## copy the messy data:
  df=messy_data.copy()
  ## transform the data
  df["subscribe"]=df["subscribe"].replace(["yes","no"],[1,0])
  df["contact"]=df["contact"].fillna("no_phone")
  df["education"]=df["education"].fillna("no_education")


  ## add the primary key value and the datetime -> for loading purposes
  arr_index=np.arange(1,len(df)+1)
  current_time = datetime.now()
  
  df['user_id']=arr_index
  df['datetime']=current_time

  
  return df

@task(
  name="load_data",
  task_name="load_data",
  description="Load the data into the desired final destination
)
def load_data(transformed_data:pd.DataFrame):
  """
  Extract the data 
     ***
     ***
     ***
Transform the data
     ***
     ***
     ***
Load the data  into final destination
"""
  import hopsworks
  import os
  import pandas as pd
  from dotenv import load_dotenv


  load_dotenv()


  project_name=os.getenv("PROJECT_NAME")

  api_key=os.getenv("API_KEY")

  project=hopsworks.login(
    project=project_name,
    api_key_value=api_key
)


  ## create the feature store:
  feature_store=project.get_feature_store()

  ## create the feature group:
  feature_group_name="bank_marketing_group"

  feature_group_version=1


  feature_group_description="The bank marketing feature group"


  ## create the feature group:
  feature_group=feature_store.get_or_create_feature_group(
    name=feature_group_name,
    version=feature_group_version,
    description=feature_group_description,
    primary_key=['user_id'],
    event_time=['datetime']
)



  feature_group.insert(df,write_options={"wait_for_job":False})
