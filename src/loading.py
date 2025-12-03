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


## read the data:
df=pd.read_csv("data/transformed_df.csv",parse_dates=['datetime'])

feature_group.insert(df,write_options={"wait_for_job":False})
