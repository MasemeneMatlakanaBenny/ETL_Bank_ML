
import pandas as pd
import numpy as np
from datetime import datetime

## load the data first:
df=pd.read_csv("data/raw_data.csv")


## transform the dataset:
df["subscribe"]=df["subscribe"].replace(["yes","no"],[1,0])
df["contact"]=df["contact"].fillna("no_phone")
df["education"]=df["education"].fillna("no_education")


## create the unique ids:
arr_index=np.arange(1,len(df)+1)
current_time = datetime.now()

df['user_id']=arr_index

df['datetime']=current_time

## save the transformed dataset:
df.to_csv("data/transformed_df.csv",index=False)

