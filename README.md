## ETL Bank For Machine Learning Workflow
The ETL data pipeline for extracting the raw data from the database,transform it and load it into the AI lakehouse,hopsworks.


### ETL Phases:

#### Step 1: Extraction
Extract the raw data from the database with the use of SQLAlchemy and Pandas. 
Use dotenv and os libraries in Python to get the credentials and coonnect to the database for querying.
After extracting the data,save it in the data folder in a csv format.

The extraction file:

src/extract.py

Execution: python src/extract.py

---

### Step 2:Transformation:
Here is a phase for checking missing values,performing necessary imputation where possible.
The transformation methods and operations are all performed over the extracted data
In this case,we add datetime and unique user ids as such these are the only transformations performed here.

The transformation file:

src/transform.py

Execution: python src/transform.py

---

### Step 3: Loading:
After transforming the data and ensuring that it is clean,we load it into hopsworks which is the AI Lakehouse.

The loading file location:

src/loading.py

Execution: python src/loading.py

---

### Data Pipeline:
For the ETL pipeline that combines extraction,transformation and loading tasks ,we use prefect as the main orchestration framework for the workflow.
We separate the phases into smaller components called tasks and the order of the tasks for successful pipeline execution is called flow in prefect.

As such the pipeline file:
src/pipeline.py

Execution: python src/pipeline.py
