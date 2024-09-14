# IMPORTING LIBRARIES.

from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime
from airflow import DAG
import pandas as pd
import pandas_gbq 
import db_dtypes
import tqdm

# CREDENTIALS.

bucket_name = 'myntra-datapipeline-project-storage-bucket'
file_name = 'Myntra_dataset_F.csv'
project_id = 'myntra-datapipeline-project'
uri = f'gs://{bucket_name}/{file_name}'
dataset_id = 'myntra_dataset_raw'
table_id = 'myntra_raw_dataset'

client = bigquery.Client(project=project_id, location='us-central1')

# PIPELINE 1 : EXTRACTS CSV FILE FROM GCS BUCKET & LOAD INTO GCP BIGQUERY TABLE.

def Load_Raw_Data_To_Bigquery_From_Source():
    try:
        # CHECKING DATASET IF EXISTS ELSE CREATING NEW DATASET.
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
            print('Dataset already exists')
        except:
            # CREATING DATASET.
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "us-central1"
            dataset = client.create_dataset(dataset)
            print(f'{dataset_id} created..')

        # DEFINING TABLE REF.
        table_ref = dataset_ref.table(table_id)

        # DEFING SCHEMA FOR TABLE.
        schema = [
            bigquery.SchemaField("product_id", "STRING"),
            bigquery.SchemaField("product_name", "STRING"),
            bigquery.SchemaField("brand_id", "STRING"),
            bigquery.SchemaField("brand_name", "STRING"),
            bigquery.SchemaField("rating", "FLOAT64"),
            bigquery.SchemaField("rating_count", "INT64"),
            bigquery.SchemaField("marked_price", 'INT64'),
            bigquery.SchemaField("discounted_price", "INT64"),
            bigquery.SchemaField("product_link", "STRING"),
            bigquery.SchemaField('img_link', "STRING"),
            bigquery.SchemaField("product_tag", "STRING"),
            bigquery.SchemaField("brand_tag", "STRING"),
            bigquery.SchemaField("discount_amount", "INT64"),
            bigquery.SchemaField("discount_percent", "INT64")
        ]

        # CREATING TABLE.
        try:
            client.get_table(table_ref)
            print('Table already exists')
        except:
            table = bigquery.Table(table_ref=table_ref, schema=schema)
            table = client.create_table(table)
            print(f"Created {table_id} table.")

        # CONFIGURATION FOR LOADING DATA INTO TABLE.
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            allow_quoted_newlines=True
        )

        # LOADING CSV FILE INTO BIGQUERY TABLE FROM BUCKET.
        load_job = client.load_table_from_uri(
            uri, table_ref, job_config=job_config
        )

        # LOADING DATA.
        load_job.result()

    except Exception as error:
        print('Oops there is an error occured', error)
    else:
        print('Successfully Data loaded into bigquery table.')
    

# PIPELINE 2 -->> EXTRACT , TRANSFORM , LOAD.

# EXTRACTING RAW DATA FROM BIGQUERY TABLE.

def Extract_Data_From_Bigquery_Table():
    try:
        query = f"""select * from `{project_id}.{dataset_id}.{table_id}` limit 100""" # TOTAL RECORDS ARE 1,65,080.

        df = pandas_gbq.read_gbq(query, project_id=project_id)
    
    except Exception as error:
        print("Oops there is an error occured in extraction process", error)
    else:
         print("Extraction process completed.")
    
    return df
         
# TRANSFORMING DATA.

def Transform_Data():
    try:
        # DERIVING DATA FROM Extract_Data_From_Bigquery_Table
        df = Extract_Data_From_Bigquery_Table()

        # TRANSFORMING COLUMN NAMES TO TITLE.
        df.columns = df.columns.str.title()

        # FETCHING COLUMNS.
        df = df[['Product_Id','Product_Name','Brand_Id','Brand_Name','Rating','Rating_Count','Marked_Price','Discounted_Price','Product_Tag','Brand_Tag','Discount_Amount','Discount_Percent']]

        # TRANSFORMATION PIPE.
        def Product_Transformation_Pipe(df):
                df['Product_Name'] = df['Product_Name'].str.strip()
                df['Product_Tag'] = df['Product_Tag'].str.strip()
                df['Product_Tag'] = df['Product_Tag'].str.replace('-', ' ', regex=False)
                return df

        def Brand_Transformation_Pipe(df):
                df['Brand_Name'] = df['Brand_Name'].str.strip()
                df['Brand_Tag'] = df['Brand_Tag'].str.strip()
                df['Brand_Tag'] = df['Brand_Tag'].str.replace('-', ' ', regex=False)  
                return df 

        # CONVERTING INTO INTEGER.
        df['Marked_Price'] = df['Marked_Price'].astype(int)
        df['Discounted_Price'] = df['Discounted_Price'].astype(int)
        df['Discount_Amount'] = df['Discount_Amount'].astype(int)
        df['Discount_Percent'] = df['Discount_Percent'].astype(int)

        # APPLYING TEXT TRANSFORMATION PIPE.
        df = df.pipe(Product_Transformation_Pipe)
        df = df.pipe(Brand_Transformation_Pipe)
 
    except Exception as error:
        print("Oops there is an error occured in transformation process", error)
    else:
        print("Completed transformation process.")

    return df


# LOADING TRANSFORMED DATA INTO BIGQUERY TABLE.

def Loading_Transformed_Data_Into_Table():
    df = Transform_Data()
    new_dataset = f'{project_id}.myntra_transformed_dataset'
    # CHECKING DATASET IF EXISTS ELSE CREATING DATASET.
    try:
        client.get_dataset(new_dataset)
        print(f'{new_dataset} already exists.')
    except:
         dataset = bigquery.Dataset(new_dataset)
         dataset.location = "us-central1"
         dataset = client.create_dataset(dataset)
         print(f"{new_dataset} created.")
         
         
    try:
        Transformed_Data_Table_Name = 'myntra_transformed_dataset.Transformed_Data'
        pandas_gbq.to_gbq(df, Transformed_Data_Table_Name, project_id=project_id)
    except Exception as error:
        print('Oops there is an error in loading data.', error)
    else:
        print("Sucessfully Cretead Transformed Data Table & Loaded Data.")


# CREATING DAG FOR PIPELINE 1. EXTRACTS CSV FILE FROM GCS BUCKET & LOAD INTO GCP BIGQUERY TABLE.    

# DEFINEING DEAFULT ARGUMENTS FOR PIPELINE 1.

deafult_args_pipeline1 = {
    'owner' : 'gcp_datapipeline_project',
    'start_date': datetime(2024, 9, 14),
    'retries' : 1, 
}

# CREATEING DAG.

with DAG(
    dag_id = 'LOADING_CSV_FILE_TO_BIQUERY_TABLE_PIPELINE_1',
    default_args = deafult_args_pipeline1,
    schedule_interval = None,
    catchup = False,
) as dag:
    
# DEFINENING PYTHON TASK.

    load_csv_file_to_bigquery_table = PythonOperator(
        task_id = 'LOADING_CSV_FILE_TO_BIQUERY_TABLE',
        python_callable = Load_Raw_Data_To_Bigquery_From_Source,
        op_kwargs = {
            'bucket_name' : bucket_name,
            'source_file_name' : file_name,
            'dataset_id' : dataset_id,
            'table_id' : table_id
        }
    )


# CREATING DAG FOR PIPELINE 2. EXTRACTS -->> TRANSFORM -->> LOAD.    

# DEFINEING DEAFULT ARGUMENTS FOR PIPELINE 2.

deafult_args_pipeline2 = {
    'owner' : 'gcp_datapipeline_project',
    'start_date' : datetime(2024, 9, 14),
    'retries' : 1,
}

# CREATEING DAG.

with DAG(
    dag_id = 'ETL_Datapipeline_Task',
    default_args = deafult_args_pipeline2,
    schedule_interval = None,
    catchup = False,
) as dag:

    # DEFINENING PYTHON TASK.

    # TASK 1.
    Extracts_Data_From_Bigquery_Table = PythonOperator(
        task_id = 'Extracting_Data_From_Table',
        python_callable = Extract_Data_From_Bigquery_Table   
    )

    # TASK 2.
    Transform_Data_Task = PythonOperator(
        task_id = 'Transforming_Data',
        python_callable = Transform_Data
    )

    # TASK 3.
    Load_Transformed_Data = PythonOperator(
        task_id = 'Loading_Transformed_Data_Into_Bigquery_Table',
        python_callable = Loading_Transformed_Data_Into_Table
    )



# SETTING TASK EXECUTION OF PIPELINE 1.
load_csv_file_to_bigquery_table

# SETTING TASK EXECUTION OF PIPELINE 2.
Extracts_Data_From_Bigquery_Table >> Transform_Data_Task >> Load_Transformed_Data