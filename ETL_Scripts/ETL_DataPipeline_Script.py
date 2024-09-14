# IMPORTING LIBRARIES.
from google.cloud import bigquery
import pandas_gbq 
import pandas as pd
import db_dtypes
import tqdm

# PIPELINE 2 -->> EXTRACT , TRANSFORM , LOAD.

# INTIALIZING BIGQUERY CLIENT.
client = bigquery.Client(
    project='myntra-datapipeline-project',
    location='us-central1'
)

# EXTRACTING RAW DATA FROM BIGQUERY TABLE.

def Extract_Data_From_Bigquery_Table():
    try:
        table_name = 'myntra_raw_dataset'
        project_id = 'myntra-datapipeline-project'
        dataset_id = 'myntra_dataset_raw'

        query = f"""select * from `{project_id}.{dataset_id}.{table_name}` limit 100""" # TOTAL RECORDS ARE 1,65,080.

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
    Project_Id = 'myntra-datapipeline-project'
    new_dataset = f'{Project_Id}.myntra_transformed_dataset'
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
        pandas_gbq.to_gbq(df, Transformed_Data_Table_Name, project_id=Project_Id)
    except Exception as error:
        print('Oops there is an error in loading data.', error)
    else:
        print("Sucessfully Cretead Transformed Data Table & Loaded Data.")

Loading_Transformed_Data_Into_Table()