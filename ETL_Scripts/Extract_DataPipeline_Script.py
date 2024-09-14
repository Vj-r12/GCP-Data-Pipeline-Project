# IMPORTING LIBRARIES.

from google.cloud import bigquery

# PIPELINE 1 -->> 

def Load_Raw_Data_To_Bigquery_From_Source():
    try:
        # CREDENTIALS.
        bucket_name = 'myntra-datapipeline-project-storage-bucket'
        file_name = 'Myntra_dataset_F.csv'
        project_id = 'myntra-datapipeline-project'
        uri = f'gs://{bucket_name}/{file_name}'
        dataset_id = 'myntra_dataset_raw'
        table_id = 'myntra_raw_dataset'

        client = bigquery.Client(project=project_id, location='us-central1')


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
    

# Runing the pipeline function
Load_Raw_Data_To_Bigquery_From_Source()