from gcloud import get_project_id
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class BQ:
    def __init__(self, project_id, logger = None):
        self.project_id = project_id
        self.logger = logger
        self.client = bigquery.Client(project=project_id)
            
    def list_datasets(self):        
        datasets = list(self.client.list_datasets())
        
        if datasets:
            if self.logger: self.logger.info(f"Datasets in project {self.project_id}:")
            for dataset in datasets:
                if self.logger: self.logger.info(dataset.dataset_id)
            return [dataset.dataset_id for dataset in datasets]
        else:
            if self.logger: self.logger.info(f"No datasets found in project {self.project_id}.")
            return []

    def create_dataset(self, dataset_id):
        dataset_ref = self.client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)

        # Create the dataset
        dataset = self.client.create_dataset(dataset, timeout=30)  # API request
        if self.logger: self.logger.info(f"Created dataset {self.client.project}.{dataset.dataset_id}")
        
    def delete_dataset(self, dataset_id, delete_contents=False):
        dataset_ref = self.client.dataset(dataset_id)

        # Delete the dataset
        try:
            self.client.delete_dataset(
                dataset_ref, delete_contents=delete_contents, not_found_ok=True
            )  # API request
            if self.logger: self.logger.info(f"Deleted dataset {self.project_id}.{dataset_id}")
        except NotFound:
            if self.logger: self.logger.info(f"Dataset {self.project_id}.{dataset_id} not found.")

    def list_tables(self, dataset_id):
        dataset_ref = self.client.dataset(dataset_id)
        # List all tables in the dataset
        tables = list(self.client.list_tables(dataset_ref))  # Convert iterator to list
        if self.logger: self.logger.info(f"Tables in dataset {dataset_id}:")
        for table in tables:
            if self.logger: self.logger.info(table.table_id)
        return [table.table_id for table in tables]

    def create_table(self, dataset_id, table_id, schema):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        table = self.client.create_table(table)
        if self.logger: self.logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        
    def remove_table(self, dataset_id, table_id):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Delete the table
        try:
            self.client.delete_table(table_ref)  # API request
            if self.logger: self.logger.info(f"Deleted table {self.project_id}.{dataset_id}.{table_id}")
        except NotFound:
            if self.logger: self.logger.info(f"Table {self.project_id}.{dataset_id}.{table_id} not found.")
            
    def insert_rows(self, dataset_id, table_id, rows_to_insert):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        errors = self.client.insert_rows_json(table_ref, rows_to_insert)
        if errors == []:
            if self.logger: self.logger.info(f"New rows have been added to {dataset_id}.{table_id}")
        else:
            if self.logger: self.logger.info(f"Encountered errors while inserting rows: {errors}")
            
    def insert_dataframe(self, dataset_id, table_id, dataframe):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job = self.client.load_table_from_dataframe(dataframe, table_ref)
        job.result()  # Wait for the job to complete.
        if self.logger: self.logger.info(f"DataFrame has been added to {dataset_id}.{table_id}")
        
    def query_table(self, query):
        query_job = self.client.query(query)
        results = query_job.result()  # Wait for the job to complete.
        return results.to_dataframe()  # Convert the result to a DataFrame

    def get_table_schema(self, dataset_id, table_id):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        schema = table.schema
        for field in schema:
            if self.logger: self.logger.info(f"Field name: {field.name}, Field type: {field.field_type}")
        return schema

if __name__ == "__main__":
    # Replace with your project ID
    project_id = get_project_id()
    bq = BQ(project_id)
    datasets = bq.list_datasets()
    for dataset in datasets:
        tables = bq.list_tables(dataset)