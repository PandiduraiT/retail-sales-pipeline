from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os


def load_csv_to_postgres():
    folder_path = 'data'
    colnames = [
        "invoice_id",
        "branch",
        "city",
        "customer_type",
        "gender",
        "product_line",
        "unit_price",
        "quantity",
        "tax_5_percent",
        "sales",
        "date",
        "time",
        "payment",
        "cogs",
        "gross_margin_percentage",
        "gross_income",
        "rating"
    ]

    hook = PostgresHook(postgres_conn_id='postgres_retail_sales')
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Load CSV files into Postgres
    file_names = os.listdir(folder_path)
    for fname in file_names:
        if fname.endswith('.csv'):
            file_path = os.path.join(folder_path, fname)
            # Read CSV, using first row as header, applying our column names
            df = pd.read_csv(file_path, names=colnames, header=0)

            # Load to Postgres
            df.to_sql('raw_sales_data', engine, if_exists='append', index=False)
            print(f"Loaded {fname} into raw_sales_data table.")

    # Query and print some rows from the table
    cursor.execute("SELECT * FROM raw_sales_data LIMIT 5;")
    rows = cursor.fetchall()
    print("\nSample rows from raw_sales_data table:")
    for row in rows:
        print(row)


with DAG(
    "source_to_raw",
    start_date=datetime(2025, 5, 25),
    schedule_interval=None,
    catchup=False
) as dag:

    create_raw_data_table = PostgresOperator(
        task_id='create_raw_data_table',
        postgres_conn_id='postgres_retail_sales',
        sql='sql/raw/create_raw_sales.sql'
        )
 
    load_csv_raw_table = PythonOperator(
        task_id='load_csv_to_raw_table',
        python_callable=load_csv_to_postgres
    )

    create_raw_data_table >> load_csv_raw_table
