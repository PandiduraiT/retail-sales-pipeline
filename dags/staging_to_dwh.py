from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd

def load_dim_table(staging_table, dim_table, dim_columns, unique_columns=None, **context):
    """
    Load deduplicated data from staging table into dimension table.
    """
    hook = PostgresHook(postgres_conn_id='postgres_retail_sales')
    engine = hook.get_sqlalchemy_engine()
    
    # Load data from staging
    df = pd.read_sql(f"SELECT {', '.join(dim_columns)} FROM {staging_table}", con=engine)

    # Deduplicate
    if unique_columns:
        df = df.drop_duplicates(subset=unique_columns)

    # Append to dimension table
    df.to_sql(
        name=dim_table,
        con=engine,
        if_exists='append',
        index=False,
        method='multi'
    )
    print(f"Inserted {len(df)} rows into {dim_table}.")

def load_sales_fact(**context):
    """
    Merge data from staging tables and insert into Sales_Fact table.
    Joins dimension tables using attributes from stg_* tables, not invoice_id.
    """
    hook = PostgresHook(postgres_conn_id='postgres_retail_sales')
    engine = hook.get_sqlalchemy_engine()

    query = """
    INSERT INTO Sales_Fact (
        invoice_id, branch_id, customer_id, product_id, date_id, time_id, payment_id,
        quantity, tax, total, cogs, gross_income, gross_margin_percentage
    )
    SELECT
        s.invoice_id,
        b.branch_id,
        c.customer_id,
        p.product_id,
        d.date_id,
        t.time_id,
        pm.payment_id,
        s.quantity,
        s.tax,
        s.total,
        s.cogs,
        s.gross_income,
        s.gross_margin_percentage
    FROM stg_sales s
    JOIN stg_branch sb ON s.invoice_id = sb.invoice_id
    JOIN Branch_Dim b ON sb.branch_name = b.branch_name AND sb.city = b.city
    JOIN stg_customer sc ON s.invoice_id = sc.invoice_id
    JOIN Customer_Dim c ON sc.customer_type = c.customer_type AND sc.gender = c.gender
    JOIN stg_product sp ON s.invoice_id = sp.invoice_id
    JOIN Product_Dim p ON sp.product_line = p.product_line
    JOIN stg_date sd ON s.invoice_id = sd.invoice_id
    JOIN Date_Dim d ON sd.full_date = d.full_date
    JOIN stg_time st ON s.invoice_id = st.invoice_id
    JOIN Time_Dim t ON st.time::time = t.time
    JOIN stg_payment spm ON s.invoice_id = spm.invoice_id
    JOIN Payment_Dim pm ON spm.payment_method = pm.payment_method;
    """

    with engine.begin() as conn:
        conn.execute(query)
    print("✅ Loaded data into Sales_Fact table successfully.")


with DAG(
    "staging_to_dwh",
    start_date=datetime(2025, 5, 25),
    schedule_interval=None,
    catchup=False,
    description="Load data from staging tables into data warehouse fact and dimension tables"
) as dag:

    # Create data warehouse tables
    create_dwh_tables = PostgresOperator(
        task_id='create_dwh_tables',
        postgres_conn_id='postgres_retail_sales',
        sql='sql/dwh/create_dwh_tables.sql'
    )

    # Dimension loading tasks
    load_branch_dim = PythonOperator(
        task_id='load_branch_dim',
        python_callable=load_dim_table,
        op_kwargs={
            'staging_table': 'stg_branch',
            'dim_table': 'Branch_Dim',
            'dim_columns': ['branch_name', 'city'],
            'unique_columns': ['branch_name', 'city']
        }
    )

    load_customer_dim = PythonOperator(
        task_id='load_customer_dim',
        python_callable=load_dim_table,
        op_kwargs={
            'staging_table': 'stg_customer',
            'dim_table': 'Customer_Dim',
            'dim_columns': ['customer_type', 'gender'],
            'unique_columns': ['customer_type', 'gender']
        }
    )

    load_product_dim = PythonOperator(
        task_id='load_product_dim',
        python_callable=load_dim_table,
        op_kwargs={
            'staging_table': 'stg_product',
            'dim_table': 'Product_Dim',
            'dim_columns': ['product_line', 'unit_price'],
            'unique_columns': ['product_line']
        }
    )

    load_date_dim = PythonOperator(
        task_id='load_date_dim',
        python_callable=load_dim_table,
        op_kwargs={
            'staging_table': 'stg_date',
            'dim_table': 'Date_Dim',
           'dim_columns': ['full_date', 'day', 'month', 'year', 'weekday'],
            'unique_columns': ['full_date']
        }
    )

    load_time_dim = PythonOperator(
        task_id='load_time_dim',
        python_callable=load_dim_table,
        op_kwargs={
            'staging_table': 'stg_time',
            'dim_table': 'Time_Dim',
            'dim_columns': ['time', 'hour', 'minute', 'am_pm', 'time_bucket'],
            'unique_columns': ['time']
        }
    )

    load_payment_dim = PythonOperator(
        task_id='load_payment_dim',
        python_callable=load_dim_table,
        op_kwargs={
            'staging_table': 'stg_payment',
            'dim_table': 'Payment_Dim',
            'dim_columns': ['payment_method'],
            'unique_columns': ['payment_method']
        }
    )

    # Fact table loading
    load_fact = PythonOperator(
        task_id='load_sales_fact',
        python_callable=load_sales_fact
    )

    # Set DAG dependencies
    create_dwh_tables >> [
        load_branch_dim, load_customer_dim, load_product_dim,
        load_date_dim, load_time_dim, load_payment_dim
    ] >> load_fact
