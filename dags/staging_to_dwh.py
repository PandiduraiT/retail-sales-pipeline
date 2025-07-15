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
    
    # Get data from staging table
    records = hook.get_records(f"SELECT {', '.join(dim_columns)} FROM {staging_table}")
    
    if not records:
        print(f"⚠️  No data found in {staging_table}")
        return
    
    # Convert to DataFrame for deduplication
    df = pd.DataFrame(records, columns=dim_columns)
    
    # Deduplicate if unique columns specified
    if unique_columns:
        df = df.drop_duplicates(subset=unique_columns)
    
    if len(df) == 0:
        print(f"⚠️  No data to insert into {dim_table} after deduplication")
        return
    
    # Clear existing data first (for fresh load)
    hook.run(f"DELETE FROM {dim_table}")
    
    # Convert back to list of tuples for insert_rows
    rows = [tuple(row) for row in df.values]
    
    # Insert using PostgresHook - this handles transactions automatically
    hook.insert_rows(
        table=dim_table,
        rows=rows,
        target_fields=dim_columns,
        commit_every=1000
    )
    
    print(f"✅ Successfully inserted {len(rows)} rows into {dim_table}")

def load_sales_fact(**context):
    """
    Load data into Sales_Fact table by joining with dimension tables.
    Uses ON CONFLICT to skip duplicates and continue with other records.
    """
    hook = PostgresHook(postgres_conn_id='postgres_retail_sales')
    
    # Modified query with ON CONFLICT DO NOTHING to skip duplicates
    query = """
    INSERT INTO Sales_Fact (
        invoice_id, branch_id, customer_id, product_id, date_id, time_id, payment_id,
        quantity, tax, total, cogs, gross_income, gross_margin_percentage, rating
    ) SELECT  
        st_s.invoice_id,
        B.branch_id,
        C.customer_id,
        P.product_id,
        D.date_id,
        T.time_id,
        Pt.payment_id,
        st_s.quantity,
        st_s.tax,
        st_s.total,
        st_s.cogs,
        st_s.gross_income,
        st_s.gross_margin_percentage,
        stg_c.rating::numeric
    FROM stg_sales AS st_s                   
    JOIN stg_branch AS stg_b ON st_s.invoice_id = stg_b.invoice_id
    JOIN Branch_Dim AS B ON stg_b.branch_name = B.branch_name AND stg_b.city = B.city
    JOIN stg_customer AS stg_c ON st_s.invoice_id = stg_c.invoice_id
    JOIN Customer_Dim AS C ON stg_c.customer_type = C.customer_type
    JOIN stg_product AS stg_p ON st_s.invoice_id = stg_p.invoice_id
    JOIN Product_Dim AS P ON stg_p.product_line = P.product_line
    JOIN stg_date AS stg_d ON st_s.invoice_id = stg_d.invoice_id
    JOIN Date_Dim AS D ON stg_d.full_date = D.full_date
    JOIN stg_time AS stg_t ON st_s.invoice_id = stg_t.invoice_id
    JOIN Time_Dim AS T ON stg_t.time::TIME = T.time::TIME
    JOIN stg_payment AS stg_pt ON st_s.invoice_id = stg_pt.invoice_id
    JOIN Payment_Dim AS Pt ON stg_pt.payment_method = Pt.payment_method
    ON CONFLICT (invoice_id) DO NOTHING;
    """
    
    try:
        # Get count before insert
        count_before = hook.get_first("SELECT COUNT(*) FROM Sales_Fact")
        before_count = count_before[0] if count_before else 0
        
        # Execute the query
        hook.run(query)
        
        # Get count after insert
        count_after = hook.get_first("SELECT COUNT(*) FROM Sales_Fact")
        after_count = count_after[0] if count_after else 0
        
        inserted_count = after_count - before_count
        
        print(f"Successfully inserted {inserted_count} new rows into Sales_Fact")
        print(f"Sales_Fact table now has {after_count} total rows")
        
        if inserted_count == 0:
            print("No new rows were inserted (all records already exist)")
        
    except Exception as e:
        print(f"Error loading Sales_Fact: {str(e)}")
        raise

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
    create_dwh_tables  >> [
        load_branch_dim, load_customer_dim, load_product_dim,
        load_date_dim, load_time_dim, load_payment_dim
    ] >> load_fact