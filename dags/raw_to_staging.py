
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os


def extract_raw_sales_data(**context):
    """
    Extract all data from the single raw_sales_data table
    
    Returns:
        str: Path to the extracted data file
    """
    hook = PostgresHook(postgres_conn_id='postgres_retail_sales')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Get column names first
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'raw_sales_data' 
            ORDER BY ordinal_position;
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        # Extract data in batches
        cursor.execute("SELECT * FROM raw_sales_data;")
        all_data = []
        
        while True:
            batch = cursor.fetchmany(size=1000)
            if not batch:
                break
            else:
                all_data.extend(batch)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data, columns=columns)
        
        # Save to temporary location
        temp_file_path = "/tmp/raw_sales_data_extracted.parquet"
        df.to_parquet(temp_file_path, index=False)
        
        # Push data path to XCom for downstream tasks
        context['task_instance'].xcom_push(key='raw_data_path', value=temp_file_path)
        
        print(f"Extracted {len(df)} rows from raw_sales_data")
        return temp_file_path
        
    finally:
        cursor.close()
        conn.close()


def load_to_staging_table(staging_table_name, **context):
    """
    Load specific data from raw table to individual staging tables
    
    Args:
        staging_table_name (str): Name of the staging table to load into
        **context: Airflow context variables
    """
    # Get the raw data path from upstream task
    raw_data_path = context['task_instance'].xcom_pull(
        task_ids='extract_raw_data', 
        key='raw_data_path'
    )
    
    if not raw_data_path or not os.path.exists(raw_data_path):
        raise ValueError("Raw data file not found")
    
    # Load the raw data
    df = pd.read_parquet(raw_data_path)
    
    # Transform/filter data based on staging table
    transformed_df = transform_for_staging_table(df, staging_table_name)
    
    # Load to staging table
    hook = PostgresHook(postgres_conn_id='postgres_retail_sales')
    engine = hook.get_sqlalchemy_engine()
    
    transformed_df.to_sql(
        name=staging_table_name,
        con=engine,
        if_exists='replace',  # or 'append' based on your needs
        index=False
    )
    
    print(f"Loaded {len(transformed_df)} rows to {staging_table_name}")


def transform_for_staging_table(df, staging_table_name):
    """
    Apply transformations specific to each staging table
    
    Args:
        df (pd.DataFrame): Raw sales data
        staging_table_name (str): Target staging table name
    
    Returns:
        pd.DataFrame: Transformed data for the specific staging table
    """
    
    if staging_table_name == 'stg_branch':
        transformed_df = df[['invoice_id', 'branch', 'city']].copy()
        transformed_df.columns = ['invoice_id', 'branch_name', 'city']
        transformed_df['branch_name'] = transformed_df['branch_name'].str.title().str.strip()
        transformed_df['city'] = transformed_df['city'].str.title().str.strip()
        return transformed_df
    
    elif staging_table_name == 'stg_customer':
        transformed_df = df[['invoice_id', 'customer_type', 'gender', 'rating']].copy()
        transformed_df['customer_type'] = transformed_df['customer_type'].str.title().str.strip()
        transformed_df['gender'] = transformed_df['gender'].str.title().str.strip()
        return transformed_df
    
    elif staging_table_name == 'stg_date':
        transformed_df = df[['invoice_id', 'date']].copy()
        transformed_df['full_date'] = pd.to_datetime(transformed_df['date'], errors='coerce')
        transformed_df['day'] = transformed_df['full_date'].dt.day
        transformed_df['month'] = transformed_df['full_date'].dt.month
        transformed_df['year'] = transformed_df['full_date'].dt.year
        transformed_df['weekday'] = transformed_df['full_date'].dt.day_name()
        transformed_df = transformed_df.drop(columns=['date'])
        return transformed_df

    elif staging_table_name == 'stg_time':
        transformed_df = df[['invoice_id', 'time']].copy()
        transformed_df['time'] = pd.to_datetime(transformed_df['time'], format='%H:%M:%S', errors='coerce').dt.time
        time_dt = pd.to_datetime(transformed_df['time'].astype(str), format='%H:%M:%S', errors='coerce')
        transformed_df['hour'] = time_dt.dt.hour
        transformed_df['minute'] = time_dt.dt.minute
        transformed_df['am_pm'] = time_dt.dt.strftime('%p')
     
        def time_bucket(hour):
            if pd.isna(hour): 
                return 'Unknown'
            if 6 <= hour < 12: 
                return 'Morning'
            elif 12 <= hour < 17: 
                return 'Afternoon'
            elif 17 <= hour < 21: 
                return 'Evening'
            else: 
                return 'Night'
        
        transformed_df['time_bucket'] = transformed_df['hour'].apply(time_bucket)
        transformed_df = transformed_df.dropna(subset=['time'])
        return transformed_df

    elif staging_table_name == 'stg_payment':
        transformed_df = df[['invoice_id', 'payment']].copy()
        transformed_df.columns = ['invoice_id', 'payment_method']
        transformed_df['payment_method'] = transformed_df['payment_method'].str.title().str.strip()
        return transformed_df

    elif staging_table_name == 'stg_sales':
        transformed_df = df[['invoice_id', 'quantity', 'tax_5_percent', 'sales', 'cogs', 'gross_income', 'gross_margin_percentage']].copy()
        transformed_df['quantity'] = pd.to_numeric(transformed_df['quantity'], errors='coerce')
        transformed_df['tax'] = pd.to_numeric(transformed_df['tax_5_percent'], errors='coerce')
        transformed_df['total'] = pd.to_numeric(transformed_df['sales'], errors='coerce')
        transformed_df['cogs'] = pd.to_numeric(transformed_df['cogs'], errors='coerce')
        transformed_df['gross_income'] = pd.to_numeric(transformed_df['gross_income'], errors='coerce')
        transformed_df['gross_margin_percentage'] = pd.to_numeric(transformed_df['gross_margin_percentage'], errors='coerce')
        transformed_df = transformed_df.drop(columns=['tax_5_percent'])
        return transformed_df

    elif staging_table_name == 'stg_product':
        transformed_df = df[['invoice_id', 'product_line', 'unit_price']].copy()
        transformed_df['product_line'] = transformed_df['product_line'].str.title().str.strip()
        transformed_df['unit_price'] = pd.to_numeric(transformed_df['unit_price'], errors='coerce')
        return transformed_df
   
    else:
        # Default: return raw data
        return df.copy()


def cleanup_temp_files(**context):
    """Clean up temporary files after processing"""
    raw_data_path = context['task_instance'].xcom_pull(
        task_ids='extract_raw_data', 
        key='raw_data_path'
    )
    
    if raw_data_path and os.path.exists(raw_data_path):
        os.remove(raw_data_path)
        print("Cleaned up temporary files")


# Configuration: Define all staging tables to create
STAGING_TABLES = [
    'stg_branch',
    'stg_customer', 
    'stg_date',
    'stg_time',
    'stg_payment',
    'stg_product',
    'stg_sales'
]


with DAG(
    "raw_to_multiple_staging",
    start_date=datetime(2025, 5, 25),
    schedule_interval=None,
    catchup=False,
    description="Extract from single raw table and load to multiple staging tables"
) as dag:

    # Create staging tables
    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='postgres_retail_sales',
        sql='sql/staging/create_staging_tables.sql'
    )
    
    # Extract raw data once
    extract_raw_data = PythonOperator(
        task_id='extract_raw_data',
        python_callable=extract_raw_sales_data
    )
    
    # Create loading tasks for each staging table
    staging_tasks = []
    for staging_table in STAGING_TABLES:
        staging_task = PythonOperator(
            task_id=f'load_{staging_table}',
            python_callable=load_to_staging_table,
            op_kwargs={'staging_table_name': staging_table}
        )
        staging_tasks.append(staging_task)
    
    cleanup = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_files,
        trigger_rule='all_done'  
    )
    

    create_staging_tables >> extract_raw_data >> staging_tasks >> cleanup