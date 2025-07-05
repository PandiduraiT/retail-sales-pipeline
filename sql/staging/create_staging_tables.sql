CREATE TABLE IF NOT EXISTS stg_branch(
    invoice_id VARCHAR(15),
    branch VARCHAR,
    city VARCHAR
)

CREATE TABLE IF NOT EXISTS stg_customer(
    invoice_id VARCHAR(15),
    customer_type VARCHAR,
    gender VARCHAR(8),
    rating DECIMAL(3,1)
)

CREATE TABLE IF NOT EXISTS stg_date(
    invoice_id VARCHAR(15),
    full_date DATE,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR
)

CREATE TABLE IF NOT EXISTS stg_time (
    invoice_id VARCHAR,
    time TIME,
    hour INTEGER,
    minute INTEGER,
    am_pm VARCHAR,
    time_bucket VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_payment (
    invoice_id VARCHAR,
    payment_method VARCHAR
);

CREATE TABLE IF NOT EXISTS stg_product (
    invoice_id VARCHAR,
    product_line VARCHAR,
    unit_price NUMERIC
);

CREATE TABLE IF NOT EXISTS stg_sales (
    invoice_id VARCHAR,
    quantity INTEGER,
    tax NUMERIC,
    total NUMERIC,
    cogs NUMERIC,
    gross_income NUMERIC,
    gross_margin_percentage NUMERIC
);



