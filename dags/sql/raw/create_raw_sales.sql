CREATE TABLE IF NOT EXISTS raw_sales_data (
    invoice_id VARCHAR(15),
    branch VARCHAR,
    city VARCHAR,
    customer_type VARCHAR,
    gender VARCHAR,
    product_line VARCHAR,
    unit_price NUMERIC,
    quantity INTEGER,
    tax_5_percent NUMERIC,
    sales NUMERIC,
    date DATE,
    time TIME,
    payment VARCHAR,
    cogs NUMERIC,
    gross_margin_percentage NUMERIC,
    gross_income NUMERIC,
    rating NUMERIC
);
