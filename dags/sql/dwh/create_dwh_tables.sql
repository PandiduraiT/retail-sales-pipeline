CREATE TABLE IF NOT EXISTS Branch_Dim (
    branch_id SERIAL PRIMARY KEY,
    branch_name VARCHAR(50),
    city VARCHAR(50)
);


CREATE TABLE IF NOT EXISTS Customer_Dim (
    customer_id SERIAL PRIMARY KEY,
    customer_type VARCHAR(20),
    gender VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS Product_Dim (
    product_id SERIAL PRIMARY KEY,
    product_line VARCHAR(100),
    unit_price NUMERIC
);

CREATE TABLE IF NOT EXISTS Date_Dim (
    date_id SERIAL PRIMARY KEY,
    full_date DATE,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR(15)
);

CREATE TABLE IF NOT EXISTS Time_Dim (
    time_id SERIAL PRIMARY KEY,
    time TIME,
    hour INTEGER,
    minute INTEGER,
    am_pm VARCHAR(5),
    time_bucket VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS Payment_Dim (
    payment_id SERIAL PRIMARY KEY,
    payment_method VARCHAR(20)
);


CREATE TABLE IF NOT EXISTS Sales_Fact (
    invoice_id VARCHAR(20) PRIMARY KEY,
    branch_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    date_id INTEGER,
    time_id INTEGER,
    payment_id INTEGER,
    quantity INTEGER,
    tax NUMERIC,
    total NUMERIC,
    cogs NUMERIC,
    gross_income NUMERIC,
    gross_margin_percentage NUMERIC,
    rating NUMERIC,

    CONSTRAINT fk_branch_id FOREIGN KEY (branch_id) REFERENCES Branch_Dim(branch_id),
    CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES Customer_Dim(customer_id),
    CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES Product_Dim(product_id),
    CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES Date_Dim(date_id),
    CONSTRAINT fk_time_id FOREIGN KEY (time_id) REFERENCES Time_Dim(time_id),
    CONSTRAINT fk_payment_id FOREIGN KEY (payment_id) REFERENCES Payment_Dim(payment_id)
);
