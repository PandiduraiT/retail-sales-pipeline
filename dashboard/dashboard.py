import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Connect to warehouse database
engine = create_engine("postgresql+psycopg2://user_airflow:psswd_airflow@postgres:5432/retail_sales")

st.title("Retail Sales Dashboard")

# 1) Total sales per branch
st.header("1️⃣ Total Sales per Branch")
query1 = """
SELECT bd.branch_name, SUM(sf.total) AS total_sales
FROM Sales_Fact sf
JOIN Branch_Dim bd ON sf.branch_id = bd.branch_id
GROUP BY bd.branch_name
ORDER BY total_sales DESC;
"""
df1 = pd.read_sql(query1, engine)
st.bar_chart(df1.set_index("branch_name"))

# 2) Average rating per customer type (fixed query)
st.header("2️⃣ Average Customer Rating by Type")
query2 = """
SELECT cd.customer_type, AVG(sf.rating) AS avg_rating
FROM Sales_Fact sf
JOIN Customer_Dim cd ON sf.customer_id = cd.customer_id
GROUP BY cd.customer_type;
"""
df2 = pd.read_sql(query2, engine)
st.bar_chart(df2.set_index("customer_type"))

# 3) Top 5 product lines by sales amount
st.header("3️⃣ Top 5 Product Lines")
query3 = """
SELECT pd.product_line, SUM(sf.total) AS total_sales
FROM Sales_Fact sf
JOIN Product_Dim pd ON sf.product_id = pd.product_id
GROUP BY pd.product_line
ORDER BY total_sales DESC
LIMIT 5;
"""
df3 = pd.read_sql(query3, engine)
st.bar_chart(df3.set_index("product_line"))

# 4) Popular payment methods
st.header("4️⃣ Popular Payment Methods")
query4 = """
SELECT pm.payment_method, COUNT(*) AS num_transactions
FROM Sales_Fact sf
JOIN Payment_Dim pm ON sf.payment_id = pm.payment_id
GROUP BY pm.payment_method
ORDER BY num_transactions DESC;
"""
df4 = pd.read_sql(query4, engine)
st.bar_chart(df4.set_index("payment_method"))

# 5) Daily sales trend
st.header("5️⃣ Daily Sales Trend")
query5 = """
SELECT dd.full_date, SUM(sf.total) AS total_sales
FROM Sales_Fact sf
JOIN Date_Dim dd ON sf.date_id = dd.date_id
GROUP BY dd.full_date
ORDER BY dd.full_date;
"""
df5 = pd.read_sql(query5, engine)
st.line_chart(df5.set_index("full_date"))
