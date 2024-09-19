#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install pandas')
get_ipython().system('pip install mysql-connector-python')


# In[2]:


get_ipython().system('pip install mysql')


# In[3]:


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector


# In[4]:


import os


# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'selleres'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('payments.csv', 'payments'),
    ('order_items.csv', 'orders_items')# Added payments.csv for specific handling
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Smit@0101',
    database='target_sales'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'D:/MIS/.UMass/Projects/Target Sales/Dataset'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()


# In[5]:


# Connect to Database
db = mysql.connector.connect(host = 'localhost',
                             username = 'root',
                             password = 'Smit@0101',
                             database = 'target_sales')

cur = db.cursor()


# In[6]:


# List all unique cities where customers are located.
query = """ SELECT DISTINCT customer_city FROM customers """

cur.execute(query)

data = cur.fetchall()

data


# In[7]:


#Count the number of orders placed in 2017.
query = """ SELECT COUNT(*) FROM orders
WHERE YEAR(order_purchase_timestamp) = 2017 """

cur.execute(query)

data = cur.fetchall()

data[0][0]


# In[8]:


#Find the total sales per category
query = """ SELECT ROUND(SUM(payment_value),2), product_category FROM payments py JOIN orders_items o JOIN products pr
            ON py.order_id = o.order_id AND o.product_id = pr.product_id
            GROUP BY product_category;"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data, columns = ["Total Sales", "Category"])
df


# In[9]:


# Calculate the percentage of orders that were paid in installments.
query = """ SELECT (SUM(CASE WHEN payment_installments > 0 THEN 1 ELSE 0 END))/COUNT(*)*100
            FROM payments;"""

cur.execute(query)

data = cur.fetchall()

data


# In[23]:


#Count the number of customers from each state.
query = """ select customer_state ,count(customer_id)
from customers group by customer_state
"""

cur.execute(query)

data = cur.fetchall()

df = pd.DataFrame(data, columns = ["State", "Customer_Count"])
df = df.sort_values(by = 'Customer_Count', ascending = False)

plt.figure(figsize = (9,3))
plt.bar(df["State"], df["Customer_Count"])
plt.xlabel("States")
plt.ylabel("Customer_Count")
plt.title("Count of Cutomers per States")
plt.show()


# In[ ]:


#Calculate the number of orders per month in 2018

