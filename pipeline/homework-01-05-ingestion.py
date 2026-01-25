#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# Read a sample of the data
# prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
prefix = '../../datasets/homework-01/'
# df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[2]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[3]:


# display first rows
df.head()


# In[4]:


# check data types
df.dtypes


# In[5]:


# check data shape
df.shape


# In[6]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
# head(n=0) maks sure we only create the table, we don't add any data yet.


# ## Ingesting the Data In Chunks
# We don't want to insert all the data at once. Let's do it in batches and use an iterator for that:

# In[14]:


df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# ### Iterate Over Chunks

# In[8]:


for df_chunk in df_iter:
    print(len(df_chunk))


# ### Inserting Data

# In[9]:


df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# ### Complete Ingestion Loop

# In[15]:


# N.B. you'll need to re-run the cell which defines the df_iter before running this cell!
first_chunk = next(df_iter)

first_chunk.head(0).to_sql(
    name="yellow_taxi_data",
    con=engine,
    if_exists="replace"
)

print("Table created")

first_chunk.to_sql(
    name="yellow_taxi_data",
    con=engine,
    if_exists="append"
)

print("Inserted first chunk:", len(first_chunk))

for df_chunk in df_iter:
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append"
    )
    print("Inserted chunk:", len(df_chunk))

