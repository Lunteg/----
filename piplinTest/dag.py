from dagster import asset
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import select
from urllib.parse import urlparse


@asset
def load_file_to_db():
   engine = create_engine('sqlite:///:memory:')
   df = pd.read_csv('data/test.csv')
   return df

@asset
def sql_create_table_as(load_file_to_db):
    load_file_to_db['domain_of_url'] = load_file_to_db['url'].apply(lambda x: urlparse(x).netloc)
    print(load_file_to_db)
    return load_file_to_db
    
@asset
def copyToFile(sql_create_table_as) -> None:
    sql_create_table_as.to_csv('output', sep=',')
    print('Save to output.csv')
