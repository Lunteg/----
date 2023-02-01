from typing import Any, Dict, List

import pandas as pd
from prefect import flow, task, Flow
from urllib.parse import urlparse


# ---------------------------------------------------------------------------- #
#                                 Create tasks                                 #
# ---------------------------------------------------------------------------- #
@task
def load_file_to_db(input: str) -> pd.DataFrame:
    return pd.read_csv(input)

@task
def sql_create_table_as(df : pd.DataFrame) -> pd.DataFrame:
    df['domain_of_url'] = df['url'].apply(lambda x: urlparse(x).netloc)
    return df

@task
def copyToFile(df : pd.DataFrame) -> None:
    df.to_csv('output', sep=',')
    
@flow
def flow_caso():
    data = load_file_to_db(input="test.csv")
    table = sql_create_table_as(data)
    copyToFile(table)
    
    
flow_caso()
    
    