import requests
import pandas as pd
import ast
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
 

pd.options.display.max_columns = None
pd.options.display.max_rows = None

headers = {
    "Authorization": "Token token=ZIWF3mDZY1wTelic8c99hg",
    "Content-Type": "application/json"
}


def get_data(endpoint, include_column) -> pd.DataFrame:
    """_summary_

    Args:
        endpoint: freshsales API endpoint
        include_column (pd.Series): additional required column

    Returns:
        df: output dataframe
    """
    response = requests.get(endpoint, headers=headers)
    if response.status_code == 200:
        result = response.json()[include_column]
        list = [data for data in result]
        df = pd.DataFrame(list) 
    else:
        print(f"Failed to retrieve contacts. Status Code: {response.status_code}, Error: {response.text}")
        df = None
    return df

def post_data(endpoint, data):
    """_summary_
    Args:
        endpoint: freshsales API endpoint
    """
    # json_data = df.to_json()
    print(data)
    response = requests.post(endpoint, data = data, headers=headers)
    print(response.status_code)







def save_local(df, name) -> None:
    """_summary_

    Args:
        df (pd.Dataframe): dataframe to save
        name (str): dataframe name to save
    """
    file_path = os.path.join('results_data/', name)
    df.to_csv(f'{file_path}.csv', index=False)


def read_data(contacts_csv, accounts_csv, deals_csv) -> pd.DataFrame:
    """_summary_

    Args:
        contacts_csv (csv file): contacts raw data
        accounts_csv (csv file): accounts raw data
        deals_csv (csv file): deals raw data

    Returns:
        d1: contacts df
        d2: accounts df
        d3: deals df
    """

    df1 = pd.read_csv(os.path.join('results_data/', contacts_csv))
    df2 = pd.read_csv(os.path.join('results_data/', accounts_csv))
    df3 = pd.read_csv(os.path.join('results_data/', deals_csv))
    return df1, df2, df3


def convert_datatypes(df, column) -> pd.DataFrame:
    """_summary_

    Args:
        df (pd.Dataframe): dataframe to preprocess
        column (pd.Series): column to change dtype

    Returns:
        df: resulting dataframe
    """
    df[column] = df[column].apply(ast.literal_eval)
    return df 


def build_db_conn():
    """_summary_

    Returns:
        engine: SQL alchemy postgres engine
    """
    connection_string = 'postgresql://priya:priya@localhost:5432/sbti'
    engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
    return engine 


def read_exec_sql(dir, file, conn) -> None:
    """_summary_

    Args:
        dir (str): folder of sql queries
        file (str): sql file
        conn (object): connection string of postgres db
    """
    file_path = os.path.join(dir, file)
    sql_file = open(file_path, 'r')
    try:
        conn.execute(text(sql_file.read()))
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)
    sql_file.close()
    


