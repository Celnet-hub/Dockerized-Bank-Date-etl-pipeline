import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# reads .env file
load_dotenv()


def extract(url, table_attributes):
    headers = {
        'User-Agent': 'MyWebScraper/1.0'
    }

    time.sleep(10)

    response = requests.get(url, headers=headers).text
    parsed_webpage = BeautifulSoup(response, 'html.parser')

    # create an empty dataframe to hold data
    df = pd.DataFrame(columns=table_attributes)

    tables = parsed_webpage.find_all("tbody")
    largest_banks_2025 = tables[0].find_all("tr")

    for rows in largest_banks_2025:
        # get columns
        columns = rows.find_all('td')
        # print(columns)

        # handle empty columns and get 'td' contents
        if len(columns) != 0:
            bank_name = columns[1].find_all('a')[1]['title']
            total_assets = columns[2].contents[0]

            dict_data = {
                "Banks" : bank_name,
                "Assets_Billions_USD_2025" : total_assets
            }

            df1 = pd.DataFrame(dict_data, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

            # df.to_csv('extracted.csv')
    
    return df

def load_to_staging(df):
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME")
    table_name = "largest_banks_2025"


    # connection string
    db_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # create SQLAlchemy engine
    engine = create_engine(db_string)

    df.to_sql(
        name=table_name,
        con = engine,
        if_exists = "replace",
        index=False,
        method='multi'
    )

    print(f"Data successfully written to the '{table_name}' table.")




    
    





table_attributes = ["Banks", "Assets_Billions_USD_2025"]
url = "https://en.wikipedia.org/wiki/List_of_largest_banks"

extracted_data = extract(url, table_attributes)

load_to_staging(extracted_data)
