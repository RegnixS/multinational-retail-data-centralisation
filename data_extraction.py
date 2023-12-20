# %%
"""Data Extraction Class Module

This module implements a class object for extracting data.
"""
from dotenv import load_dotenv
from database_utils import DatabaseConnector
import boto3
import os
import pandas as pd
import requests
import tabula

load_dotenv()


class DataExtractor:
    """
    This class is used to extract data.
    
    Attributes:
        name (type): Description.
    """
    def __init__(self):
        """
            Initializes DataExtrator.
        """
    def read_rds_table(self, connector, table_name):
        """
        Get a specific table from the database.
        
        Args:
            String : The name of the table to fetch.
        
        Returns:
            Panda Dataframe : Dataframe of table data.
        """
        df = pd.read_sql_table(table_name, connector.engine)
        df.set_index("index", inplace=True)
        return df

    def retrieve_pdf_data(self, url):
        """
        Get a pdf with a table from a url.
        
        Args:
            String : The url of the pdf document.
        
        Returns:
            Panda Dataframe : Dataframe of table data from the pdf.
        """
        dfs = tabula.read_pdf(url, pages='all', stream=True)
        df = dfs[0]
        for i, item in enumerate(dfs):
            if i > 0:
                df = pd.concat((df, dfs[i]), ignore_index=True)

        return df
    
    def list_number_stores(self, end_point, header):
        """
        Get the number of stores from a WebAPI.
        
        Args:
            String : The url of the endpoint.
        
        Returns:
            Integer : The number of stores.
        """
        response = requests.get (url=end_point, headers=header)           

        return response.json()['number_stores']
    
    def retrieve_stores_data(self, end_point, header, no_of_stores):
        """
        Get details of all stores from a WebAPI.
        
        Args:
            String : The url of the endpoint.
            Integer : The total number of stores at the API.

        Returns:
            Panda Dataframe : Dataframe of all store details.
        """
        for i in range(0, no_of_stores):
            response = requests.get (url=end_point + str(i), headers=header)           
            dict_store = response.json()
            if i == 0:
                df_stores = pd.DataFrame(dict_store, index=[i])
            else:
                df_stores = pd.concat((df_stores, pd.DataFrame(dict_store, index=[i])))#, ignore_index=True)

        df_stores.set_index('index', inplace=True)

        return df_stores
      
    def extract_from_s3(self, uri):
        """
        Extracts a csv file from S3.
        
        Args:
            String : The uri of the file.
 

        Returns:
            Panda Dataframe : Dataframe of the data.
        """
        list_uri = uri.split('/')
        bucket = list_uri[2]
        filename = list_uri[3]

        s3 = boto3.client('s3')
        s3.download_file(bucket, filename, filename)

        df_csv = pd.read_csv(filename)
        return df_csv

    def retrieve_json_data(self, url):
        """
        Get a json file from an S3 url.
        
        Args:
            String : The url of the json file.
        
        Returns:
            Panda Dataframe : Dataframe of json file.
        """
        list_url = url.split('/')
        bucket = list_url[2].split('.')[0]
        filename = list_url[3]

        s3 = boto3.client('s3')
        s3.download_file(bucket, filename, filename)
        
        df_json = pd.read_json(filename)

        return df_json

if __name__ == '__main__':
    
    local_connector = DatabaseConnector('localdb_creds.yaml') 
    rds_connector = DatabaseConnector('db_creds.yaml')
    extractor = DataExtractor() 

    df_users = extractor.read_rds_table(rds_connector, 'legacy_users')

    df_card_details = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    
    mrdc_api_key = os.getenv('MRDC_API_KEY')
    header = {'x-api-key': mrdc_api_key}
    end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    no_of_stores = extractor.list_number_stores(end_point, header)
    end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    df_stores = extractor.retrieve_stores_data(end_point, header, no_of_stores)
    
    df_products = extractor.extract_from_s3('s3://data-handling-public/products.csv')

    df_orders = extractor.read_rds_table(rds_connector, 'orders_table')

    df_dates = extractor.retrieve_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

    local_connector.upload_to_db(df_users, 'legacy_users')
    local_connector.upload_to_db(df_card_details, 'legacy_card_details')
    local_connector.upload_to_db(df_stores, 'legacy_store_details')
    local_connector.upload_to_db(df_products, 'legacy_products')
    local_connector.upload_to_db(df_orders, 'legacy_orders')
    local_connector.upload_to_db(df_dates, 'legacy_dates')
    
# %%
