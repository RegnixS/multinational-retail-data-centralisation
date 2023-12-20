# %%
"""Data Cleaning Class Module

This module implements a class object for cleaning data.
"""
from dotenv import load_dotenv
from dateutil.parser import parse
import numpy as np
import os
import pandas as pd
import string

from database_utils import DatabaseConnector
from data_extraction import DataExtractor


load_dotenv()

class DataCleaning:
    """
    This class is used to clean data.
    
    Attributes:
        name (type): Description.
    """
    def __init__(self):
        """
            Initializes DataCleaning.
        """
    def clean_user_data(self, df_users, validate_phone=False):
        """
            Cleans the user data.
            
            Args:
                Pandas Dataframe : The dataframe to be cleaned.
                Boolean : Option to validate phone numbers. Default = False.
                          Only UK numbers are validated pending choice of regex for other countries.
            
            Returns:
                Pandas Dataframe : Cleaned user data.
        """
        # drop rows with "NULL" in all columns
        df_users.replace({'NULL':None}, inplace=True)
        df_users.dropna(how='all', inplace=True)

        # convert date_of_birth to date
        df_users['date_of_birth'] = df_users['date_of_birth'].apply(self.parse_date)

        # drop rows with NaT in date_of_birth (These rows have random characters in all columns)
        df_users.dropna(inplace=True)

        # convert join_date to date        
        df_users['join_date'] = df_users['join_date'].apply(self.parse_date)

        # fix country code "GGB"
        df_users['country_code'].replace({'GGB':'GB'}, inplace=True)
        
        # validate phone numbers (Only UK phone numbers are validated at present)
        if validate_phone:
            regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
            df_users.loc[(~df_users['phone_number'].str.match(regex_expression)) & (df_users['country_code'] == 'GB'), 'phone_number'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        
        return df_users

    def clean_card_data(self, df_cards):
        """
            Cleans the card data.
            
            Args:
                Pandas Dataframe : The dataframe to be cleaned.
            
            Returns:
                Pandas Dataframe : Cleaned card data.
        """
        # drop rows with "NULL" in all columns
        df_cards.replace({'NULL':None, 'NULL NULL':None}, inplace=True)
        df_cards.dropna(how='all', inplace=True)

        # drop column "Unnamed: 0"
        df_cards.drop('Unnamed: 0', axis=1, inplace=True)

        # slice "card_number expiry_date" and put in relevant columns
        df_cards.loc[(~df_cards['card_number expiry_date'].isnull()), 'card_number'] = df_cards['card_number expiry_date'].str[:-5]
        df_cards.loc[(~df_cards['card_number expiry_date'].isnull()), 'expiry_date'] = df_cards['card_number expiry_date'].str[-5:]
        df_cards.drop('card_number expiry_date', axis=1, inplace=True)

        # remove "?" from card_number, convert to numbers and drop nulls
        # (rows with nulls have random characters in all columns) 
        df_cards['card_number'] = df_cards['card_number'].str.replace('?','')
        df_cards['card_number'] = pd.to_numeric(df_cards['card_number'], errors='coerce')
        df_cards.dropna(inplace=True)
        df_cards['card_number'] = df_cards['card_number'].astype('int64')

        # convert date_payment_confirmed to dates
        df_cards['date_payment_confirmed'] = df_cards['date_payment_confirmed'].apply(self.parse_date)

        return df_cards
    
    def clean_store_data(self, df_stores):
        """
            Cleans the store data.
            
            Args:
                Pandas Dataframe : The dataframe to be cleaned.
            
            Returns:
                Pandas Dataframe : Cleaned store data.
        """
        # drop rows with "NULL" in all columns
        df_stores.replace({'NULL':None, 'N/A':None}, inplace=True)
        df_stores.dropna(how='all', inplace=True)
        
        # drop rows where lat has a value (these rows have random characters in all columns)
        # then drop lat
        df_stores = df_stores[df_stores['lat'].isnull()].copy()
        df_stores.drop('lat', axis=1, inplace=True)

        # move latitude to before longtitude where it should be
        col = df_stores.pop('latitude')
        df_stores.insert(1, col.name, col)

        # fix continents with prefix "ee"
        df_stores['continent'].replace({'eeAmerica':'America', 'eeEurope':'Europe'}, inplace=True)
        
        # assuming the numbers are ok, get rid of extra non-numeric characters in staff_numbers
        df_stores['staff_numbers'] = df_stores['staff_numbers'].apply(lambda x: x.translate(str.maketrans('', '', string.ascii_letters)))

        # cast number columns
        df_stores['latitude'] = df_stores['latitude'].astype('float64')
        df_stores['longitude'] = df_stores['longitude'].astype('float64')
        df_stores['staff_numbers'] = df_stores['staff_numbers'].astype('int64')

        # convert opening_date to date
        df_stores['opening_date'] = df_stores['opening_date'].apply(self.parse_date)

        return df_stores

    def convert_product_weights(self, df_products):
        """
            Converts all product weights to kilograms.
            
            Args:
                Pandas Dataframe : The dataframe to be cleaned.
            
            Returns:
                Pandas Dataframe : Cleaned products data.
        """
        for index, row in df_products.iterrows():
            weight = row['weight']
            # catch the nulls
            try:
                # strip out the " ."
                if weight[-2:] == ' .':
                    weight = weight[:len(weight)-2]
                # change "ml" to "g"
                if weight[-2:] == 'ml':
                    weight = weight[:len(weight)-2] + 'g'
                # convert kg to g
                if weight[-2:] == 'kg':
                    weight = weight[:len(weight)-2] + ' x 1000'       
                # convert oz to g
                elif weight[-2:] == 'oz':
                    weight = weight[:len(weight)-2] + ' x 28.35'
                # should only be g left
                elif weight[-1:] == 'g':
                    weight = weight[:len(weight)-1]
                # anything else would be invalid
                else:    
                    weight = np.nan
                
                # now calculate multiples & conversions
                if weight != np.nan:
                    list_weights = weight.split(' x ')
                    weight = 1
                    for i in list_weights:
                        weight = float(weight) * float(i)
                    # convert back to kg
                    weight = weight / 1000
            except:
                weight = np.nan
            
            df_products.loc[index, 'weight'] = weight
        
        df_products['weight'] = df_products['weight'].astype('float64')

        return df_products
    
    def clean_products_data(self, df_products):
        """
            Cleans the products data.
            
            Args:
                Pandas Dataframe : The dataframe to be cleaned.
            
            Returns:
                Pandas Dataframe : Cleaned products data.
        """
        # drop rows with "NULL" in any columns
        # this includes rows with random characters in all columns as previous function changed weight to NaN
        df_products.dropna(inplace=True)

        # drop extra index column
        df_products.drop('Unnamed: 0', axis=1, inplace=True)

        # remove £ sign and set to numeric
        df_products['product_price'] = df_products['product_price'].str.replace('£','')
        df_products['product_price'] = df_products['product_price'].astype('float64')

        # convert date_added to date
        #df_copy = df_products.copy()
        #df_copy['date_added'] = pd.to_datetime(df_copy['date_added'], errors='coerce')
        df_products['date_added'] = df_products['date_added'].apply(self.parse_date)
        #print(df_products.loc[df_copy['date_added'].isnull(),['date_added']])

        return df_products

    def clean_orders_data(self, df_orders):
        """
            Cleans the orders data.
            
            Args:
                Pandas Dataframe : The dataframe to be cleaned.
            
            Returns:
                Pandas Dataframe : Cleaned orders data.
        """
        # drop extra columns
        df_orders.drop('level_0', axis=1, inplace=True)
        df_orders.drop('first_name', axis=1, inplace=True)
        df_orders.drop('last_name', axis=1, inplace=True)
        df_orders.drop('1', axis=1, inplace=True)

        return df_orders
    
    def clean_dates_data(self, df_dates):
        """
            Cleans the dates data.
            
            Args:
                Pandas Dataframe : The dataframe to be cleaned.
            
            Returns:
                Pandas Dataframe : Cleaned dates data.
        """
        # drop rows with length of date_uuid < 36
        # these rows have "NULL" or random characters in all columns
        df_dates = df_dates[df_dates['date_uuid'].apply(len) == 36].copy() 

        return df_dates
    
    @staticmethod
    def parse_date(x):
        """
        Allows for strings that can't be converted while parsing the date.

        Args:
            String : The column from the dataframe.
        
        Returns:
            String : String parsed as Datetime 
        """
        try:
            return parse(x)
        except:
            return pd.NaT
        
if __name__ == '__main__':
    
    # initialize instances
    local_connector = DatabaseConnector('localdb_creds.yaml')  
    rds_connector = DatabaseConnector('db_creds.yaml')
    extractor = DataExtractor()
    cleaner = DataCleaning() 

    # extract data
    df_users = extractor.read_rds_table(rds_connector, 'legacy_users')

    df_card_details = extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
   
    mrdc_api_key = os.getenv('MRDC_API_KEY')
    header = {'x-api-key': mrdc_api_key}
    end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    no_of_stores = extractor.list_number_stores(end_point, header)
    end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    df_store_details = extractor.retrieve_stores_data(end_point, header, no_of_stores)

    df_products = extractor.extract_from_s3('s3://data-handling-public/products.csv')

    df_orders = extractor.read_rds_table(rds_connector, 'orders_table')

    df_dates = extractor.retrieve_json_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
 
    # clean the data
    df_users = cleaner.clean_user_data(df_users)
    df_card_details = cleaner.clean_card_data(df_card_details)
    df_store_details = cleaner.clean_store_data(df_store_details)
    df_products = cleaner.convert_product_weights(df_products)
    df_products = cleaner.clean_products_data(df_products)
    df_orders = cleaner.clean_orders_data(df_orders)
    df_dates = cleaner.clean_dates_data(df_dates)

    # upload to database
    local_connector.upload_to_db(df_users, 'dim_users')
    local_connector.upload_to_db(df_card_details, 'dim_card_details')
    local_connector.upload_to_db(df_store_details, 'dim_store_details')
    local_connector.upload_to_db(df_products, 'dim_products')
    local_connector.upload_to_db(df_orders, 'orders_table')
    local_connector.upload_to_db(df_dates, 'dim_date_times')

 # %%
