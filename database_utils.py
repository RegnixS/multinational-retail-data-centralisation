# %%
"""Database Utils Class Module

This module implements a class object for connecting to a database and interacting with it.
"""
from sqlalchemy import create_engine
from sqlalchemy import inspect
import yaml


class DatabaseConnector:
    """
    This class is used to connect to a database and interact with it.
    
    Attributes:
        cred_file (string): The YAML file with the database credentials.
        engine (sqlalchmeny engine) : Database engine.
    """
    def __init__(self, creds_file):
        """
            Initializes DatabaseConnector.
        """
        self.creds_file = creds_file
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        """
        Reads database credentials from YAML.
        
        Returns:
            Dictionary : Database credentials.
        """
        
        with open(self.creds_file, 'r') as f:
            db_creds = yaml.safe_load(f)

        return db_creds
    
    def init_db_engine(self):
        """
        Initializes the sqlalchemy database engine.

        Returns:
            Sqlalchmeny Engine : Database engine.
        """
        db_creds = self.read_db_creds()
       
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = db_creds["RDS_HOST"]
        USER = db_creds["RDS_USER"]
        PASSWORD = db_creds["RDS_PASSWORD"]
        PORT = db_creds["RDS_PORT"]
        DATABASE = db_creds["RDS_DATABASE"]
        
        return create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
    
    def list_db_tables(self):
        """
        Lists all tables in database.
        
        Returns:
            List : Database tables.
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        """
        Uploads the dataframe to a table in the database.
        
        Args:
            Pandas Dataframe : The dataframe to be uploaded.
            String : The name of the database table.
        """
        df.to_sql(table_name, self.engine, if_exists='replace')

if __name__ == '__main__':
    rds_connnector = DatabaseConnector('db_creds.yaml')
    list_of_tables = rds_connnector.list_db_tables()
    print(list_of_tables)
# %%
