"""
mysql version: 0.0.3

(I'm using python version 3.11)

pip install mysql
pip install pymysql

Note:
    - You'll also need to download MySQL, or else you have nothing to connect to
    - You also need to set up a user account with a username and password during your MySQL set-up.
      This is what you'll be putting into your config file to pass into this module when running your SQL statements.

NOTE: on mysql-connector-python-rf, we get this issue:
    Authentication plugin 'caching_sha2_password' is not supported
    SOLN: So install mysql-connector-python instead
"""
import sys

from common.constants.sql_constants import SQLConstants as cc
from common.constants.hidden_info import SQLConstants as hc

from common.sql_operations import create_database, insert_spotify_data, create_table, duplication_check


def worker(df, table_name, execution_date):
    """
    Function to write (append) our transformed dataframe into a SQL table.
    :param df:
    :param table_name:
    :return:
    """
    create_database(username=hc.username, password=hc.password, database_name=cc.database_name)

    create_table(username=hc.username, password=hc.password, database_name=cc.database_name,
                 table_name=table_name, df=df)

    duplicate_found = duplication_check(execution_date=execution_date, username=hc.username, password=hc.password, database_name=cc.database_name,
                      table_name=table_name)

    if duplicate_found:
        print(f'Duplicate found for execution date: {execution_date}')
        sys.exit()

    insert_spotify_data(db=cc.database_name, table_name=cc.top_50_table_name, df=df,
                        user=hc.username, pw=hc.password)



