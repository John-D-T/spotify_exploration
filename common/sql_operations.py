import mysql.connector
from common.constants.sql_constants import SQLConstants as cc
from common.constants.hidden_info import SQLConstants as hc
from sqlalchemy import create_engine


def initialize_connection(username, password, database_name=None):
    if database_name:
        connection = mysql.connector.connect(
            host="localhost",
            user=username,
            password=password,
            database=database_name,
            auth_plugin='mysql_native_password'
        )
    else:
        connection = mysql.connector.connect(
            host="localhost",
            user=username,
            password=password,
            auth_plugin='mysql_native_password'
        )

    cursor = connection.cursor()

    return cursor, connection


def create_database(username, password, database_name):

    cursor, connection = initialize_connection(username=username, password=password)

    create_database_query = "CREATE DATABASE IF NOT EXISTS %s" % database_name

    cursor.execute(create_database_query)

    connection.commit()
    connection.close()


def generate_create_table_statement(df, database_name, table_name):
    """
    print schema of df, use that to get the names and columns, then concatenate to form the table creation_script
    """

    inferred_typing = []

    column_dtypes = str(df.dtypes).split('\n')[:-1]

    pandas_to_sql_type_conversion = {
        "string": "VARCHAR(100)",
        "int64": "BIGINT",
        "datetime64[ns]": "TIMESTAMP"
    }

    for column in column_dtypes:
        column_name = column.split(' ')[0]
        column_type = column.split(' ')[-1]
        converted_column_type = pandas_to_sql_type_conversion.get(column_type)

        inferred_typing.append("%s %s" % (column_name, converted_column_type))

    query_columns = ", ".join(inferred_typing)

    create_table_query = """CREATE TABLE IF NOT EXISTS %s.%s (%s)""" % (database_name, table_name, query_columns)

    return create_table_query


def insert_spotify_data(db, table_name, df, user, pw):
    """
    Function to insert dataframe collected from the spotify API into our MySQL table.

    :param df:
    :param cursor:
    :param table_name: table we're inserting into
    :return: N/A
    """
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                           .format(host='localhost:3306', db=db, user=user, pw=pw))

    df.to_sql(name=table_name, con=engine, index=False, if_exists='append')


def create_table(username, password, database_name, table_name, df):

    cursor, connection = initialize_connection(username=username, password=password, database_name=database_name)

    create_table_query = generate_create_table_statement(df=df,
                                                         database_name=database_name,
                                                         table_name=table_name)

    cursor.execute(create_table_query)

    connection.commit()
    connection.close()


def extract_query(username, password, database_name, table_name, query):

    cursor, connection = initialize_connection(username=username, password=password, database_name=database_name)

    cursor.execute(query)

    result = cursor.fetchall()

    connection.commit()
    connection.close()

    return result
