import datetime
import uuid

from pipeline.mappings import column_mappings as cm


def worker(df, table_name):
    """
    :param df:
    :return:
    """
    df = transform_df_wrapper(df=df, table_name=table_name)

    return df


def transform_df_wrapper(df, table_name):
    """
    A wrapper function containing all the necessary transformations for the dataframe

    :param df: input dataframe
    :return: dataframe with all transformations applied
    """

    df = add_watermark_columns(df=df)
    df = convert_typing(df=df, table_name=table_name)
    return df


def add_watermark_columns(df):
    """
    Function which adds watermark columns to the dataframe. These watermark columns include:
    1. An execution timestamp
    2. The execution date
    2. A unique id

    :param df: input dataframe
    :return: dataframe with watermark columns added
    """
    execution_date = datetime.date.today().strftime("%d/%m/%Y")
    df['execution_date'] = execution_date

    execution_timestamp = datetime.datetime.now()
    df['execution_timestamp'] = execution_timestamp

    unique_id = uuid.uuid4()
    df['unique_id'] = unique_id

    return df


def convert_typing(df, table_name):
    """
    Function to loop through a mapping, and convert the column types in the df accordingly
    :param df:
    :param table_name:
    :return:
    """
    conversion_dict = cm.tables_to_convert[table_name]

    for conversion in conversion_dict:
        df[conversion] = df[conversion].astype(conversion_dict[conversion])

    return df


if __name__ == '__main__':
    pass
