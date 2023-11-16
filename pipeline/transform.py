import polars
import datetime
import uuid


def worker(df):
    """
    :param df:
    :return:
    """

    track_dict = transform_df_wrapper(df=df)
    return df


def transform_df_wrapper(df):
    """
    A wrapper function containing all the necessary transformations for the dataframe

    :param df: input dataframe
    :return: dataframe with all transformations applied
    """

    df = add_watermark_columns(df=df)
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
    execution_timestamp = datetime.datetime.now()
    unique_id = uuid.uuid4()
    df = df.with_columns(execution_date = execution_date)
    df = df.with_columns(execution_timestamp = execution_timestamp)
    df = df.with_columns(unique_id = unique_id)

    return df


if __name__ == '__main__':
    pass
