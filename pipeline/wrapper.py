from pipeline.extract import worker as extract_worker
from pipeline.transform import worker as transform_worker
from pipeline.load import worker as load_worker
from common.constants.sql_constants import SQLConstants as cc


def main(playlist_link, table_name, execution_date):
    """
    A wrapper function, which calls several functions:
    1. To pass in the url of the spotify playlist and extract all data on the songs from that playlist into a dataframe.
    2. Then transform this dataframe into a more comprehensive dataframe (using polars)
    3. Then load this dataframe into a .csv file or sql table (tbd)
    :param playlist_link:
    :return:
    """
    df = extract_worker(playlist_link=playlist_link, execution_date=execution_date, table_name=table_name)

    transformed_df = transform_worker(df=df, table_name=table_name, execution_date=execution_date)

    load_worker(df=transformed_df, table_name=table_name)


if __name__ == '__main__':
    spotify_global_top_50_songs = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"

    execution_date = datetime.date.today().strftime("%d/%m/%Y")

    main(playlist_link=spotify_global_top_50_songs, table_name=cc.top_50_table_name, execution_date=execution_date)