from pipeline.extract import worker as extract_worker
from pipeline.transform import worker as transform_worker
from pipeline.load import worker as load_worker


def main(playlist_link):
    """
    A wrapper function, which calls several functions:
    1. To pass in the url of the spotify playlist and extract all data on the songs from that playlist into a dataframe.
    2. Then transform this dataframe into a more comprehensive dataframe (using polars)
    3. Then load this dataframe into a .csv file or sql table (tbd)
    :param playlist_link:
    :return:
    """
    df = extract_worker(playlist_link=playlist_link)

    transformed_df = transform_worker(df=df)

    load_worker(df=transformed_df)


if __name__ == '__main__':

    # TODO - think about an airflow scheduler (do not place it here)
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    main(playlist_link=playlist_link)