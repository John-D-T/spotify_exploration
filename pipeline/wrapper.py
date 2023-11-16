from pipeline.extract import worker as extract_worker
from pipeline.transform import worker as transform_worker
from pipeline.load import worker as load_worker


def main(playlist_link):
    df = extract_worker(playlist_link=playlist_link)
    transformed_df = transform_worker(df=df)
    load_worker(df=transformed_df)


if __name__ == '__main__':
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    main(playlist_link=playlist_link)