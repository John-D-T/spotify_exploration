import pandas as pd

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from common.constants.hidden_info import ClientInfoConstants as cic

from common.sql_operations import extract_query
from common.constants.sql_constants import SQLConstants as cc
from common.constants.hidden_info import SQLConstants as hc


def worker(playlist_link, execution_date, table_name):
    """

    :param playlist_link:
    :return: pandas dataframe with relevant information
    """

    duplication_check(execution_date=execution_date, username=hc.username, password=hc.password, database_name=cc.database_name,
                      table_name=table_name)

    # Authentication - without user
    client_credentials_manager = SpotifyClientCredentials(client_id=cic.cid, client_secret=cic.secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    track_dict = extract_songs(playlist_link=playlist_link, sp=sp)

    df = create_pandas_dataframe(track_dict=track_dict)

    return df


def duplication_check(execution_date, username, password, database_name, table_name):
    """
    partition check on execution_date to avoid reloading the same top 50 charts for a certain day

    :param execution_date:
    :param username:
    :param password:
    :param database_name:
    :param table_name:
    :return: Boolean value
    """

    query = ''

    list_of_dates_processed = extract_query(username=username, password=password, database_name=database_name,
                                            table_name=table_name, query=query)

    if execution_date in list_of_dates_processed:
        return True

    else:
        return False

def extract_songs(playlist_link, sp):
    """

    :param playlist_link:
    :param sp: spotify api
    :return:
    """
    track_dict = {
        'track_uri': [],
        'track_name': [],
        'artist_uri': [],
        'artist_followers': [],
        'artist_name': [],
        'artist_pop': [],
        'artist_genres': [],
        'album': [],
        'track_pop': []
    }

    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_object = sp.playlist_tracks(playlist_URI)["items"]

    for track in track_object:
        # URI
        track_dict['track_uri'] += [track["track"]["uri"]]

        # Track name
        track_dict['track_name'] += [track["track"]["name"]]

        # Main Artist
        artist_uri = track["track"]["artists"][0]["uri"]
        track_dict['artist_uri'] += [artist_uri]

        followers = [sp.artist(artist_uri)][0]['followers']['total']
        track_dict['artist_followers'] += [followers if followers else 0]

        # Name, popularity, genre
        track_dict['artist_name'] += [track["track"]["artists"][0]["name"]]
        track_dict['artist_pop'] += [(sp.artist(artist_uri)["popularity"])]
        track_dict['artist_genres'] += [sp.artist(artist_uri)["genres"]]

        # Album
        track_dict['album'] += [track["track"]["album"]["name"]]

        # Popularity of the track
        track_dict['track_pop'] += [(track["track"]["popularity"])]

    return track_dict


def create_pandas_dataframe(track_dict):
    df = pd.DataFrame(track_dict)
    return df


if __name__ == '__main__':
    pass
