import pandas as pd

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from common.constants.hidden_info import ClientInfoConstants as cic

from common.sql_operations import extract_query


def worker(playlist_link):
    """

    :param playlist_link:
    :return: pandas dataframe with relevant information
    """

    # Authentication - without user
    client_credentials_manager = SpotifyClientCredentials(client_id=cic.cid, client_secret=cic.secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    track_dict = extract_songs(playlist_link=playlist_link, sp=sp)

    df = create_pandas_dataframe(track_dict=track_dict)

    return df


def extract_songs(playlist_link, sp):
    """

    :param playlist_link:
    :param sp: spotify api
    :return:
    """
    track_dict = {
        'ranking': [],
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

    for ranking, track in enumerate(track_object):
        # Ranking
        track_dict['ranking'] += [ranking + 1]

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
