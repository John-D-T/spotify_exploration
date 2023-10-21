import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from common.constants.client_info import ClientInfoConstants as cic



# TODO - edit the python venv to 3.11

def pipeline(playlist_link):
    """

    :param playlist_link:
    :return:
    """
    # Authentication - without user
    client_credentials_manager = SpotifyClientCredentials(client_id=cic.cid, client_secret=cic.secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    extract_songs(playlist_link=playlist_link, sp=sp)


def extract_songs(playlist_link, sp):
    """

    :param playlist_link:
    :param sp:
    :return:
    """
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_object = sp.playlist_tracks(playlist_URI)["items"]
    for track in track_object:
        # URI
        track_uri = track["track"]["uri"]

        # Track name
        track_name = track["track"]["name"]

        # Main Artist
        artist_uri = track["track"]["artists"][0]["uri"]
        artist_info = sp.artist(artist_uri)

        # Name, popularity, genre
        artist_name = track["track"]["artists"][0]["name"]
        artist_pop = artist_info["popularity"]
        artist_genres = artist_info["genres"]

        # Album
        album = track["track"]["album"]["name"]

        # Popularity of the track
        track_pop = track["track"]["popularity"]

if __name__ == '__main__':
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
    pipeline(playlist_link=playlist_link)


    # TODO - look into this next: https://www.aicrowd.com/challenges/spotify-million-playlist-dataset-challenge