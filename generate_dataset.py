import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
import json
import argparse

# spotify docs: 
# https://spotipy.readthedocs.io/en/2.16.1/
# dataset woth big data songs:
# https://www.kaggle.com/adityak80/spotify-millions-playlist

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--keyfile", help="Json api key file name")

    args = parser.parse_args()

    if args.keyfile is None:
        parser.error("please add a keyfile argument")

    with open(args.keyfile) as f:
        keys = json.load(f)        

    # read songs from dataset
    
    with open("mpd.slice.0-999.json") as f:
        ds = json.load(f)

    spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(keys['client_id'], keys['client_secret']))

    f = open("dataset_with_features", "a")

    listofallsongs = []
    for playlist in ds["playlists"]:
        for track in playlist["tracks"]:
            track_uri = track["track_uri"]
            track_features = spotify.audio_features(track_uri)
            listofallsongs.append(track_features[0])
              
        break # just taking a fews song to test poc

    x = json.dumps(listofallsongs)
    f.write(x + "\n")

    f.close()

if __name__ == "__main__":
    main()

