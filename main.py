import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
import json
import argparse


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--keyfile", help="Json api key file name")

    args = parser.parse_args()

    if args.keyfile is None:
        parser.error("please add a keyfile argument")

    keys= {}
    with open(args.keyfile) as f:
        keys = json.load(f)
        print("keys:::")
        print(keys['client_id'])
        
    birdy_uri = 'spotify:artist:2WX2uTcsvV5OnS0inACecP'
    spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(keys['client_id'], keys['client_secret']))

    results = spotify.artist_albums(birdy_uri, album_type='album')
    albums = results['items']
    while results['next']:
        results = spotify.next(results)
        albums.extend(results['items'])

    for album in albums:
        print(album['name'])

if __name__ == "__main__":
    main()

