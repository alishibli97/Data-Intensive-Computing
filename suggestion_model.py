import random
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import argparse


class SuggestionModel():

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-k", "--keyfile", help="Json api key file name")
        args = parser.parse_args()
        with open(args.keyfile) as f:
            keys = json.load(f)
        self.spotifyAPI = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(keys['client_id'], keys['client_secret']))
        self.user_vector = {
            "danceability":0.0,
            "energy":0.0,
            "key":0.0,
            "loudness":0.0,
            "mode":0.0,
            "speechiness":0.0,
            "acousticness":0.0,
            "instrumentalness":0.0,
            "liveness":0.0,
            "valence":0.0,
            "tempo":0.0,
        }

        # spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
        # df = spark.read.csv('dataset_with_features.csv',header=True)

    def get_10_songs(self, songs_i_like = [], random_samples=False):
        # if we want random songs in beginning or user doesn't like any songs => generate new random songs
        if random_samples or not songs_i_like:
            ds = json.load(open("dataset_with_features"))
            links = ["https://open.spotify.com/embed/track/" + song["uri"].split(":")[-1] for song in ds]
            # print("The user vector is: ",self.user_vector)
            return random.sample(links, 10)

        # user likes some songs => update the user vector and get similar songs vectors
        else:
            ## 1. First update the user vector:
            # iterate over each song
            for track_uri in songs_i_like:
                # get the features for each song
                track_features = self.spotifyAPI.audio_features(track_uri)[0]
                # add the value for each feature from that song to the user dict
                for feature in self.user_vector:
                    self.user_vector[feature] += track_features[feature]
            # finally normalize
            for feature in self.user_vector:
                self.user_vector[feature]/=len(songs_i_like)
            # print("The user vector is: ",self.user_vector)

            ## 2. Recommendation algorithm (filtering closest vectors)
            # recommendation code goes here
            #
            ds = json.load(open("dataset_with_features"))
            links = ["https://open.spotify.com/embed/track/" + song["uri"].split(":")[-1] for song in ds]
            #
            
            return random.sample(links, 10)