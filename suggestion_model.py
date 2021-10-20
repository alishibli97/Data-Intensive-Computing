import random
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,mean,stddev
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import argparse
# import findspark

class User:
    def __init__(self,danceability=0.0,energy=0.0,key=0.0,loudness=0.0,mode=0.0,speechiness=0.0,acousticness=0.0,instrumentalness=0.0,liveness=0.0,valence=0.0,tempo=0.0):
        self.danceability=danceability
        self.energy=energy
        self.key=key
        self.loudness=loudness
        self.mode=mode
        self.speechiness=speechiness
        self.acousticness=acousticness
        self.instrumentalness=instrumentalness
        self.liveness=liveness
        self.valence=valence
        self.tempo=tempo

class SuggestionModel:

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-k", "--keyfile", help="Json api key file name")
        args = parser.parse_args()
        with open(args.keyfile) as f:
            keys = json.load(f)
        self.spotifyAPI = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(keys['client_id'], keys['client_secret']))

        self.user = User()

        spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
        self.df = spark.read.csv('dataset_with_features.csv',header=True)
        self.ds = json.load(open('dataset_with_features'))

        # findspark.find()

    def get_10_songs(self, songs_i_like = [], random_samples=False):
        # if we want random songs in beginning or user doesn't like any songs => generate new random songs
        if random_samples or not songs_i_like:
            links = ["https://open.spotify.com/embed/track/" + song["uri"].split(":")[-1] for song in self.ds]
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
                for feature in vars(self.user):
                    attr = getattr(self.user, feature)
                    setattr(self.user, feature, attr+track_features[feature])
                    # self.user.feature += track_features[feature]
            # finally normalize
            for feature in vars(self.user):
                attr = getattr(self.user, feature)
                setattr(self.user, feature, attr/len(songs_i_like))
                # self.user_vector[feature]/len(songs_i_like)
            # print("The user vector is: ",self.user_vector)

            ## 2. Recommendation algorithm (filtering closest vectors)

            # ds = json.load(open("dataset_with_features"))
            # links = ["https://open.spotify.com/embed/track/" + song["uri"].split(":")[-1] for song in ds]
            
            std1 = self.df.select(stddev(col('danceability')).alias('std')).collect()[0].asDict()['std']
            std2 = self.df.select(stddev(col('energy')).alias('std')).collect()[0].asDict()['std']
            std3 = self.df.select(stddev(col('key')).alias('std')).collect()[0].asDict()['std']
            std4 = self.df.select(stddev(col('loudness')).alias('std')).collect()[0].asDict()['std']
            std5 = self.df.select(stddev(col('mode')).alias('std')).collect()[0].asDict()['std']
            std6 = self.df.select(stddev(col('speechiness')).alias('std')).collect()[0].asDict()['std']
            std7 = self.df.select(stddev(col('acousticness')).alias('std')).collect()[0].asDict()['std']
            std8 = self.df.select(stddev(col('instrumentalness')).alias('std')).collect()[0].asDict()['std']
            std9 = self.df.select(stddev(col('liveness')).alias('std')).collect()[0].asDict()['std']
            std10 = self.df.select(stddev(col('valence')).alias('std')).collect()[0].asDict()['std']
            std11 = self.df.select(stddev(col('tempo')).alias('std')).collect()[0].asDict()['std']
            # print(std1,std2,std3,std4,std5,std6,std7,std8,std9,std10,std11)
            condition = (self.df.danceability-self.user.danceability<std1) \
                    & (self.df.energy-self.user.energy<std2) \
                    & (self.df.key-self.user.key<std3) \
                    & (self.df.loudness-self.user.loudness<std4) \
                    & (self.df.mode-self.user.mode<std5) \
                    & (self.df.speechiness-self.user.speechiness<std6) \
                    & (self.df.acousticness-self.user.acousticness<std7) \
                    & (self.df.instrumentalness-self.user.instrumentalness<std8) \
                    & (self.df.liveness-self.user.liveness<std9) \
                    & (self.df.valence-self.user.valence<std10) \
                    & (self.df.tempo-self.user.tempo<std11)

            self.df.where(condition).show(10)
            filtered_df = self.df.where(condition).head(10)

            links = ["https://open.spotify.com/embed/track/" + row.uri.split(":")[-1] for row in filtered_df]

            return random.sample(links, 10)
