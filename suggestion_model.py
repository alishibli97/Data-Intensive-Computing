import random
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,mean,stddev
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import argparse
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from loguru import logger


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
        parser.add_argument("-m","--method", help="std or kmeans")
        args = parser.parse_args()
        with open(args.keyfile) as f:
            keys = json.load(f)
        self.spotifyAPI = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(keys['client_id'], keys['client_secret']))

        # initialize a user
        self.user = User()

        # initialize the spark session and read data
        logger.info("Initializing a SparkSession and reading data")
        spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
        self.df = spark.read.csv('dataset_with_features.csv',header=True,inferSchema=True)
        
        self.df = self.df.drop_duplicates()

        # get a list of all uris once
        self.uris = self.df.select("uri").collect()

        logger.info("Normalizing dataset to values between 0 and 1")
        self.normalize_df()

        logger.info("Getting the standard deviation of each feature column")
        self.generate_stds()

        self.std = args.method=="std"

        # kmeans
        if args.method=="kmeans":
            logger.info("Fitting kmeans model")
            self.generate_kmeans_model()

    def generate_stds(self):
        # get the std for each column once
        self.std1 = self.df.select(stddev(col('danceability')).alias('std')).collect()[0].asDict()['std']
        self.std2 = self.df.select(stddev(col('energy')).alias('std')).collect()[0].asDict()['std']
        self.std3 = self.df.select(stddev(col('key')).alias('std')).collect()[0].asDict()['std']
        self.std4 = self.df.select(stddev(col('loudness')).alias('std')).collect()[0].asDict()['std']
        self.std5 = self.df.select(stddev(col('mode')).alias('std')).collect()[0].asDict()['std']
        self.std6 = self.df.select(stddev(col('speechiness')).alias('std')).collect()[0].asDict()['std']
        self.std7 = self.df.select(stddev(col('acousticness')).alias('std')).collect()[0].asDict()['std']
        self.std8 = self.df.select(stddev(col('instrumentalness')).alias('std')).collect()[0].asDict()['std']
        self.std9 = self.df.select(stddev(col('liveness')).alias('std')).collect()[0].asDict()['std']
        self.std10 = self.df.select(stddev(col('valence')).alias('std')).collect()[0].asDict()['std']
        self.std11 = self.df.select(stddev(col('tempo')).alias('std')).collect()[0].asDict()['std']
        # print(self.std1,self.std2,self.std3,self.std4,self.std5,self.std6,self.std7,self.std8,self.std9,self.std10,self.std11)

    def normalize_df(self):
        unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())
        for i in vars(self.user):
            assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")
            scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")
            pipeline = Pipeline(stages=[assembler, scaler])
            self.df = pipeline.fit(self.df).transform(self.df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i+"_Vect")
            self.df = self.df.drop(i).withColumnRenamed(i+"_Scaled",i)

    def generate_kmeans_model(self):
        # assemble the data for fitting a kmeans model
        assemble=VectorAssembler(inputCols=['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo'],outputCol='features')
        logger.info("Fitting assembler")
        df_assembled=assemble.transform(self.df)
        kmeans=KMeans(featuresCol='features', k=10)
        logger.info("Fitting actual model")
        self.model = kmeans.fit(df_assembled)

    def get_10_songs(self, songs_i_like = [], random_samples=False):
        # if we want random songs in beginning or user doesn't like any songs => generate new random songs
        if random_samples or not songs_i_like:
            links = ["https://open.spotify.com/embed/track/" + uri.__getitem__("uri").split(":")[-1] for uri in self.uris]
            return random.sample(links, 10)

        # user likes some songs => update the user vector and get similar songs vectors
        else:
            ## 1. First update the user vector:
            # iterate over each song
            for track_uri in songs_i_like:
                # get the features for each song
                # track_features = self.spotifyAPI.audio_features(track_uri)[0]
                song_features = self.df.filter(self.df.uri=="spotify:track:"+track_uri.split("/")[-1]).take(1)[0].asDict()
                print("The new song features are",song_features)
                # add the value for each feature from that song to the user dict
                for feature in vars(self.user):
                    attr = getattr(self.user, feature)
                    setattr(self.user, feature, attr+song_features[feature])
            # finally normalize
            for feature in vars(self.user):
                attr = getattr(self.user, feature)
                setattr(self.user, feature, attr/len(songs_i_like))

            print("The new user vector is",vars(self.user))

            ## 2. Recommendation algorithm (filtering closest vectors)
            
            if self.std:
                return self.recommend_by_std()
            else:
                return self.recommend_by_kmeans()

    def recommend_by_std(self):
        # to check for feature importance: if previous feature value - new feature value >2 * std => not important
        
        # need all rows where difference between user feature and row feature is less than 1 std
        condition = (self.df.danceability-self.user.danceability<self.std1) \
                & (self.df.energy-self.user.energy<self.std2) \
                & (self.df.key-self.user.key<self.std3) \
                & (self.df.loudness-self.user.loudness<self.std4) \
                & (self.df.mode-self.user.mode<self.std5) \
                & (self.df.speechiness-self.user.speechiness<self.std6) \
                & (self.df.acousticness-self.user.acousticness<self.std7) \
                & (self.df.instrumentalness-self.user.instrumentalness<self.std8) \
                & (self.df.liveness-self.user.liveness<self.std9) \
                & (self.df.valence-self.user.valence<self.std10) \
                & (self.df.tempo-self.user.tempo<self.std11)

        # self.df.where(condition).show(10)
        filtered_df = self.df.where(condition).head(10)

        links = ["https://open.spotify.com/embed/track/" + row.uri.split(":")[-1] for row in filtered_df]

        return random.sample(links, 10)

    def recommend_by_kmeans(self):

        user_vec = Vectors.dense(list(vars(self.user).values()))
        idx = self.model.predict(user_vec)

        center = self.model.clusterCenters()[idx]

        condition = (self.df.danceability-center[0]<self.std1) \
                & (self.df.energy-center[1]<self.std2) \
                & (self.df.key-center[2]<self.std3) \
                & (self.df.loudness-center[3]<self.std4) \
                & (self.df.mode-center[4]<self.std5) \
                & (self.df.speechiness-center[5]<self.std6) \
                & (self.df.acousticness-center[6]<self.std7) \
                & (self.df.instrumentalness-center[7]<self.std8) \
                & (self.df.liveness-center[8]<self.std9) \
                & (self.df.valence-center[9]<self.std10) \
                & (self.df.tempo-center[10]<self.std11)

        # self.df.where(condition).show(10)
        filtered_df = self.df.where(condition).head(10)

        links = ["https://open.spotify.com/embed/track/" + row.uri.split(":")[-1] for row in filtered_df]

        return random.sample(links, 10)
