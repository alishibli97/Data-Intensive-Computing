import random
import json
# pyspark

user_vector = [0.0]*11
# danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo

def get_10_songs(songs_i_like = []):

    # if user_vector==[0.0]*8 => do average
    # else: ..

    # pyspark
    with open("dataset_with_features") as f:

        wordlist = []

        ds = json.load(f)
        for song in ds:
            wordlist.append("https://open.spotify.com/embed/track/" + song["uri"].split(":")[-1])


        return random.sample(wordlist, 10)