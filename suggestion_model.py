import random
import json

def get_10_songs(songs_i_like = []):

    with open("dataset_with_features") as f:

        wordlist = []

        ds = json.load(f)
        for song in ds:
            wordlist.append("https://open.spotify.com/embed/track/" + song["uri"].split(":")[-1])

        return random.sample(wordlist, 10)
