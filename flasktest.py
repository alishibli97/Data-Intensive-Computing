from flask import Flask, render_template, request
from suggestion_model import SuggestionModel
app = Flask(__name__)

model = SuggestionModel()
songlist = model.get_10_songs(random_samples=True) # get the 10 new songs based on nothing, a bit unneccessery but

@app.route('/', methods=['GET', 'POST'])
def index():

    checkedsongs = []
    
    if request.method == 'POST':
        global songlist
        for song in songlist:

            if request.form.get(song) != None:
                checkedsongs.append(song)

<<<<<<< HEAD
        songlist = suggestion_model.get_10_songs(checkedsongs) # get the 10 new songs based on user input
        return render_template('form.html', songlist=songlist)
    else:
        songlist = suggestion_model.get_10_songs() # get the 10 songs based on nothing
        return render_template('form.html', songlist=songlist, user_vector=suggestion_model.user_vector)
=======
        songlist = model.get_10_songs(checkedsongs) # get the 10 new songs based on user input
        return render_template('form.html', songlist=songlist)
    else:
        songlist = model.get_10_songs() # get the 10 songs based on nothing
        return render_template('form.html', songlist=songlist)
>>>>>>> aca80807615e5a075d39ef59263e6b21cb2e01d2

app.run(host='0.0.0.0', port=5000, debug=True)
