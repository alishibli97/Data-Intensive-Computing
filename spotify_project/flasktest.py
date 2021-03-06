from flask import Flask, render_template, request
from suggestion_model import SuggestionModel
app = Flask(__name__)

model = SuggestionModel()
songlist = model.get_10_songs() # get the 10 new songs based on nothing, a bit unneccessery but

@app.route('/', methods=['GET', 'POST'])
def index():

    checkedsongs = []
    
    if request.method == 'POST':
        global songlist
        for song in songlist:

            if request.form.get(song) != None:
                checkedsongs.append(song)

        songlist = model.get_10_songs(checkedsongs) # get the 10 new songs based on user input
        # print(list(vars(model.user).values()))
        return render_template('form.html', songlist=songlist,user_vector=list(vars(model.user).values()))
    else:
        songlist = model.get_10_songs() # get the 10 songs based on nothing
        # print(list(vars(model.user).values()))
        return render_template('form.html', songlist=songlist, user_vector=list(vars(model.user).values()))

app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
