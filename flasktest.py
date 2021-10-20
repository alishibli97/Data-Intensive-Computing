from flask import Flask, render_template, request
import suggestion_model
app = Flask(__name__)

songlist = suggestion_model.get_10_songs()

@app.route('/', methods=['GET', 'POST'])
def index():

    checkedsongs = []
    
    if request.method == 'POST':
        global songlist
        for song in songlist:

            #print(song, request.form.getlist(song))
            if request.form.get(song) != None:
                checkedsongs.append(song)

        #print(checkedsongs)
        songlist = suggestion_model.get_10_songs(checkedsongs)
        return render_template('form.html', songlist=songlist)
    else:
        songlist = suggestion_model.get_10_songs()
        return render_template('form.html', songlist=songlist)

app.run(host='0.0.0.0', port=5000, debug=True)
