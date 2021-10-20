from flask import Flask, render_template, request
import suggestion_model
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():

    checkedsongs = []
    songlist = suggestion_model.get_10_songs()

    if request.method == 'POST':
        for song in songlist:
            if request.form.get(song) != None:
                checkedsongs.append(song)
                print("Entered")

        print(checkedsongs)
        songlist = suggestion_model.get_10_songs(checkedsongs)
        return render_template('form.html', songlist=songlist)
    else:
        return render_template('form.html', songlist=songlist)

app.run(host='0.0.0.0', port=5000, debug=True)
