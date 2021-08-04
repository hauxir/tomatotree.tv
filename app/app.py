from flask import Flask, render_template
import json
import sqlite3

app = Flask(__name__, template_folder=".")


@app.route("/")
def index():
    rt_db = sqlite3.connect("rt.db")
    rt_cursor = rt_db.cursor()
    rt_cursor.execute(f"select * from series;")
    results = rt_cursor.fetchall()
    data = []
    for r in results:
        item = dict(
            url=r[0],
            name=r[1],
            image=r[2],
            genre=r[3],
            network=r[4],
            year=r[5],
            tomatometer_score=r[6],
            audience_score=r[7]
        )
        data.append(item)
    return render_template("index.html", data=json.dumps(data))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
