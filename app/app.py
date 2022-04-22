from flask import Flask, render_template
import json
import sqlite3

app = Flask(__name__, template_folder=".")

MIN_VOTES = 20


@app.route("/")
def index():
    rt_db = sqlite3.connect("rt.db")
    rt_cursor = rt_db.cursor()
    rt_cursor.execute(
        f"""
        SELECT series.*, s.cr, s.ur, s.cert
        FROM series JOIN (
          SELECT series_url, sum(critic_ratings) as cr, sum(user_ratings) AS ur, (sum(certified) > 0) AS cert FROM SEASONS GROUP BY series_url
         ) AS s ON s.series_url=url;
    """
    )
    results = rt_cursor.fetchall()
    data = []
    for r in results:
        item = dict(
            url=r[0].replace(
                "https://rottentomatoes.com", "https://www.rottentomatoes.com"
            ),
            name=r[1],
            image=r[2],
            genre=r[3],
            network=r[4],
            year=r[5],
            tomatometer_score=r[6],
            audience_score=r[7],
            no_seasons=r[8],
            critic_ratings=r[9],
            user_ratings=r[10],
            certified=r[11] > 0,
        )
        if item["critic_ratings"] < MIN_VOTES:
            item["tomatometer_score"] = None
        if item["user_ratings"] < MIN_VOTES:
            item["audience_score"] = None
        data.append(item)
    return render_template("index.html", data=json.dumps(data))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=81)
