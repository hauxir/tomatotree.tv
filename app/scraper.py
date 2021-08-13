import datetime
import sys
import json
import sqlite3
import urllib

import aiohttp
import asyncio
import requests

from bs4 import BeautifulSoup
from tqdm import tqdm
from yarl import URL

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

CURRENT_YEAR = datetime.date.today().year
LAST_YEAR = CURRENT_YEAR - 1

urlmap_db = sqlite3.connect("urlmap.db")
urlmap_cursor = urlmap_db.cursor()
delay_per_request = 0.1

rt_db = sqlite3.connect("rt.db")
rt_cursor = rt_db.cursor()

proxy = None

if len(sys.argv) > 1:
    proxy = sys.argv[1]


try:
    urlmap_cursor.execute(
        """
        CREATE TABLE urlmap (
            name text unique,
            url text
        )
    """
    )
except sqlite3.OperationalError:
    pass

try:
    rt_cursor.execute(
        """
        CREATE TABLE series (
            url TEXT UNIQUE,
            name TEXT,
            image TEXT,
            genre TEXT,
            network TEXT,
            year INT,
            tomatometer_score INT,
            audience_score INT,
            no_seasons INT
        )
    """
    )
except sqlite3.OperationalError:
    pass

try:
    rt_cursor.execute(
        """
        CREATE TABLE seasons (
            series_url TEXT,
            season_no INT NOT NULL,
            image TEXT,
            tomatometer_score INT,
            critic_ratings INT NOT NULL,
            audience_score INT,
            user_ratings INT NOT NULL,
            certified BOOLEAN NOT NULL,
            year INT,
            FOREIGN KEY(series_url) REFERENCES series(url) ON DELETE CASCADE,
            UNIQUE(series_url, season_no)
        )
    """
    )
except sqlite3.OperationalError as e:
    pass


def generate_urlmap():
    def map_exists(show_name):
        urlmap_cursor.execute(f"select count(*) from urlmap where name='{show_name}';")
        results = urlmap_cursor.fetchall()[0][0] > 0
        return results

    show_names = []

    print("Getting series names from Epguides...")
    for letter in tqdm(
        [
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
            "g",
            "h",
            "i",
            "j",
            "k",
            "l",
            "m",
            "n",
            "o",
            "p",
            "q",
            "r",
            "s",
            "t",
            "u",
            "v",
            "w",
            "x",
            "y",
            "z",
        ]
    ):
        page = requests.get(f"http://epguides.com/menu{letter}/")
        soup = BeautifulSoup(page.content, "html.parser")
        links = soup.select(".cont a")
        for link in links:
            show_name = link.text.replace("'", "")
            if not map_exists(show_name):
                show_names.append(show_name)

    pbar = tqdm(show_names)

    async def get_url(show_name, session):
        pbar.set_description(show_name)
        url = (
            "https://www.rottentomatoes.com/napi/search/all?type=tv&searchQuery="
            + urllib.parse.quote(show_name)
        )
        try:
            async with session.get(
                url, headers={"User-Agent": USER_AGENT}, proxy=proxy
            ) as response:
                result = await response.json()
                tvs = result.get("tv")
                items = tvs.get("items")
                if len(items) > 0:
                    item = items[0]
                    url = item["url"].replace("'", "''")
                    urlmap_cursor.execute(
                        f"""
                        INSERT INTO urlmap VALUES (
                        ?,?
                        )
                    """,
                        (show_name, url),
                    )
                    urlmap_db.commit()
                pbar.update(1)
        except Exception as e:
            print(show_name, url, e)

    async def get_urls():
        async with aiohttp.ClientSession() as client:
            tasks = []
            for show_name in show_names:
                tasks.append(
                    asyncio.ensure_future(
                        get_url(
                            show_name,
                            client,
                        )
                    )
                )
                await asyncio.sleep(delay_per_request)
            await asyncio.gather(*tasks)

    print("Getting Rotten Tomatoes URLs for series...")
    asyncio.run(get_urls())


def extract_data_from_urls():
    print("Scraping data from from Rotten Tomatoes...")

    def url_exists(url):
        rt_cursor.execute(
            f"""select count(*) from series where url='{url.replace("'","''")}';"""
        )
        return rt_cursor.fetchall()[0][0] > 0

    def delete_last_2_years_of_series():
        rt_cursor.execute(f"delete from series where year>={LAST_YEAR};")

    urls = []
    urlmap_cursor.execute(f"select url from urlmap")
    results = urlmap_cursor.fetchall()
    for r in results:
        urls.append(r[0])
    urls = list(dict.fromkeys(urls))
    urls = [url for url in urls if not url_exists(url)]
    pbar = tqdm(urls)

    def extract_rt_data(html):
        soup = BeautifulSoup(html, "html.parser")
        name = soup.select("[data-qa='score-panel-series-title']")[0].text.strip()
        no_seasons = len(soup.select("[data-qa='season-item']"))
        try:
            image = soup.select("[data-qa='poster-image']")[0].get("src")
        except:
            image = ""
        try:
            genre = soup.select("[data-qa='series-details-genre']")[0].text.strip()
        except:
            genre = ""
        try:
            network = soup.select("[data-qa='series-details-network']")[0].text.strip()
        except:
            network = ""
        try:
            year = soup.select("[data-qa='series-details-premiere-date']")[
                0
            ].text.split()[-1]
        except:
            year = 0
        try:
            tomatometer_score = (
                soup.select("[data-qa='tomatometer']")[0].text.strip().replace("%", "")
            )
        except:
            tomatometer_score = 0
        try:
            audience_score = (
                soup.select("[data-qa='audience-score']")[0]
                .text.strip()
                .replace("%", "")
            )
        except:
            audience_score = 0
        if tomatometer_score == 0 and audience_score == 0:
            raise Exception("Missing score")
        return dict(
            name=name,
            image=image,
            genre=genre,
            network=network,
            year=year,
            tomatometer_score=tomatometer_score,
            audience_score=audience_score,
            no_seasons=no_seasons,
        )

    async def scrape_url(url, session):
        pbar.set_description(url)
        try:
            async with session.get(
                url, headers={"User-Agent": USER_AGENT}, proxy=proxy
            ) as response:
                result = await response.text()
                pbar.update(1)
                item = None
                try:
                    item = extract_rt_data(result)
                except Exception as e:
                    print(url, e)
                if not item:
                    return
                url = url.replace("'", "''")
                name = (
                    f"""'{item['name'].replace("'","''")}'"""
                    if item["name"]
                    else "NULL"
                )
                image = f"'{item['image']}'" if item["image"] else "NULL"
                genre = f"'{item['genre']}'" if item["genre"] else "NULL"
                network = (
                    f"""'{item['network'].replace("'","''")}'"""
                    if item["network"]
                    else "NULL"
                )
                year = f"{item['year']}" if item["year"] else "NULL"
                tomatometer_score = (
                    f"{item['tomatometer_score']}"
                    if item["tomatometer_score"]
                    else "NULL"
                )
                audience_score = (
                    f"{item['audience_score']}" if item["audience_score"] else "NULL"
                )
                no_seasons = item["no_seasons"]
                rt_cursor.execute(
                    f"""
                    INSERT INTO series VALUES (
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ? 
                    )
                """,
                    (
                        url,
                        name,
                        image,
                        genre,
                        network,
                        year,
                        tomatometer_score,
                        audience_score,
                        no_seasons,
                    ),
                )
                rt_db.commit()
        except Exception as e:
            print(url, e)

    async def scrape_urls():
        async with aiohttp.ClientSession() as client:
            tasks = []
            for url in urls:
                if url_exists(url):
                    continue
                tasks.append(
                    asyncio.ensure_future(
                        scrape_url(
                            url,
                            client,
                        )
                    )
                )
                await asyncio.sleep(delay_per_request)
            await asyncio.gather(*tasks)

    delete_last_2_years_of_series()  # So their data gets updated
    asyncio.run(scrape_urls())


def scrape_seasons():
    print("Getting season data from from Rotten Tomatoes...")

    def season_exists(series_url, season_no):
        rt_cursor.execute(
            f"""select count(*) from seasons where series_url='{series_url.replace("'","''")}' and season_no={season_no};"""
        )
        return rt_cursor.fetchall()[0][0] > 0

    rt_cursor.execute("select url, no_seasons from series;")
    results = rt_cursor.fetchall()
    seasons = []
    for r in results:
        for i in range(1, r[1] + 1):
            if not season_exists(r[0], i):
                seasons.append((r[0], i))
    pbar = tqdm(seasons)

    def extract_rt_data(html):
        soup = BeautifulSoup(html, "html.parser")
        try:
            image = soup.select("[data-qa='poster-image']")[0].get("src")
        except:
            image = ""
        try:
            year = int(
                soup.select("[data-qa='season-premiere-date']")[0].text.split()[-1]
            )
        except:
            year = 0
        try:
            tomatometer_score = (
                soup.select("[data-qa='tomatometer']")[0].text.strip().replace("%", "")
            )
        except:
            tomatometer_score = 0

        try:
            critic_ratings = soup.select("[data-qa='tomatometer-review-count']")[
                0
            ].text.strip()
        except:
            critic_ratings = 0

        try:
            audience_score = (
                soup.select("[data-qa='audience-score']")[0]
                .text.strip()
                .replace("%", "")
            )
        except:
            audience_score = 0

        try:
            user_ratings = (
                soup.select("[data-qa='audience-rating-count']")[0].text.strip()
            ).split(" ")[-1]
        except:
            user_ratings = 0

        certified = len(soup.select(".certified-fresh")) > 0

        if tomatometer_score == 0 and audience_score == 0:
            raise Exception("Missing score")

        return dict(
            image=image,
            year=year,
            tomatometer_score=tomatometer_score,
            critic_ratings=critic_ratings,
            audience_score=audience_score,
            user_ratings=user_ratings,
            certified=certified,
        )

    async def scrape_url(urltuple, session):
        base_url, season_no = urltuple
        url = f"{base_url}/s{season_no:02}"
        pbar.set_description(url)
        try:
            async with session.get(
                url, headers={"User-Agent": USER_AGENT}, proxy=proxy
            ) as response:
                if response.status == 404:
                    raise Exception("404")
                result = await response.text()
                pbar.update(1)
                item = None
                try:
                    item = extract_rt_data(result)
                except:
                    pass
                if not item:
                    return
                image = f"'{item['image']}'" if item["image"] else "NULL"
                year = item["year"] if item["year"] else "NULL"
                tomatometer_score = (
                    item["tomatometer_score"] if item["tomatometer_score"] else "NULL"
                )
                critic_ratings = item["critic_ratings"]
                audience_score = (
                    item["audience_score"] if item["audience_score"] else "NULL"
                )
                user_ratings = item["user_ratings"]
                certified = "1" if item["certified"] else "0"
                rt_cursor.execute(
                    f"""
                    INSERT INTO seasons VALUES (
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?
                    )
                """,
                    (
                        base_url,
                        season_no,
                        image,
                        tomatometer_score,
                        critic_ratings,
                        audience_score,
                        user_ratings,
                        certified,
                        year,
                    ),
                )
                rt_db.commit()
        except Exception as e:
            print(url, e)

    async def scrape_urls():
        async with aiohttp.ClientSession() as client:
            tasks = []
            for (url, season_no) in seasons:
                if season_exists(url, season_no):
                    continue
                tasks.append(
                    asyncio.ensure_future(
                        scrape_url(
                            (url, season_no),
                            client,
                        )
                    )
                )
                await asyncio.sleep(delay_per_request)
            await asyncio.gather(*tasks)

    asyncio.run(scrape_urls())


if __name__ == "__main__":
    generate_urlmap()
    extract_data_from_urls()
    scrape_seasons()
