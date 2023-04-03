import datetime
import json
import re
import sqlite3
import sys
import traceback
import urllib

import aiohttp
import asyncio
import requests

from bs4 import BeautifulSoup
from tqdm import tqdm
from yarl import URL

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem


def user_agent():
    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value]
    user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)
    return user_agent_rotator.get_random_user_agent()


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


def only_numbers(s):
    return re.sub(r'[^0-9]', '', s)


def generate_urlmap():
    def map_exists(show_name):
        urlmap_cursor.execute("select count(*) from urlmap where name=?;",(show_name,))
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
            show_name = link.text
            if not map_exists(show_name):
                show_names.append(show_name)

    pbar = tqdm(show_names)

    async def get_url(show_name, session):
        url = (
            "https://www.rottentomatoes.com/search?search="
            + urllib.parse.quote(show_name)
        )
        try:
            async with session.get(
                url, headers={}, proxy=proxy, raise_for_status=True
            ) as response:
                html = await response.text()
                soup = BeautifulSoup(html, features="html.parser")
                tv = soup.find("search-page-result", dict(type='tvSeries'))
                if not tv:
                    raise Exception("No results found")
                items = [dict(url=a['href'],title=a.contents[0].strip()) for a in tv.findAll("a", {"data-qa" : "info-name"})]
                for item in items:
                    url = item.get("url")
                    title = item.get("title")
                    pbar.set_description(title)
                    if not map_exists(title):
                        urlmap_cursor.execute(
                            """
                            INSERT INTO urlmap VALUES (
                            ?,?
                            )
                        """,
                            (title, url),
                        )
                urlmap_db.commit()
                pbar.update(1)
        except Exception as e:
            pbar.set_description(f"{show_name}, {url}: {e}")

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
            "select count(*) from series where url=? and year < ?;",
            (url, LAST_YEAR)
        )
        return rt_cursor.fetchall()[0][0] > 0

    urls = []
    urlmap_cursor.execute("select url from urlmap")
    results = urlmap_cursor.fetchall()
    for r in results:
        urls.append(r[0])
    urls = list(dict.fromkeys(urls))
    urls = [url for url in urls if not url_exists(url)]
    pbar = tqdm(urls)

    def extract_rt_data(html):
        soup = BeautifulSoup(html, "html.parser")
        name = soup.select("h1")[0].contents[0].strip()
        no_seasons = len(soup.select("[data-qa='season-item']"))
        try:
            image = soup.select("[data-qa='poster-image']")[0].get("src")
        except:
            image = ""
        try:
            genre = soup.select('b:contains("Genre: ")')[
                0
            ].find_next_siblings()[0].text
        except:
            genre = ""
        try:
            network = soup.select('b:contains("TV Network: ")')[
                0
            ].find_next_siblings()[0].text
        except:
            network = ""
        try:
            year = soup.select('b:contains("Premiere Date: ")')[
                0
            ].find_next_siblings()[0].text.split()[-1]
        except Exception as e:
            year = 0
        try:
            scoreboard = soup.select("score-board")[0]
            tomatometer_score = scoreboard.get("tomatometerscore") or 0
        except:
            tomatometer_score = 0
        try:
            scoreboard = soup.select("score-board")[0]
            audience_score = scoreboard.get("audiencescore") or 0
        except:
            audience_score = 0
        if tomatometer_score == 0 and audience_score == 0:
            raise Exception("Missing score")

        if year == 0:
            raise Exception("Missing year")

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
                url, headers={"User-Agent": user_agent()},
                proxy=proxy
            ) as response:
                if response.status == 404:
                    pbar.set_description("404")
                    return
                if response.status == 403:
                    pbar.set_description("403")
                    return
                result = await response.text()
                pbar.update(1)
                item = None
                try:
                    item = extract_rt_data(result)
                except Exception as e:
                    pbar.set_description(f"{url}: {e}")
                if not item:
                    return
                url = url.replace("https://rottentomates.com","https://www.rottentomatoes.com")
                name = item["name"]
                image = item['image']
                genre = item['genre']
                network = item['network']
                year = item['year']
                tomatometer_score = item['tomatometer_score']
                audience_score = item['audience_score']
                no_seasons = item["no_seasons"]
                rt_cursor.execute(
                    """
                    INSERT OR REPLACE INTO series VALUES (
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
                    )
                )
                rt_db.commit()
        except Exception as e:
            pbar.set_description(f"{url}: {e}")

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

    asyncio.run(scrape_urls())
    return urls


def scrape_seasons(urls):
    print("Getting season data from from Rotten Tomatoes...")

    def season_exists(series_url, season_no):
        rt_cursor.execute(
            "select count(*) from seasons where series_url=? and season_no=? and year < ?;",
            (series_url, season_no, CURRENT_YEAR-1)
        )
        return rt_cursor.fetchall()[0][0] > 0

    rt_cursor.execute("select url, no_seasons from series;")
    results = rt_cursor.fetchall()
    results = [r for r in results if r[0] in urls]
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
            year = soup.select('b:contains("Premiere Date:")')[
                0
            ].find_next_siblings()[0].text.split()[-1]
        except:
            year = 0
        try:
            scoreboard = soup.select("score-board")[0]
            tomatometer_score = scoreboard.get("tomatometerscore") or 0
        except:
            tomatometer_score = 0

        try:
            critic_ratings = only_numbers(
                soup.select("[data-qa='tomatometer-review-count']")[
                0
                ].text.strip()
            )
        except:
            critic_ratings = 0

        try:
            scoreboard = soup.select("score-board")[0]
            audience_score = scoreboard.get("audiencescore") or 0
        except:
            audience_score = 0

        try:
            user_ratings = only_numbers(
                soup.select("[data-qa='audience-rating-count']")[0].text.strip()
            )
        except:
            user_ratings = 0

        certified = len(soup.select(("[state='certified-fresh']"))) > 0

        if tomatometer_score == 0 and audience_score == 0:
            raise Exception("Missing score")

        if year == 0:
            raise Exception("Missing year")

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
                url, headers={"User-Agent": user_agent()}, proxy=proxy
            ) as response:
                if response.status == 404:
                    pbar.set_description("404")
                    return
                if response.status == 403:
                    pbar.set_description("403")
                    return
                result = await response.text()
                pbar.update(1)
                item = None
                try:
                    item = extract_rt_data(result)
                except Exception as e:
                    pbar.set_description(f"{url}: {e}")
                if not item:
                    return
                image = item['image']
                year = item["year"]
                tomatometer_score = item["tomatometer_score"]
                critic_ratings = item["critic_ratings"]
                audience_score = item["audience_score"]
                user_ratings = item["user_ratings"]
                certified = item["certified"]
                rt_cursor.execute(
                    """
                    INSERT OR REPLACE INTO seasons VALUES (
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
            pbar.set_description(f"{url}: {e}")

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
    urls = extract_data_from_urls()
    scrape_seasons(urls)
