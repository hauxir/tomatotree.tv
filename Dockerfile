FROM python:3.9-slim

RUN pip install flask aiohttp requests beautifulsoup4 tqdm gunicorn Flask-Caching random_user_agent

COPY app app/

WORKDIR /app

CMD bash entrypoint.sh
