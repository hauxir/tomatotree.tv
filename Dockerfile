FROM python:3.9-slim

RUN pip install flask aiohttp requests beautifulsoup4 tqdm

COPY app app/

WORKDIR /app

CMD python app.py
