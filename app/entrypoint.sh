#!/bin/bash
gunicorn --limit-request-line 10000 -w 8 --bind 0.0.0.0:81 app:app --access-logfile -
