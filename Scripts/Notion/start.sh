#!/bin/bash
gunicorn webhook_server:app --bind 0.0.0.0:$PORT
