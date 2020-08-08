#!/bin/sh
CMD_ARGS=$( python get_gunicorn_args.py 2>&1)
GUNICORN_CMD_ARGS="$CMD_ARGS" gunicorn app:server
