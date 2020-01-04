#!/usr/bin/env python3

import logging, json, datetime, io, zlib, gzip
from collections import OrderedDict

import psycopg2

import flask
from flask import Response
from flask import request

from util import config
from util import database
from util import query

app = flask.Flask(__name__)
_web_db = None

class UnauthenticatedException(Exception): pass

def error_page(code, message):
    return flask.render_template('error.html', messages=["{0} - {1}".format(code, message)]), code

def format_time(dt, part):
    if part=="w":
        dt = dt.get("working")
    elif part==".":
        dt = dt.get("estimated") or dt.get("actual")
    else:
        raise ValueError()

    if not dt:
        return ""
    else:
        return dt.strftime("%H%M") + "½"*(dt.second==30)

@app.route('/')
def index():
    return flask.render_template('index.html')

@app.route('/style')
def style():
    return app.send_static_file('style.css')

@app.route('/swallow')
def swallow():
    return app.send_static_file('swallow.svg')

@app.route('/json/departures/<location>', defaults={"time": "now"})
@app.route('/json/departures/<location>/<time>')
def json_departures(location, time):
    failure_message = None
    status = 200
    try:
        if not location.isalnum(): raise ValueError
        if time=="now":
            time = datetime.datetime.now()
        else:
            time = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")

        with get_cursor() as c:
            struct=query.station_board(c, (location,), time)
        return Response(json.dumps(struct, indent=2, default=query.json_default), mimetype="application/json", status=status)
    except ValueError as e:
        status, failure_message = 400, "Location codes must be alphanumeric, and the only permitted time is 'now'... for now"
    except ValueError as e:
        if not failure_message:
            status, failure_message = 500, "Unhandled exception"
    return Response(json.dumps({"success": False, "message":failure_message}, indent=2), mimetype="application/json", status=status)

@app.route('/departures/<location>', defaults={"time": "now"})
@app.route('/departures/<location>/<time>')
@app.route('/d/<location>', defaults={"time": "now"})
@app.route('/d/<location>/<time>')
def html_location(location, time):
    try:
        if not location.isalnum(): raise ValueError

        notes = []

        if time=="now":
            time = datetime.datetime.now()
            notes.append("Departures are for time of request")
        else:
            time = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")

        with get_cursor() as c:
            services = query.station_board(c, (location,), time, limit=50)

    except ValueError as e:
        return error_page(400, "Location names must be alphanumeric, datestamp must be either ISO 8601 format (YYYY-MM-DDThh:mm:ss) or 'now'")
    except UnauthenticatedException as e:
        return error_page(403, "Unauthenticated")
    except Exception as e:
        return error_page(500, "Unhandled exception")
    return Response(
        flask.render_template("location.html", services=services, time=time, location=location, message=None, notes=notes, format_time=format_time),
        status=200,
        mimetype="text/html"
        )

def get_cursor():
    global _web_db

    if not _web_db:
        _web_db = database.DatabaseConnection()
        _web_db.connect()
    return _web_db.new_cursor()

if __name__ == "__main__":
    app.logger.setLevel(logging.ERROR)

    with open("secret.json") as f:
        SECRET = json.load(f)

    app.run(
        config.get("flask-host", "127.0.0.1"),
        config.get("flask-port", 36323),
        config.get("flask-debug", False),
        ssl_context=None)
