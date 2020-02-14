#!/usr/bin/env python3

import logging, json, datetime, io, zlib, gzip
from collections import OrderedDict

import psycopg2

import flask
from flask import Response
from flask import request

from ironswallow.util import config
from ironswallow.util import database
from ironswallow.util import query

app = flask.Flask(__name__)
_web_db = None

class UnauthenticatedException(Exception): pass

def error_page(code, message):
    return flask.render_template('error.html', messages=["{0} - {1}".format(code, message)]), code

def format_time(dt, part):
    short = {"a": "arrival", "p": "pass", "d": "departure"}
    suffix = ""
    prefix = ""

    dt = dt["times"][short[part[0]]]

    if part[1]=="w":
        dt = dt.get("working")
        prefix += "s"
    elif part[1]==".":
        suffix += "."*bool(dt.get("actual")) or "~"
        dt = dt.get("estimated") or dt.get("actual")
    else:
        raise ValueError()

    if not dt:
        return ""
    else:
        return prefix + dt.strftime("%H%M") + "½"*(dt.second==30) + suffix

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
@app.route('/j/d/<location>', defaults={"time": "now"})
@app.route('/j/d/<location>/<time>')
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
            response = query.station_board(c, (location,), time, period=500)

        if response:
            return Response(json.dumps(response, indent=2, default=query.json_default), mimetype="application/json", status=status)
        else:
            status, failure_message = 404, "Location(s) not found"
    except ValueError as e:
        status, failure_message = 400, "Location codes must be alphanumeric, and the only permitted time is 'now'... for now"
    except ValueError as e:
        if not failure_message:
            status, failure_message = 500, "Unhandled exception"
    return Response(json.dumps({"status": status, "message":failure_message}, indent=2), mimetype="application/json", status=status)

@app.route('/json/service/<id>', defaults={"date": None})
@app.route('/json/service/<id>/<date>')
@app.route('/j/s/<id>', defaults={"date": None})
@app.route('/j/s/<id>/<date>')
def json_service(id, date):
    failure_message = None
    status = 200
    try:
        if not id.isalnum(): raise ValueError
        if date in ["now", "today"]:
            date = datetime.datetime.now().date()
        elif date!=None:
            date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        with get_cursor() as c:
            response = query.service(c, id, date)

        if response:
            return Response(json.dumps(response, indent=2, default=query.json_default), mimetype="application/json", status=status)
        else:
            status,failure_message = 404, "Schedule not found"
    except ValueError as e:
        status, failure_message = 400, "/<rid> requires a valid RID, /<uid>/<date> requires a valid UID, and a ISO 8601 date, or 'now'"
    except Exception as e:
        if not failure_message:
            status, failure_message = 500, "Unhandled exception"
    return Response(json.dumps({"status": status, "message":failure_message}, indent=2), mimetype="application/json", status=status)

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
            last_retrieved = query.last_retrieved(c)
            if not last_retrieved or (datetime.datetime.now()-last_retrieved).seconds > 300:
                notes.append("Last Darwin message was parsed more than five minutes ago, information is likely out of date.")

            board = query.station_board(c, (location,), time, limit=50)
            if not board:
                return error_page(404, "No such location code is known")

    except ValueError as e:
        return error_page(400, "Location names must be alphanumeric, datestamp must be either ISO 8601 format (YYYY-MM-DDThh:mm:ss) or 'now'")
    except UnauthenticatedException as e:
        return error_page(403, "Unauthenticated")
    except Exception as e:
        return error_page(500, "Unhandled exception")
    return Response(
        flask.render_template("location.html", board=board, time=time, location=location, message=None, notes=notes, format_time=format_time),
        status=200,
        mimetype="text/html"
        )

@app.route('/service/<id>', defaults={"date": None})
@app.route('/service/<id>/<date>')
@app.route('/s/<id>', defaults={"date": None})
@app.route('/s/<id>/<date>')
def html_service(id, date):
    try:
        if not id.isalnum(): raise ValueError

        notes = []

        if date in ["now", "today"]:
            date = datetime.datetime.now().date()
        elif date!=None:
            date = datetime.datetime.strptime(date, "%Y-%m-%d").date()

        with get_cursor() as c:
            last_retrieved = query.last_retrieved(c)
            if not last_retrieved or (datetime.datetime.now()-last_retrieved).seconds > 300:
                notes.append("Last Darwin message was parsed more than five minutes ago, information is likely out of date.")

            schedule=query.service(c, id, date)

            if not schedule:
                return error_page(404, "No such service is known")

    except ValueError as e:
        return error_page(400, "/<rid> requires a valid RID, /<uid>/<date> requires a valid UID, and a ISO 8601 date, or 'now'")
    except UnauthenticatedException as e:
        return error_page(403, "Unauthenticated")
    except Exception as e:
        return error_page(500, "Unhandled exception")
    return Response(
        flask.render_template("schedule.html", schedule=schedule, date=date, message=None, notes=notes, format_time=format_time),
        status=200,
        mimetype="text/html"
        )

@app.route("/redirect/schedule")
def redirect_schedule():
    uid = request.args.get("uid", '')
    date = request.args.get("date", '')
    return flask.redirect(flask.url_for("html_service", id=uid, date=date))

@app.route("/redirect/location")
def redirect_location():
    code = request.args.get("code", '')
    time = request.args.get("time", '')
    return flask.redirect(flask.url_for("html_location", location=code, time=time))

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

    try:
        __import__("_web").init(app)
    except ImportError as e:
        pass

    app.run(
        config.get("flask-host", "127.0.0.1"),
        config.get("flask-port", 36323),
        config.get("flask-debug", False),
        ssl_context=None)
