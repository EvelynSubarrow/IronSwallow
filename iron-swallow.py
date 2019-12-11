#!/usr/bin/env python3

import logging, json, datetime, io, zlib
from time import sleep
from collections import OrderedDict
import lxml.etree as ElementTree

import xmlschema
import stomp

from util import database

fh = logging.FileHandler('logs/swallow.log')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh.setLevel(logging.DEBUG)

log = logging.getLogger("IronSwallow")
log.setLevel(logging.DEBUG)
log.propagate = False

format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", '%Y-%m-%dT%H:%M:%S%z')
for handler in (ch,fh):
    handler.setFormatter(format)
    log.addHandler(handler)

SCHEMA = xmlschema.XMLSchema("ppv16/rttiPPTSchema_v16.xsd")

with open("secret.json") as f:
    SECRET = json.load(f)

def strip_message(obj, l=0):
    out = obj
    if type(obj) in (dict,OrderedDict):
        out = type(obj)()
        for key,value in obj.items():
            new_key = key.split(":")[-1].lstrip("@")

            if l==0 and new_key.startswith("ns") or new_key.startswith("xmlns"):
                pass
            elif type(value) in (list, dict, OrderedDict):
                out[new_key] = strip_message(value, l+1)
            else:
                out[new_key] = value
    elif type(obj) in (list,):
        out = type(obj)()
        for item in obj:
            if type(item) in (list, dict, OrderedDict):
                out.append(strip_message(item, l+1))
            else:
                out.append(item)

    return out

def connect_and_subscribe(mq):
    for n in range(1,31):
        try:
            log.info("Connecting... (attempt %s)" % n)
            mq.start()
            mq.connect(**{
                "username": SECRET["username"],
                "passcode": SECRET["password"],
                "wait": True,
                "client-id": SECRET["username"],
                })
            mq.subscribe(**{
                "destination": SECRET["subscribe"],
                "id": 1,
                "ack": "client-individual",
                "activemq.subscriptionName": SECRET["identifier"],
                })
            log.info("Connected!")
            return
        except:
            backoff = min(n**2, 600)
            log.error("Failed to connect, waiting {}s".format(backoff))
            sleep(backoff)
    log.error("Connection attempts exhausted")

class Listener(stomp.ConnectionListener):
    def __init__(self, mq, cursor):
        self._mq = mq
        self.cursor = cursor

    def on_message(self, headers, message):
        c = self.cursor

        message = zlib.decompress(message, zlib.MAX_WBITS | 32)

        tree = ElementTree.fromstring(message)

        parsed = SCHEMA.to_dict(tree)
        parsed = strip_message(parsed)

        if headers["MessageType"]=="SC":
            for schedule in parsed["uR"].get("schedule", []):
                c.execute("""INSERT INTO darwin_schedules VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (rid) DO UPDATE SET
                    signalling_id=EXCLUDED.signalling_id, status=EXCLUDED.status, category=EXCLUDED.category,
                    operator=EXCLUDED.operator, is_active=EXCLUDED.is_active, is_charter=EXCLUDED.is_charter,
                    is_deleted=EXCLUDED.is_deleted, is_passenger=EXCLUDED.is_passenger; COMMIT;""", (
                    schedule["uid"], schedule["rid"], schedule["ssd"], schedule["trainId"], schedule["status"],
                    schedule["trainCat"], schedule["toc"], schedule["isActive"],
                    schedule["isCharter"], schedule["deleted"], schedule["isPassengerSvc"],
                    ))

        self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_heartbeat_timeout(self):
        log.error("Heartbeat timeout")
        self._mq.set_listener("iron-swallow", self)
        connect_and_subscribe(self._mq)

    def on_disconnected(self):
        log.error("Disconnected")

mq = stomp.Connection([(SECRET["hostname"], 61613)],
    keepalive=True, auto_decode=False, heartbeats=(10000, 10000))

with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as cursor:
    mq.set_listener('iron-swallow', Listener(mq, cursor))
    connect_and_subscribe(mq)

    while True:
        sleep(1)
