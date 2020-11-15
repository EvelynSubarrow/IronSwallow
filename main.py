#!/usr/bin/env python3

import logging, json, datetime, io, zlib, gzip, multiprocessing, ftplib, tempfile, re
from time import sleep
from decimal import Decimal
from collections import OrderedDict
from typing import List

import boto3
import psycopg2
import psycopg2.extras
import stomp

from ironswallow.util import database, query
from ironswallow.darwin import parse
import ironswallow.store

from IronSwallowORM import models

LOCATIONS = {}
REASONS = {}


def incorporate_reference_data(c) -> None:
    ironswallow.store.reference.store(c, retrieve_reference_data(c))


def retrieve_reference_data(c) -> List[dict]:
    client = boto3.client('s3', aws_access_key_id=SECRET["s3-access"], aws_secret_access_key=SECRET["s3-secret"])
    obj_list = client.list_objects(Bucket="darwin.xmltimetable")["Contents"]
    obj_list = [a for a in obj_list if "ref" in a["Key"]]
    stream = client.get_object(Bucket="darwin.xmltimetable", Key=obj_list[-1]["Key"])["Body"]
    parsed = parse.parse_xml(gzip.decompress(stream.read()))

    return parsed


def incorporate_ftp(c) -> None:
    ftp = ftplib.FTP(SECRET["ftp-hostname"])
    for n in range(1,31):
        try:
            log.info("FTP Connecting... (attempt %s)" % n)
            ftp.connect()
            log.info("FTP Connected")

            read_buffer = bytearray()
            file_list = []
            actual_files = []

            ftp.login(SECRET["ftp-username"], SECRET["ftp-password"])

            ftp.retrlines("NLST snapshot", file_list.append)
            ftp.retrlines("NLST pushport", file_list.append)

            for r_filename in file_list:
                temp_file = tempfile.TemporaryFile()
                log.info("FTP retrieving {}".format(r_filename))
                ftp.retrbinary("RETR {}".format(r_filename), temp_file.write)
                temp_file.seek(0)
                actual_files.append((r_filename, temp_file))

            log.info("Purging database")
            c.execute("BEGIN;")
            c.execute("ALTER TABLE darwin_schedules DISABLE TRIGGER USER;")
            c.execute("TRUNCATE TABLE darwin_schedule_locations,darwin_schedule_status,darwin_associations,darwin_schedules,darwin_messages;")
            c.execute("ALTER TABLE darwin_schedules ENABLE TRIGGER USER;")

            with multiprocessing.Pool(8) as pool:
                while actual_files:
                    file_name, file = actual_files[0]
                    log.info("Enqueueing retrieved file {}".format(file_name))

                    # A little bit messy, here the idea is to capture exceptions in the map, but not the storage
                    # Because those issues tend to be ones which abort the transaction
                    e2 = None
                    try:
                        for idx,result in pool.imap(parse.parse_darwin_suppress, enumerate(gzip.open(file))):
                            try:
                                if type(result) == str:
                                    logging.error("FTP message parse failed (line {})".format(idx))
                                    logging.error(result)
                                else:
                                    ironswallow.store.darwin.store_message(c, result)
                            except Exception as e2:
                                log.exception(e2)
                                raise e2
                    except Exception as e1:
                        if e2: raise e2
                        log.exception(e1)

                    file.close()
                    del actual_files[0]

            c.execute("COMMIT;")
            return
        except ftplib.Error as e:
            backoff = min(n**2, 600)
            log.error("FTP failed to connect, waiting {}s".format(backoff))
            sleep(backoff)
    log.error("FTP connection attempts exhausted")

def connect_and_subscribe(mq):
    for n in range(1,31):
        try:
            log.info("Connecting... (attempt %s)" % n)
            #mq.start()
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
        except Exception as e:
            backoff = min(n**2, 600)
            log.error("Failed to connect, waiting {}s".format(backoff))
            log.exception(e)
            sleep(backoff)
    log.error("Connection attempts exhausted")


class Listener(stomp.ConnectionListener):
    def __init__(self, mq, cursor):
        self._mq = mq
        self.cursor = cursor

    def on_message(self, headers, message):
        try:
            c = self.cursor
            c.execute("BEGIN;")
    
            c.execute("SELECT * FROM last_received_sequence;")
            row = c.fetchone()
            if row and ((row[1]+5)%10000000)<=int(headers["SequenceNumber"]) < 10000000-5:
                log.error("Skipped sequence count exceeds limit ({}->{})".format(row[1], headers["SequenceNumber"]))
    
            message = zlib.decompress(message, zlib.MAX_WBITS | 32)
    
            try:
                ironswallow.store.darwin.store_message(self.cursor, parse.parse_darwin(message))
            except Exception as e:
                log.exception(e)
            self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])
    
            c.execute("""INSERT INTO last_received_sequence VALUES (0, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET sequence=EXCLUDED.sequence, time_acquired=EXCLUDED.time_acquired;""", (
                headers["SequenceNumber"], datetime.datetime.utcnow()))
    
            c.execute("COMMIT;")
        except Exception as e:
            log.exception(e)
        
    def on_error(self, headers, message):
        log.error('received an error "%s"' % message)

    def on_heartbeat_timeout(self):
        log.error("Heartbeat timeout")
        self._mq.set_listener("iron-swallow", self)
        connect_and_subscribe(self._mq)

    def on_disconnected(self):
        log.error("Disconnected")

if __name__ == "__main__":
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

    with open("secret.json") as f:
        SECRET = json.load(f)

    with database.DatabaseConnection() as db_connection:
        models.create_all(db_connection.engine)

    mq = stomp.Connection([(SECRET["hostname"], 61613)],
        keepalive=True, auto_decode=False, heartbeats=(10000, 10000))

    with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as cursor:
        incorporate_reference_data(cursor)

        last_retrieved = query.last_retrieved(cursor)
        if not last_retrieved or (datetime.datetime.utcnow()-last_retrieved).seconds > 300:
            log.info("Last retrieval too old, using FTP snapshots")
            incorporate_ftp(cursor)

        if not SECRET.get("no_listen_stomp"):
            mq.set_listener('iron-swallow', Listener(mq, cursor))
            connect_and_subscribe(mq)

        while True:
            with db_connection.new_cursor() as c2:
                ironswallow.store.meta.renew_schedule_meta(c2)
            sleep(3600*12)
            with db_connection.new_cursor() as c3:
                incorporate_reference_data(c3)
