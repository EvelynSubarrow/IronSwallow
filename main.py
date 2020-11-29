#!/usr/bin/env python3

import logging, json, datetime, zlib, gzip, multiprocessing, ftplib, tempfile
from time import sleep
from typing import List

import boto3
import stomp

from ironswallow.util import database, query
from ironswallow.darwin import parse
import ironswallow.store
import ironswallow.bplan

from IronSwallowORM import models

LOCATIONS = {}
REASONS = {}


def incorporate_reference_data(c) -> None:
    ironswallow.store.reference.insert.store(c, retrieve_reference_data(c))


def retrieve_reference_data(c) -> List[dict]:
    client = boto3.client('s3', aws_access_key_id=SECRET["s3-access"], aws_secret_access_key=SECRET["s3-secret"])
    obj_list = client.list_objects(Bucket="darwin.xmltimetable")["Contents"]
    obj_list = [a for a in obj_list if "ref" in a["Key"]]
    stream = client.get_object(Bucket="darwin.xmltimetable", Key=obj_list[-1]["Key"])["Body"]
    parsed = parse.parse_xml(gzip.decompress(stream.read()))

    return parsed


def incorporate_ftp(mp) -> None:
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
                if SECRET.get("ftp_snapshot_base_snapshot_only", False):
                    break

            log.info("Purging database")
            mp.execute("BEGIN;")
            mp.execute("ALTER TABLE darwin_schedules DISABLE TRIGGER USER;")
            mp.execute("TRUNCATE TABLE darwin_schedule_locations,darwin_schedule_status,darwin_associations,darwin_schedules,darwin_messages;")
            mp.execute("ALTER TABLE darwin_schedules ENABLE TRIGGER USER;")

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
                                    mp.store(result)
                            except Exception as e2:
                                log.exception(e2)
                                raise e2
                    except Exception as e1:
                        if e2: raise e2
                        log.exception(e1)

                    file.close()
                    del actual_files[0]

            mp.execute("COMMIT;")
            return
        except ftplib.Error as e:
            backoff = min(n**2, 600)
            log.error("FTP failed to connect, waiting {}s".format(backoff))
            sleep(backoff)
    log.error("FTP connection attempts exhausted")


class Listener(stomp.ConnectionListener):
    def __init__(self, mp):
        self.processor = mp
        self._mq: stomp.Connection = None
        self.disconnected = True
        self._attempting_connection = False
        self._connection_attempts = 0
        self._connection_attempts_total = 0
        self._last_connection_attempt = datetime.datetime(1989, 1, 1, 1, 1, 1)

    def on_message(self, headers, message):
        try:
            self.processor.execute("BEGIN;")

            message = zlib.decompress(message, zlib.MAX_WBITS | 32)
    
            try:
                self.processor.store(parse.parse_darwin(message))
            except Exception as e:
                log.exception(e)
            self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])
    
            self.processor.execute("""INSERT INTO last_received_sequence VALUES (0, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET sequence=EXCLUDED.sequence, time_acquired=EXCLUDED.time_acquired;""", (
                headers["SequenceNumber"], datetime.datetime.utcnow()))
    
            self.processor.execute("COMMIT;")
        except Exception as e:
            log.exception(e)
        
    def on_error(self, headers, message: bytes):
        log.error('received an error "%s"' % message.split(b"\n")[0])

    def on_heartbeat_timeout(self):
        log.error("Heartbeat timeout")

    def on_disconnected(self):
        log.error("Disconnected")
        if not self._attempting_connection:
            self.disconnected = True

    def connect_and_subscribe(self):
        if not self.disconnected and not self._attempting_connection:
            return

        self.disconnected = False
        self._connection_attempts = 0
        self._attempting_connection = True

        if (datetime.datetime.utcnow()-self._last_connection_attempt).seconds < 10:
            log.info("Last connection attempt less than 10s ago, delaying")
            sleep(10)


        mq = stomp.Connection([(SECRET["hostname"], 61613)],
                              keepalive=True, auto_decode=False, heartbeats=(35000, 35000))
        self._mq = mq
        mq.set_listener("iron-swallow", self)

        for n in range(1, 31):
            self._connection_attempts = n
            self._connection_attempts_total += 1
            self._last_connection_attempt = datetime.datetime.utcnow()
            try:
                log.info("Connecting... (attempt %s)" % n)
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
                self._attempting_connection = False
                return
            except Exception as e:
                backoff = max(min(n ** 2, 600), 10)
                log.error("Failed to connect, waiting {}s".format(backoff))
                log.exception(e)
                sleep(backoff)
        log.error("Connection attempts exhausted")
        self.disconnected = True
        self._attempting_connection = False

    def is_disconnected(self):
        # Check the STOMP object itself
        # Check our semaphore
        # Check the number of connection attempts. If it's 0 we never tried so we're not """disconnected""" from the
        # main thread's pov
        # check time elapsed between now and last connection attempt is greater than 5s
        return (self._mq and not self._mq.is_connected() and self.disconnected and self._connection_attempts and
                (datetime.datetime.utcnow() - self._last_connection_attempt).seconds > 5)


    def is_before_first_connection(self):
        return not self._mq and self.disconnected and self._connection_attempts_total == 0


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


    with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as cursor:
        ironswallow.bplan.parse_store_bplan()
        incorporate_reference_data(cursor)

        last_retrieved = query.last_retrieved(cursor)

        with ironswallow.store.darwin.MessageProcessor(cursor) as mp:
            if (not last_retrieved or (datetime.datetime.utcnow()-last_retrieved).seconds > 300) and not SECRET.get("no_from_ftp"):
                log.info("Last retrieval too old, using FTP snapshots")
                incorporate_ftp(mp)

            while mp.count() > 100:
                log.info(f"Waiting for database queue ({mp.count()}) to empty below limit")
                sleep(10)

            listener = None
            if not SECRET.get("no_listen_stomp"):
                listener = Listener(mp)

            tick = 0
            while True:
                tick += 1

                if listener and (listener.is_disconnected() or listener.is_before_first_connection()):
                    listener.connect_and_subscribe()

                if tick % 3600 == 0:
                    with db_connection.new_cursor() as c3:
                        incorporate_reference_data(c3)

                if tick % 3600 == 1:
                    with db_connection.new_cursor() as c2:
                        ironswallow.store.meta.renew_schedule_meta(c2)

                if tick % 30 == 0:
                    if mp.count() > 500:
                        log.info(f"Database queue count ({mp.count()}) over limit.")

                sleep(1)
