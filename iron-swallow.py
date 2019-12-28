#!/usr/bin/env python3

import logging, json, datetime, io, zlib, gzip
from ftplib import FTP
from time import sleep
from decimal import Decimal
from collections import OrderedDict

import psycopg2
import stomp

from util import database
from util import pushport

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

def compare_time(t1, t2):
    if not (t1 and t2):
        return 0
    t1,t2 = [a.hour*3600+a.minute*60+a.second for a in (t1,t2)]
    return (Decimal(t1)-Decimal(t2))/3600

def process_time(time):
    if not time:
        return None
    if len(time)==5:
        time += ":00"
    return datetime.datetime.strptime(time, "%H:%M:%S").time()

def form_original_wt(times):
    out = ""
    for time in times:
        if time:
            out += time.strftime("%H%M%S")
        else:
            out += "      "
    return out

def incorporate_ftp(c):
    ftp = FTP(SECRET["ftp-hostname"])
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

            for file in file_list:
                log.info("FTP retrieving {}".format(file))
                read_buffer = bytearray()
                ftp.retrbinary("RETR {}".format(file), read_buffer.extend)
                actual_files.append((file, read_buffer))

            while actual_files:
                file, contents = actual_files[0]
                log.info("Enqueueing retrieved file {}".format(file))
                lines = gzip.decompress(contents).split(b"\n")
                for line in lines:
                    if line:
                        parse(c, line)
                del lines
                del actual_files[0]

            return
        except AssertionError as e:
            backoff = min(n**2, 600)
            log.error("FTP failed to connect, waiting {}s".format(backoff))
            sleep(backoff)
    log.error("FTP connection attempts exhausted")


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

def parse(cursor, message):
    c = cursor

    parsed = pushport.PushPortParser().parse(io.StringIO(message.decode("utf8")))["Pport"].get("uR", {})

    for record in parsed.get("list", []):
        if record["tag"]=="schedule":
            c.execute("""INSERT INTO darwin_schedules VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (rid) DO UPDATE SET
                signalling_id=EXCLUDED.signalling_id, status=EXCLUDED.status, category=EXCLUDED.category,
                operator=EXCLUDED.operator, is_active=EXCLUDED.is_active, is_charter=EXCLUDED.is_charter,
                is_deleted=EXCLUDED.is_deleted, is_passenger=EXCLUDED.is_passenger;""", (
                record["uid"], record["rid"], record.get("rsid"), record["ssd"], record["trainId"],
                record.get("status") or "P", record.get("trainCat") or "OO", record["toc"], record.get("isActive") or True,
                bool(record.get("isCharter")), bool(record.get("deleted")), record.get("isPassengerSvc") or True,
                ))

            index = 0
            last_time, ssd_offset = None, 0

            c.execute("DELETE FROM darwin_schedule_locations WHERE rid=%s;", (record["rid"],))

            for location in record["list"]:
                if location["tag"] in ["OPOR", "OR", "OPIP", "IP", "PP", "DT", "OPDT"]:

                    times = []
                    for time_n, time in [(a, location.get(a, None)) for a in ["pta", "wta", "wtp", "ptd", "wtd"]]:
                        if time:
                            if len(time)==5:
                                time += ":00"
                            time = datetime.datetime.strptime(time, "%H:%M:%S").time()

                            # Crossed midnight, increment ssd offset
                            if compare_time(time, last_time) < -6:
                                ssd_offset += 1
                            # Normal increase or decrease, nothing we really need to do here
                            elif -6 <= compare_time(time, last_time) <= +18:
                                pass
                            # Back in time, crossed midnight (in reverse), decrement ssd offset
                            elif +18 < compare_time(time, last_time):
                                ssd_offset -= 1

                            last_time = time
                            time = datetime.datetime.combine(datetime.datetime.strptime(record["ssd"], "%Y-%m-%d").date(), time) + datetime.timedelta(days=ssd_offset)
                        times.append(time)

                    original_wt = form_original_wt([process_time(location.get(a)) for a in ("wta", "wtp", "wtd")])

                    c.execute("""INSERT INTO darwin_schedule_locations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""",
                        (record["rid"], index, location["tag"], location["tpl"], location.get("act", ''), original_wt, *times, bool(location.get("can")), location.get("rdelay", 0)))

                    index += 1

        if record["tag"]=="TS":
            for location in record["list"]:
                original_wt = form_original_wt([process_time(location.get(a)) for a in ("wta", "wtp", "wtd")])
                if location["tag"]=="location":
                    times = []
                    times_source = []
                    times_type = []
                    times_delay = []

                    for i, time_d in enumerate([location.get(a, {}) for a in ["arr", "pass", "dep"]]):
                        time_content = time_d.get("at",None) or time_d.get("et",None)
                        time = None
                        if time_content:
                            time = process_time(time_content)

                        times.append(time)
                        times_source.append(time_d.get("src"))
                        times_type.append("E"*("et" in time_d) or "A"*("at" in time_d) or None)
                        times_delay.append(bool(time_d.get("delayed")))

                    plat = location.get("plat", {})
                    c.execute("INSERT INTO darwin_schedule_status VALUES (%s,%s,%s,  %s,%s,%s,  %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;", (
                        record["rid"], location["tpl"], original_wt, *times, *times_source, *times_type, *times_delay,
                        plat.get("$"), bool(plat.get("platsup")), bool(plat.get("cisPlatsup")), bool(plat.get("conf")), bool(plat.get("platsrc"))))

class Listener(stomp.ConnectionListener):
    def __init__(self, mq, cursor):
        self._mq = mq
        self.cursor = cursor

    def on_message(self, headers, message):
        c = self.cursor
        c.execute("BEGIN;")

        c.execute("SELECT * FROM last_received_sequence;")
        row = c.fetchone()
        if row and ((row[1]+5)%10000000)<=int(headers["SequenceNumber"]) < 10000000-5:
            log.error("Skipped sequence count exceeds limit ({}->{})".format(row[1], headers["SequenceNumber"]))

        message = zlib.decompress(message, zlib.MAX_WBITS | 32)

        parse(self.cursor, message)
        self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])

        c.execute("""INSERT INTO last_received_sequence VALUES (0, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET sequence=EXCLUDED.sequence, time_acquired=EXCLUDED.time_acquired;""", (
            headers["SequenceNumber"], datetime.datetime.utcnow()))

        c.execute("COMMIT;")

    def on_error(self, headers, message):
        log.error('received an error "%s"' % message)

    def on_heartbeat_timeout(self):
        log.error("Heartbeat timeout")
        self._mq.set_listener("iron-swallow", self)
        connect_and_subscribe(self._mq)

    def on_disconnected(self):
        log.error("Disconnected")

mq = stomp.Connection([(SECRET["hostname"], 61613)],
    keepalive=True, auto_decode=False, heartbeats=(10000, 10000))

with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as cursor:
    cursor.execute("SELECT * FROM last_received_sequence;")
    row = cursor.fetchone()
    if not row or (datetime.datetime.utcnow()-row[2]).seconds > 300:
        log.info("Last retrieval too old, using FTP snapshots")
        incorporate_ftp(cursor)

    mq.set_listener('iron-swallow', Listener(mq, cursor))
    connect_and_subscribe(mq)

    while True:
        sleep(1)
