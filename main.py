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

LOCATIONS = {}
REASONS = {}

def compare_time(t1, t2) -> int:
    if not (t1 and t2):
        return 0
    t1,t2 = [a.hour*3600+a.minute*60+a.second for a in (t1,t2)]
    return (Decimal(t1)-Decimal(t2))/3600

def process_time(time) -> datetime.time:
    if not time:
        return None
    if len(time)==5:
        time += ":00"
    return datetime.datetime.strptime(time, "%H:%M:%S").time()

def full_original_wt(location) -> None:
    return form_original_wt([process_time(location.get(a)) for a in ("wta", "wtp", "wtd")])

def form_original_wt(times) -> str:
    out = ""
    for time in times:
        if time:
            out += time.strftime("%H%M%S")
        else:
            out += "      "
    return out

def incorporate_reference_data(c) -> None:
    store_reference_data(c, retrieve_reference_data(c))

def retrieve_reference_data(c) -> List[dict]:
    client = boto3.client('s3', aws_access_key_id=SECRET["s3-access"], aws_secret_access_key=SECRET["s3-secret"])
    obj_list = client.list_objects(Bucket="darwin.xmltimetable")["Contents"]
    obj_list = [a for a in obj_list if "ref" in a["Key"]]
    stream = client.get_object(Bucket="darwin.xmltimetable", Key=obj_list[-1]["Key"])["Body"]
    parsed = parse.parse_xml(gzip.decompress(stream.read()))

    return parsed

def store_reference_data(c, parsed) -> None:
    strip = lambda x: x.rstrip() or None if x else None
    case = lambda x: x.title() if x else x

    with open("datasets/corpus.json", encoding="iso-8859-1") as f:
        corpus = json.load(f)["TIPLOCDATA"]
    corpus = {a["TIPLOC"]: a for a in corpus}

    for reference in parsed["PportTimetableRef"]["list"]:
        if reference["tag"]=="LocationRef":
            corpus_loc = corpus.get(reference["tpl"], {})

            loc = OrderedDict([
                ("tiploc", reference["tpl"]),
                ("crs_darwin", reference.get("crs")),
                ("crs_corpus", strip(corpus_loc.get("3ALPHA"))),
                ("operator", reference.get("toc")),
                ("name_darwin", reference["locname"]*(reference["locname"]!=reference["tpl"]) or None),
                ("name_corpus", case(strip(corpus_loc.get("NLCDESC")))),
                ])
            loc.update(OrderedDict([
                ("name_short",loc["name_darwin"] or loc["name_corpus"]),
                ("name_full",loc["name_corpus"] or loc["name_darwin"]),
                ]))

            c.execute("""INSERT INTO darwin_locations VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(tiploc) DO UPDATE SET
                (tiploc,crs_darwin,crs_corpus,operator,name_short,name_full,dict)=
                (EXCLUDED.tiploc,EXCLUDED.crs_darwin,EXCLUDED.crs_corpus,EXCLUDED.operator,
                EXCLUDED.name_short,EXCLUDED.name_full,EXCLUDED.dict);
                """, (loc["tiploc"], loc["crs_darwin"], loc["crs_corpus"], loc["operator"],
                    loc["name_short"], loc["name_full"],
                    json.dumps(loc)))

            LOCATIONS[reference["tpl"]] = loc

        if reference["tag"]=="TocRef":
            c.execute("""INSERT INTO darwin_operators VALUES (%s, %s, %s) ON CONFLICT (operator)
                DO UPDATE SET (operator_name, url)=(EXCLUDED.operator_name, EXCLUDED.url);""",
                (reference["toc"], reference["tocname"], reference.get("url")))

        if reference["tag"] in ["CancellationReasons", "LateRunningReasons"]:
            reason_type = "C"*(reference["tag"]=="CancellationReasons") or "D"
            for reason in reference["list"]:
                if reason["tag"]=="Reason":
                    c.execute("""INSERT INTO darwin_reasons VALUES (%s, %s, %s) ON CONFLICT (id, type) DO UPDATE
                        SET (type, message)=(EXCLUDED.type, EXCLUDED.message);""",
                        (reason["code"], reason_type, reason["reasontext"]))
                    REASONS[(reason["code"], reason_type)] = reason["reasontext"]

    c.execute("COMMIT;")

def renew_schedule_association_meta(c, main_rid=None, assoc_rid=None) -> None:
    if main_rid and assoc_rid:
        c.execute("""SELECT a.category,tiploc,s1.rid,s1.origins,s1.destinations,s2.rid,s2.origins,s2.destinations
            FROM darwin_associations AS a
            INNER JOIN darwin_schedules AS s1 on s1.rid=a.main_rid
            INNER JOIN darwin_schedules AS s2 on s2.rid=a.assoc_rid
            WHERE a.category!='NP' AND main_rid=%s AND assoc_rid=%s;""", (main_rid, assoc_rid))
    else:
        c.execute("""SELECT a.category,tiploc,s1.rid,s1.origins,s1.destinations,s2.rid,s2.origins,s2.destinations
            FROM darwin_associations AS a
            INNER JOIN darwin_schedules AS s1 on s1.rid=a.main_rid
            INNER JOIN darwin_schedules AS s2 on s2.rid=a.assoc_rid
            WHERE a.category!='NP';""")

    for row in c.fetchall():
        row = list(row)[::-1]
        row = OrderedDict([(a, row.pop()) for a in ("category", "tiploc", "main_rid", "main_origins", "main_destinations", "assoc_rid", "assoc_origins", "assoc_destinations")])

        for location in row["assoc_destinations"]:
            location["association_tiploc"] = row["tiploc"]
            location["source"] = row["category"]

        for location in row["main_origins"]:
            location["association_tiploc"] = row["tiploc"]
            location["source"] = row["category"]

        row["main_origins"] = [json.dumps(a) for a in row["main_origins"]]
        row["assoc_destinations"] = [json.dumps(a) for a in row["assoc_destinations"]]

        if not any([a.get("association_tiploc")==row["tiploc"] and a["source"]==row["category"] for a in row["main_destinations"]]):
            c.execute("""UPDATE darwin_schedules SET (destinations)=(darwin_schedules.destinations || %s::json[]) WHERE rid=%s;""", (row["assoc_destinations"], row["main_rid"]))

        if not any([a.get("association_tiploc")==row["tiploc"] and a["source"]==row["category"] for a in row["assoc_origins"]]):
            c.execute("""UPDATE darwin_schedules SET (origins)=(darwin_schedules.origins || %s::json[]) WHERE rid=%s;""", (row["main_origins"],row["assoc_rid"]))

def renew_schedule_meta(c) -> None:
    log.info("Computing origin/destination lists for schedules")

    crid = None
    origins = []
    destinations = []
    batch = []

    c.execute("""SELECT type,activity,cancelled,loc.rid,tiploc FROM darwin_schedule_locations as loc
        INNER JOIN darwin_schedules AS s ON s.rid=loc.rid
        WHERE type='OR' OR type='OPOR' OR type='DT' OR type='OPDT' ORDER BY rid DESC, index ASC;""")

    for i,row in enumerate(c.fetchall()):
        row = list(row)[::-1]
        row = OrderedDict([(a, row.pop()) for a in ("type", "activity", "canc", "rid", "tiploc")])
        if row["rid"]!=crid:
            batch.append((origins, destinations, crid))
            origins,destinations = [],[]

        crid=row["rid"]

        loc_dict = OrderedDict([("source", "SC"), ("type", row["type"]), ("activity", row["activity"]), ("cancelled", row["canc"])])
        loc_dict.update(LOCATIONS[row["tiploc"]])
        loc_dict = query.process_location_outline(loc_dict)

        if row["type"][-2:]=="OR":
            origins.append(json.dumps(loc_dict))
        elif row["type"][-2:]=="DT":
            destinations.append(json.dumps(loc_dict))

        if not i%100:
            psycopg2.extras.execute_batch(c, "UPDATE darwin_schedules SET (origins,destinations)=(%s::json[],%s::json[]) WHERE rid=%s;", batch)
            batch = []

    psycopg2.extras.execute_batch(c, "UPDATE darwin_schedules SET (origins,destinations)=(%s::json[],%s::json[]) WHERE rid=%s;", batch)

    log.info("Precompution of origin/destination lists completed, adding associations")
    renew_schedule_association_meta(c)
    log.info("All origin and destination lists have been completed")

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

                    for result in pool.imap(parse.parse_darwin, gzip.open(file)):
                        try:
                            store_message(c,result)
                        except Exception as e:
                            log.exception(e)

                    file.close()
                    del actual_files[0]

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

def process_reason(reason):
    return OrderedDict([
        ("code", reason["$"]),
        ("message", REASONS[(reason["$"], "C"*(reason["tag"]=="cancelReason") or "D")]),
        ("location", query.process_location_outline(LOCATIONS.get(reason.get("tiploc")))),
        ("near", bool(reason.get("near"))),
        ])

def store_message(cursor, parsed) -> None:
    if not parsed:
        return
    c = cursor

    for record in parsed.get("list", []):
        if record["tag"]=="schedule":

            index = 0
            last_time, ssd_offset = None, 0

            c.execute("DELETE FROM darwin_schedule_locations WHERE rid=%s;", (record["rid"],))

            origins, destinations = [], []
            batch = []

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

                    batch.append((record["rid"], index, location["tag"], location["tpl"], location.get("act", ''), original_wt, *times, bool(location.get("can")), location.get("rdelay", 0)))

                    loc_dict = OrderedDict([("source", "SC"), ("type", location["tag"]), ("activity", location.get("act",'')), ("cancelled", bool(location.get("can")))])
                    loc_dict.update(LOCATIONS[location["tpl"]])

                    if location["tag"] in ("OR", "OPOR"):
                        origins.append(json.dumps(loc_dict))
                    if location["tag"] in ("DT", "OPDT"):
                        destinations.append(json.dumps(loc_dict))

                    index += 1

                elif location["tag"]=="cancelReason":
                    c.execute("UPDATE darwin_schedules SET cancel_reason=%s WHERE rid=%s;", (json.dumps(process_reason(location)), record["rid"]))

            c.execute("""INSERT INTO darwin_schedules VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::json[], %s::json[])
                ON CONFLICT (rid) DO UPDATE SET
                signalling_id=EXCLUDED.signalling_id, status=EXCLUDED.status, category=EXCLUDED.category,
                operator=EXCLUDED.operator, is_active=EXCLUDED.is_active, is_charter=EXCLUDED.is_charter,
                is_deleted=EXCLUDED.is_deleted, is_passenger=EXCLUDED.is_passenger, origins=EXCLUDED.origins, destinations=EXCLUDED.destinations;""", (
                record["uid"], record["rid"], record.get("rsid"), record["ssd"], record["trainId"],
                record.get("status") or "P", record.get("trainCat") or "OO", record["toc"], record.get("isActive") or True,
                bool(record.get("isCharter")), bool(record.get("deleted")), record.get("isPassengerSvc") or True,
                origins, destinations
                ))

            psycopg2.extras.execute_batch(c, """INSERT INTO darwin_schedule_locations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""", batch)

        if record["tag"]=="TS":
            batch = []
            for location in record["list"]:
                original_wt = form_original_wt([process_time(location.get(a)) for a in ("wta", "wtp", "wtd")])
                if location["tag"]=="Location":
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
                    batch.append((
                        record["rid"], location["tpl"], original_wt, *times, *times_source, *times_type, *times_delay,
                        plat.get("$"), bool(plat.get("platsup")), bool(plat.get("cisPlatsup")), bool(plat.get("conf")), plat.get("platsrc"),
                    location.get("length", {}).get("$")))

                if location["tag"]=="LateReason":
                    c.execute("UPDATE darwin_schedules SET delay_reason=%s WHERE rid=%s;", (json.dumps(process_reason(location)), record["rid"]))

            psycopg2.extras.execute_batch(c, """INSERT INTO darwin_schedule_status VALUES (%s,%s,%s,  %s,%s,%s,  %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s, %s)
                ON CONFLICT (rid, tiploc, original_wt) DO UPDATE SET
                (ta,tp,td, ta_source,tp_source,td_source, ta_type,tp_type,td_type, ta_delayed,tp_delayed,td_delayed, length, plat,plat_suppressed,plat_cis_suppressed,plat_confirmed,plat_source)=
                (EXCLUDED.ta,EXCLUDED.tp,EXCLUDED.td, EXCLUDED.ta_source,EXCLUDED.tp_source,EXCLUDED.td_source, EXCLUDED.ta_type,EXCLUDED.tp_type,EXCLUDED.td_type, EXCLUDED.ta_delayed,EXCLUDED.tp_delayed,EXCLUDED.td_delayed, EXCLUDED.length, EXCLUDED.plat,EXCLUDED.plat_suppressed,EXCLUDED.plat_cis_suppressed,EXCLUDED.plat_confirmed,EXCLUDED.plat_source);""",
                batch)

        if record["tag"]=="deactivated":
            c.execute("UPDATE darwin_schedules SET is_active=FALSE WHERE rid=%s;", (record["rid"],))
        if record["tag"]=="OW":
            station_list = [a["crs"] for a in record["list"] if a["tag"]=="Station"]


            message = [a.get("$") or '' for a in record["list"] if a["tag"]=="Msg"][0]

            # Some messages are enclosed in <p> tags, some have a <p></p> in them.
            # Thank you National Rail, very cool
            pattern = pattern = re.compile("(^<p>)|(</p>$)")
            message = pattern.sub("", message).replace("<p></p>", "").replace('</p><p>', '<br>')

            if station_list:
                c.execute("""INSERT INTO darwin_messages VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (message_id)
                    DO UPDATE SET (category, severity, suppress, stations, message)=
                    (EXCLUDED.category, EXCLUDED.severity, EXCLUDED.suppress, EXCLUDED.stations, EXCLUDED.message);""",
                    (record["id"], record["cat"], record["sev"], bool(record.get("suppress")), station_list, message))
            else:
                c.execute("DELETE FROM darwin_messages WHERE message_id=%s;", (record["id"],))
        if record["tag"]=="association":
            if record["category"]=="JJ":
                # Semantically it makes a lot more sense to invert joins, so that all associations point to the "next" service
                # JN should hopefully make this distinct from JJ
                c.execute("""INSERT INTO darwin_associations VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(tiploc,main_rid,assoc_rid) DO NOTHING;""",
                    ("JN", record["tiploc"], record["assoc"]["rid"], full_original_wt(record["assoc"]), record["main"]["rid"], full_original_wt(record["main"])))
            else:
                c.execute("""INSERT INTO darwin_associations VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(tiploc,main_rid,assoc_rid) DO NOTHING;""",
                    (record["category"], record["tiploc"], record["main"]["rid"], full_original_wt(record["main"]), record["assoc"]["rid"], full_original_wt(record["assoc"])))

            # Make sure origin/dest lists are updated as appropriate
            renew_schedule_association_meta(c, record["main"]["rid"], record["assoc"]["rid"])

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
                store_message(self.cursor, parse.parse_darwin(message))
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

    mq = stomp.Connection([(SECRET["hostname"], 61613)],
        keepalive=True, auto_decode=False, heartbeats=(10000, 10000))

    with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as cursor:
        incorporate_reference_data(cursor)

        last_retrieved = query.last_retrieved(cursor)
        if not last_retrieved or (datetime.datetime.utcnow()-last_retrieved).seconds > 300:
            log.info("Last retrieval too old, using FTP snapshots")
            incorporate_ftp(cursor)

        mq.set_listener('iron-swallow', Listener(mq, cursor))
        connect_and_subscribe(mq)

        while True:
            with db_connection.new_cursor() as c2:
                renew_schedule_meta(c2)
            sleep(3600*12)
            with db_connection.new_cursor() as c3:
                incorporate_reference_data(c3)
