import datetime, json, re, logging, time, threading
from collections import OrderedDict
from decimal import Decimal
from queue import Queue
from queue import LifoQueue
from typing import Union

import psycopg2.extras

from ironswallow.store import meta
from ironswallow.util import query
from main import LOCATIONS, REASONS

OBSERVED_LOCATIONS = set()

log = logging.getLogger("IronSwallow")

def compare_time(t1, t2) -> int:
    if not (t1 and t2):
        return 0
    t1, t2 = [a.hour*3600+a.minute*60+a.second for a in (t1,t2)]
    return (Decimal(t1)-Decimal(t2))/3600


def process_time(time) -> datetime.time:
    if not time:
        return None
    if len(time) == 5:
        time += ":00"
    return datetime.datetime.strptime(time, "%H:%M:%S").time()


def full_original_wt(location):
    return form_original_wt([process_time(location.get(a)) for a in ("wta", "wtp", "wtd")])


def form_original_wt(times) -> str:
    out = ""
    for time in times:
        if time:
            out += time.strftime("%H%M%S")
        else:
            out += "      "
    return out


def process_reason(reason):
    return OrderedDict([
        ("code", reason["$"]),
        ("message", REASONS[(reason["$"], "C"*(reason["tag"]=="cancelReason") or "D")]),
        ("location", query.process_location_outline(LOCATIONS.get(reason.get("tiploc")))),
        ("near", bool(reason.get("near"))),
        ])


class MessageProcessor:
    """In theory you can use this without the context manager, but don't"""

    def __init__(self, cursor):
        self.cursor = cursor
        self._query_queue = Queue(maxsize=1000)
        self._query_fetch = LifoQueue()
        self._thread_quit = False
        self._thread_start = False
        self._thread = None

    def execute(self, query: str, params: Union[tuple, list]=(), batch=False, retain=False, use_retain=False):
        self._query_queue.put((query, params, batch, retain, use_retain))


    def __enter__(self) -> "MessageProcessor":
        self.thread = threading.Thread(target=self._execute_thread)
        self.thread.start()
        self._thread_start = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._query_queue.put(None)
        while not self._thread_quit:
            time.sleep(0.1)

        return False

    def _execute_thread(self):
        while True:
            entry = self._query_queue.get()
            if not entry:
                self._thread_quit = True
                return
            query, params, batch, retain, use_retain = entry

            if use_retain:
                params = self._query_fetch.get()

            if batch:
                psycopg2.extras.execute_batch(self.cursor, query, params)
            else:
                self.cursor.execute(query, params)

            if retain:
                self._query_fetch.put(self.cursor.fetchall())

    def store(self, parsed) -> None:
        global OBSERVED_LOCATIONS
        if not parsed:
            return

        assoc_batch = []

        for record in parsed.get("list", []):
            if record["tag"]=="schedule":

                index = 0
                last_time, ssd_offset = None, 0

                # I feel like I owe an explanation for this abomination, so here we go - it turns out that breaking a
                # foreign key reference by means other than straightforward deletion isn't something that you can handle
                # with constraints in psql. Ideally you could very neatly put aside something that didn't match back
                # up, but that's just not how it goes
                # The select here is so bizarre just so this can be fed direct back into the insert later on
                self.execute("SELECT category,tiploc,main_rid,main_original_wt,assoc_rid,assoc_original_wt, "
                          "tiploc,main_rid,main_original_wt,"
                          "tiploc,assoc_rid,assoc_original_wt "
                          "FROM darwin_associations WHERE main_rid=%s OR assoc_rid=%s", (record["rid"], record["rid"]), retain=True)

                self.execute("DELETE FROM darwin_schedule_locations WHERE rid=%s;", (record["rid"],))

                origins, destinations = [], []
                batch = []

                for location in record["list"]:
                    if location["tag"] in ["OPOR", "OR", "OPIP", "IP", "PP", "DT", "OPDT"]:
                        OBSERVED_LOCATIONS |= {location["tpl"]}

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
                        self.execute("UPDATE darwin_schedules SET cancel_reason=%s WHERE rid=%s;", (json.dumps(process_reason(location)), record["rid"]))

                self.execute("""INSERT INTO darwin_schedules VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::json[], %s::json[])
                    ON CONFLICT (rid) DO UPDATE SET
                    signalling_id=EXCLUDED.signalling_id, status=EXCLUDED.status, category=EXCLUDED.category,
                    operator=EXCLUDED.operator, is_active=EXCLUDED.is_active, is_charter=EXCLUDED.is_charter,
                    is_deleted=EXCLUDED.is_deleted, is_passenger=EXCLUDED.is_passenger, origins=EXCLUDED.origins, destinations=EXCLUDED.destinations;""", (
                    record["uid"], record["rid"], record.get("rsid"), record["ssd"], record["trainId"],
                    record.get("status") or "P", record.get("trainCat") or "OO", record["toc"], record.get("isActive") or True,
                    bool(record.get("isCharter")), bool(record.get("deleted")), record.get("isPassengerSvc") or True,
                    origins, destinations
                    ))

                self.execute("""INSERT INTO darwin_schedule_locations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""", params=batch, batch=True)

                self.execute("""INSERT INTO darwin_associations
                (category,tiploc,main_rid,main_original_wt,assoc_rid,assoc_original_wt) SELECT %s,%s,%s,%s,%s,%s WHERE
                EXISTS (SELECT * FROM darwin_schedule_locations WHERE tiploc=%s AND rid=%s AND original_wt=%s) AND
                EXISTS (SELECT * FROM darwin_schedule_locations WHERE tiploc=%s AND rid=%s AND original_wt=%s) ON CONFLICT DO NOTHING;""", batch=True, use_retain=True)

            if record["tag"] == "TS":
                batch = []
                for location in record["list"]:
                    original_wt = form_original_wt([process_time(location.get(a)) for a in ("wta", "wtp", "wtd")])
                    if location["tag"] == "Location":
                        times = []
                        times_source = []
                        times_type = []
                        times_delay = []

                        for i, time_d in enumerate([location.get(a, {}) for a in ["arr", "pass", "dep"]]):
                            time_content = time_d.get("at", None) or time_d.get("et", None)
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
                        self.execute("UPDATE darwin_schedules SET delay_reason=%s WHERE rid=%s;", (json.dumps(process_reason(location)), record["rid"]))

                self.execute("""INSERT INTO darwin_schedule_status VALUES (%s,%s,%s,  %s,%s,%s,  %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s, %s)
                    ON CONFLICT (rid, tiploc, original_wt) DO UPDATE SET
                    (ta,tp,td, ta_source,tp_source,td_source, ta_type,tp_type,td_type, ta_delayed,tp_delayed,td_delayed, length, plat,plat_suppressed,plat_cis_suppressed,plat_confirmed,plat_source)=
                    (EXCLUDED.ta,EXCLUDED.tp,EXCLUDED.td, EXCLUDED.ta_source,EXCLUDED.tp_source,EXCLUDED.td_source, EXCLUDED.ta_type,EXCLUDED.tp_type,EXCLUDED.td_type, EXCLUDED.ta_delayed,EXCLUDED.tp_delayed,EXCLUDED.td_delayed, EXCLUDED.length, EXCLUDED.plat,EXCLUDED.plat_suppressed,EXCLUDED.plat_cis_suppressed,EXCLUDED.plat_confirmed,EXCLUDED.plat_source);""",
                    params=batch, batch=True)

            if record["tag"]=="deactivated":
                self.execute("UPDATE darwin_schedules SET is_active=FALSE WHERE rid=%s;", (record["rid"],))
            if record["tag"]=="OW":
                station_list = [a["crs"] for a in record["list"] if a["tag"] == "Station"]


                message = [a.get("$") or '' for a in record["list"] if a["tag"]=="Msg"][0]

                # Some messages are enclosed in <p> tags, some have a <p></p> in them.
                # Thank you National Rail, very cool
                pattern = pattern = re.compile("(^<p>)|(</p>$)")
                message = pattern.sub("", message).replace("<p></p>", "").replace('</p><p>', '<br>')

                if station_list:
                    self.execute("""INSERT INTO darwin_messages VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (message_id)
                        DO UPDATE SET (category, severity, suppress, stations, message)=
                        (EXCLUDED.category, EXCLUDED.severity, EXCLUDED.suppress, EXCLUDED.stations, EXCLUDED.message);""",
                        (record["id"], record["cat"], record["sev"], bool(record.get("suppress")), station_list, message))
                else:
                    self.execute("DELETE FROM darwin_messages WHERE message_id=%s;", (record["id"],))

            if record["tag"] == "association":
                main_owt = full_original_wt(record["main"])
                assoc_owt = full_original_wt(record["assoc"])

                if record["category"]=="JJ":
                    # Semantically it makes a lot more sense to invert joins, so that all associations point to the "next" service
                    # JN should hopefully make this distinct from JJ
                    # The subclauses here replace a slightly nicer set of queries which notified about orphan assocs. Oh well.
                    assoc_batch.append(("JN", record["tiploc"], record["assoc"]["rid"], assoc_owt, record["main"]["rid"], main_owt,
                                        record["tiploc"], record["main"]["rid"], main_owt,
                                        record["tiploc"], record["assoc"]["rid"], assoc_owt))
                else:
                    assoc_batch.append((record["category"], record["tiploc"], record["main"]["rid"], main_owt, record["assoc"]["rid"], assoc_owt,
                                        record["tiploc"], record["main"]["rid"], main_owt,
                                        record["tiploc"], record["assoc"]["rid"], assoc_owt))

        if assoc_batch:
            self.execute("""INSERT INTO darwin_associations SELECT %s, %s, %s, %s, %s, %s WHERE
                                EXISTS (SELECT * FROM darwin_schedule_locations WHERE tiploc=%s AND rid=%s AND original_wt=%s) AND
                                EXISTS (SELECT * FROM darwin_schedule_locations WHERE tiploc=%s AND rid=%s AND original_wt=%s)
                                ON CONFLICT(tiploc,main_rid,assoc_rid) DO NOTHING;""", assoc_batch, batch=True)

        if not self._thread_start:
            self._query_queue.put(None)
            self._execute_thread()
