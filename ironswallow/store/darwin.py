import datetime, json, re, logging
from collections import OrderedDict
from decimal import Decimal

import psycopg2.extras

from ironswallow.store import meta
from ironswallow.util import query
from main import LOCATIONS, REASONS

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


def store_message(cursor, parsed) -> None:
    if not parsed:
        return
    c = cursor

    for record in parsed.get("list", []):
        if record["tag"]=="schedule":

            index = 0
            last_time, ssd_offset = None, 0

            # I feel like I owe an explanation for this abomination, so here we go - it turns out that breaking a
            # foreign key reference by means other than straightforward deletion isn't something that you can handle
            # with constraints in psql. Ideally you could very neatly put aside something that didn't match back
            # up, but that's just not how it goes
            # The select here is so bizarre just so this can be fed direct back into the insert later on
            c.execute("SELECT category,tiploc,main_rid,main_original_wt,assoc_rid,assoc_original_wt, "
                      "tiploc,main_rid,main_original_wt,"
                      "tiploc,assoc_rid,assoc_original_wt "
                      "FROM darwin_associations WHERE main_rid=%s OR assoc_rid=%s", (record["rid"], record["rid"]))
            associations_held_back = c.fetchall()

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

            psycopg2.extras.execute_batch(c, """INSERT INTO darwin_associations
            (category,tiploc,main_rid,main_original_wt,assoc_rid,assoc_original_wt) SELECT %s,%s,%s,%s,%s,%s WHERE
            EXISTS (SELECT * FROM darwin_schedule_locations WHERE tiploc=%s AND rid=%s AND original_wt=%s) AND
            EXISTS (SELECT * FROM darwin_schedule_locations WHERE tiploc=%s AND rid=%s AND original_wt=%s) ON CONFLICT DO NOTHING;""", associations_held_back)

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
                    c.execute("UPDATE darwin_schedules SET delay_reason=%s WHERE rid=%s;", (json.dumps(process_reason(location)), record["rid"]))

            psycopg2.extras.execute_batch(c, """INSERT INTO darwin_schedule_status VALUES (%s,%s,%s,  %s,%s,%s,  %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s,%s, %s)
                ON CONFLICT (rid, tiploc, original_wt) DO UPDATE SET
                (ta,tp,td, ta_source,tp_source,td_source, ta_type,tp_type,td_type, ta_delayed,tp_delayed,td_delayed, length, plat,plat_suppressed,plat_cis_suppressed,plat_confirmed,plat_source)=
                (EXCLUDED.ta,EXCLUDED.tp,EXCLUDED.td, EXCLUDED.ta_source,EXCLUDED.tp_source,EXCLUDED.td_source, EXCLUDED.ta_type,EXCLUDED.tp_type,EXCLUDED.td_type, EXCLUDED.ta_delayed,EXCLUDED.tp_delayed,EXCLUDED.td_delayed, EXCLUDED.length, EXCLUDED.plat,EXCLUDED.plat_suppressed,EXCLUDED.plat_cis_suppressed,EXCLUDED.plat_confirmed,EXCLUDED.plat_source);""",
                batch)

        if record["tag"]=="deactivated":
            c.execute("UPDATE darwin_schedules SET is_active=FALSE WHERE rid=%s;", (record["rid"],))
        if record["tag"]=="OW":
            station_list = [a["crs"] for a in record["list"] if a["tag"] == "Station"]


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

        if record["tag"] == "association":
            main_owt = full_original_wt(record["main"])
            assoc_owt = full_original_wt(record["assoc"])

            # thank you psycopg2 very cool
            c.execute("SELECT count(*) FROM darwin_schedules WHERE rid IN %s;", ((record["assoc"]["rid"], record["main"]["rid"]),))
            ct_a = c.fetchone()[0]
            c.execute("SELECT count(*) FROM darwin_schedule_locations WHERE rid=%s AND original_wt=%s AND tiploc=%s;", (record["assoc"]["rid"], assoc_owt, record["tiploc"]))
            ct_b = c.fetchone()[0]
            c.execute("SELECT count(*) FROM darwin_schedule_locations WHERE rid=%s AND original_wt=%s AND tiploc=%s;", (record["main"]["rid"], main_owt, record["tiploc"]))
            ct_c = c.fetchone()[0]

            if ct_a != 2:
                log.error("Orphan association: main ({}), assoc ({}) cat ({}) loc ({})".format(record["main"]["rid"], record["assoc"]["rid"], record["category"], record["tiploc"]))
            elif ct_b != 1:
                log.error("Orphan loc association: main ({}), assoc (.{}.) cat ({}) loc ({})".format(record["main"]["rid"], record["assoc"]["rid"], record["category"], record["tiploc"]))
            elif ct_c != 1:
                log.error("Orphan loc association: main (.{}.), assoc ({}) cat ({}) loc ({})".format(record["main"]["rid"], record["assoc"]["rid"], record["category"], record["tiploc"]))
            else:
                if record["category"]=="JJ":
                    # Semantically it makes a lot more sense to invert joins, so that all associations point to the "next" service
                    # JN should hopefully make this distinct from JJ
                    c.execute("""INSERT INTO darwin_associations VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(tiploc,main_rid,assoc_rid) DO NOTHING;""",
                        ("JN", record["tiploc"], record["assoc"]["rid"], full_original_wt(record["assoc"]), record["main"]["rid"], main_owt))
                else:
                    c.execute("""INSERT INTO darwin_associations VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(tiploc,main_rid,assoc_rid) DO NOTHING;""",
                        (record["category"], record["tiploc"], record["main"]["rid"], full_original_wt(record["main"]), record["assoc"]["rid"], assoc_owt))


            # Make sure origin/dest lists are updated as appropriate
            meta.renew_schedule_association_meta(c, record["main"]["rid"], record["assoc"]["rid"])
