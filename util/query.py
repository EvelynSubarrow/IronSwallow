import datetime
from collections import OrderedDict
from decimal import Decimal

from util import database

def json_default(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, datetime.date):
        return value.isoformat()
    elif isinstance(value, datetime.time):
        return value.isoformat()
    else:
        raise ValueError(type(value))

def compare_time(t1, t2):
    if not (t1 and t2):
        return 0
    t1,t2 = [a.hour*3600+a.minute*60+a.second for a in (t1,t2)]
    return (Decimal(t1)-Decimal(t2))/3600

def combine_darwin_time(working_time, darwin_time):
    if not working_time:
        return None

    # Crossed midnight, increment ssd offset
    if compare_time(darwin_time, working_time) < -6:
        ssd_offset = +1
    # Normal increase or decrease, nothing we really need to do here
    elif -6 <= compare_time(darwin_time, working_time) <= +18:
        ssd_offset = 0
    # Back in time, crossed midnight (in reverse), decrement ssd offset
    elif +18 < compare_time(darwin_time, working_time):
        ssd_offset = -1

    return datetime.datetime.combine(working_time.date(), darwin_time) + datetime.timedelta(days=ssd_offset)

def form_location_select(names):
    stat_select = ""
    for location_name, stat_name, loc_dict_name in names:
        stat_select += "{ln}.type, {ld}.dict, {ln}.activity, {ln}.cancelled, {ln}.wta, {ln}.pta, {ln}.wtp, NULL, {ln}.wtd, {ln}.ptd,\n".format(ln=location_name, ld=loc_dict_name)
        stat_select += "{sn}.plat, {sn}.plat_suppressed, {sn}.plat_cis_suppressed, {sn}.plat_confirmed, {sn}.plat_source,\n".format(sn=stat_name)
        for time_name in ("ta", "tp", "td"):
            stat_select += "{sn}.{tn}, {sn}.{tn}_source, {sn}.{tn}_type, {sn}.{tn}_delayed{comma}\n".format(
                sn=stat_name, tn=time_name, comma=","*(not(stat_name==names[-1][1] and time_name=="td")))
    return stat_select

def process_location_outline(location):
    if location:
        del location["name_darwin"]
        del location["name_corpus"]
        del location["crs_corpus"]
        del location["operator"]
    return location

def location_dict(row, preserve_null_times=False, preserve_null_platform=False):
    out_row = OrderedDict([(a,row.pop()) for a in ("type","location","activity","cancelled")])

    loc_outline = process_location_outline(out_row["location"])
    del out_row["location"]
    if loc_outline:
        out_row.update(loc_outline)

    out_row["times"] = OrderedDict()

    for time_name in ("arrival", "pass", "departure"):
        out_row["times"][time_name] = OrderedDict()
        for time_type in ("working", "public"):
            time = row.pop()
            if time or preserve_null_times:
                out_row["times"][time_name][time_type] = time
        if preserve_null_times:
            out_row["times"][time_name]["estimated"] = None
            out_row["times"][time_name]["actual"] = None

    out_row["platform"] = OrderedDict()
    for platform_field_name in ("platform", "suppressed", "cis_suppressed", "confirmed", "source"):
        platform_field = row.pop()
        if platform_field!=None or preserve_null_platform:
            out_row["platform"][platform_field_name] = platform_field

    for time_name in ("arrival", "pass", "departure"):
        darwin_time = OrderedDict([(a, row.pop()) for a in ("time", "source", "type", "delayed")])
        working_time = out_row["times"][time_name].get("working")

        if darwin_time["time"] and working_time:
            full_dt = combine_darwin_time(working_time, darwin_time["time"])

        if darwin_time["type"]=="A":
            out_row["times"][time_name]["actual"] = full_dt
        elif darwin_time["type"]=="E":
            out_row["times"][time_name]["estimated"] = full_dt

    return out_row

def station_board(cursor, locations, base_dt=None, period=480, limit=15, intermediate_tiploc=None, passenger_only=True):
    locations = tuple([a.upper() for a in locations])
    out = OrderedDict()

    cursor.execute("""SELECT tiploc, dict FROM darwin_locations WHERE crs_darwin IN %s OR tiploc IN %s;""", [locations]*2)
    locations = OrderedDict([(a[0],a[1]) for a in cursor.fetchall()])

    # Whatever location code you gave us, if it doesn't exist, let's not even try to pretend now, go away
    if not locations:
        return None

    out["locations"] = locations

    # In principle you can pass in a list of locations with several corresponding CRS codes
    crs_list_dedup = list(OrderedDict((a["crs_darwin"],0) for a in locations.values()).keys())

    out["messages"] = []

    cursor.execute("SELECT category,severity,suppress,stations,message FROM darwin_messages WHERE stations && %s::VARCHAR(3)[];", (crs_list_dedup,))
    for row in cursor.fetchall():
        out["messages"].append(OrderedDict([(a,row[i]) for i,a in enumerate(["category", "severity", "suppress", "stations", "message"])]))

    stat_select = form_location_select([("base", "b_stat", "b_loc"), ("orig", "o_stat", "o_loc"), ("inter", "i_stat", "i_loc"), ("dest", "d_stat", "d_loc")])
    cursor.execute("""SELECT
        sch.uid,sch.rid,sch.rsid,sch.ssd,sch.signalling_id,sch.status,sch.category,sch.operator,
        sch.is_active,sch.is_charter,sch.is_passenger,

        {}

        FROM darwin_schedule_locations AS base
        LEFT JOIN darwin_schedules AS sch ON base.rid=sch.rid

        LEFT JOIN darwin_schedule_locations AS dest ON base.rid=dest.rid AND dest.type='DT'
        LEFT JOIN darwin_schedule_locations AS orig ON base.rid=orig.rid AND orig.type='OR'
        LEFT JOIN darwin_schedule_locations AS inter ON base.rid=inter.rid AND inter.type!='PP' AND inter.tiploc=%s

        LEFT JOIN darwin_schedule_status AS o_stat ON orig.rid=o_stat.rid AND orig.original_wt=o_stat.original_wt
        LEFT JOIN darwin_schedule_status AS b_stat ON base.rid=b_stat.rid AND base.original_wt=b_stat.original_wt
        LEFT JOIN darwin_schedule_status AS i_stat ON inter.rid=i_stat.rid AND inter.original_wt=i_stat.original_wt
        LEFT JOIN darwin_schedule_status AS d_stat ON dest.rid=d_stat.rid AND dest.original_wt=d_stat.original_wt

        LEFT JOIN darwin_locations AS o_loc ON orig.tiploc=o_loc.tiploc
        LEFT JOIN darwin_locations AS b_loc ON base.tiploc=b_loc.tiploc
        LEFT JOIN darwin_locations AS i_loc ON inter.tiploc=i_loc.tiploc
        LEFT JOIN darwin_locations AS d_loc ON dest.tiploc=d_loc.tiploc

        WHERE base.wtd IS NOT NULL
        AND base.tiploc in %s
        AND base.type in ('IP', 'DT', 'OR')
        AND NOT sch.is_deleted
        AND %s <= base.wtd
        AND %s >= base.wtd
        ORDER BY base.wtd
        LIMIT %s;""".format(stat_select), (
        intermediate_tiploc, tuple(locations.keys()), base_dt, base_dt+datetime.timedelta(minutes=period), limit))

    out["services"] = []

    for row in cursor.fetchall():
        # Reverse so popping will retrieve from the front
        row = list(row)[::-1]
        out_row = OrderedDict()
        for key in ["uid", "rid", "rsid", "ssd", "signalling_id", "status", "category", "operator", "is_active", "is_charter", "is_passenger"]:
            out_row[key] = row.pop()

        for location_name in ("here", "origin", "intermediate", "destination"):
            out_row[location_name] = location_dict(row)

        out["services"].append(out_row)

    return out

def service(cursor, sid, date=None):
    cursor.execute("""SELECT sch.uid,sch.rid,sch.rsid,sch.ssd,sch.signalling_id,sch.status,sch.category,sch.operator,
        sch.is_active,sch.is_charter,sch.is_passenger FROM darwin_schedules as sch WHERE rid=%s OR (uid=%s AND ssd=%s);""", (sid, sid, date))

    row = cursor.fetchone()

    if not row:
        return None
    else:
        row = list(row)[::-1]

        schedule = OrderedDict()
        for key in ["uid", "rid", "rsid", "ssd", "signalling_id", "status", "category", "operator", "is_active", "is_charter", "is_passenger"]:
            schedule[key] = row.pop()

        cursor.execute("""SELECT {} FROM darwin_schedule_locations as loc
            LEFT JOIN darwin_schedule_status AS stat ON loc.rid=stat.rid AND loc.original_wt=stat.original_wt
            LEFT JOIN darwin_locations AS loc_outline ON loc.tiploc=loc_outline.tiploc
            WHERE loc.rid=%s ORDER BY INDEX ASC;
            """.format(form_location_select([("loc", "stat", "loc_outline")])), (schedule["rid"],))

        schedule["locations"] = []

        for row in cursor.fetchall():
            row = list(row)[::-1]
            schedule["locations"].append(location_dict(row))

        return schedule

def last_retrieved(cursor):
    cursor.execute("SELECT time_acquired FROM last_received_sequence;")
    row = cursor.fetchone()
    if row:
        return row[0]

