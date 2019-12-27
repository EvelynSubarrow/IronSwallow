import datetime
from collections import OrderedDict

from util import database

def json_default(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, datetime.date):
        return value.isoformat()
    else:
        raise ValueError(type(value))

def _location_outline(target_list):
    pass

def station_board(cursor, tiplocs, intermediate_tiploc=None, passenger_only=True):
    stat_select = ""
    for location_name, stat_name in [("base", "b_stat"), ("orig", "o_stat"), ("inter", "i_stat"), ("dest", "d_stat")]:
        stat_select += "{ln}.type, {ln}.tiploc, {ln}.activity, {ln}.wta, {ln}.wtp, {ln}.wtd, {ln}.pta, {ln}.ptd, {ln}.cancelled,\n".format(ln=location_name)
        stat_select += "{sn}.plat, {sn}.plat_suppressed, {sn}.plat_cis_suppressed, {sn}.plat_confirmed, {sn}.plat_source,\n".format(sn=stat_name)
        for time_name in ("ta", "tp", "td"):
            stat_select += "{sn}.{tn}, {sn}.{tn}_source, {sn}.{tn}_type, {sn}.{tn}_delayed{comma}\n".format(
                sn=stat_name, tn=time_name, comma=","*(not(stat_name=="d_stat" and time_name=="td")))
    cursor.execute("""SELECT
        sch.uid,sch.rid,sch.rsid,sch.ssd,sch.signalling_id,sch.status,sch.category,sch.operator,
        sch.is_active,sch.is_charter,sch.is_passenger,

        {}

        FROM darwin_schedule_locations AS base
        LEFT JOIN darwin_schedules AS sch ON base.rid=sch.rid

        LEFT JOIN darwin_schedule_locations AS dest ON base.rid=dest.rid AND dest.type='DT'
        LEFT JOIN darwin_schedule_locations AS orig ON base.rid=orig.rid AND orig.type='OR'
        LEFT JOIN darwin_schedule_locations AS inter ON base.rid=inter.rid AND inter.type!='PP'

        LEFT JOIN darwin_schedule_status AS o_stat ON orig.rid=o_stat.rid AND orig.tiploc=o_stat.tiploc
        LEFT JOIN darwin_schedule_status AS b_stat ON base.rid=b_stat.rid AND base.tiploc=b_stat.tiploc
        LEFT JOIN darwin_schedule_status AS i_stat ON inter.rid=i_stat.rid AND inter.tiploc=i_stat.tiploc
        LEFT JOIN darwin_schedule_status AS d_stat ON dest.rid=d_stat.rid AND dest.tiploc=d_stat.tiploc

        WHERE base.wtd IS NOT NULL
        AND base.tiploc in %s
        AND base.type in ('IP', 'DT', 'OR')
        AND NOT sch.is_deleted ORDER BY base.wtd;""".format(stat_select), (
        tiplocs,))

    services = []

    for row in cursor.fetchall():
        # Reverse so popping will retrieve from the front
        row = list(row)[::-1]
        out_row = OrderedDict()
        for key in ["uid", "rid", "rsid", "ssd", "signalling_id", "status", "category", "operator", "is_active", "is_charter", "is_passenger"]:
            out_row[key] = row.pop()

        for location_name in ("here", "origin", "intermediate", "destination"):
        # "{ln}.type, {ln}.tiploc, {ln}.activity, {ln}.wta, {ln}.wtp, {ln}.wtd, {ln}.pta, {ln}.ptd, {ln}.cancelled,\n"
            out_row[location_name] = OrderedDict([(a,row.pop()) for a in ("type","tiploc","activity","wta","wtp","wtd","pta","ptd", "cancelled")])
            out_row[location_name]["platform"] = OrderedDict(
                [(a, row.pop()) for a in ("platform", "suppressed", "cis_suppressed", "confirmed", "source")])
            for time_name in ("ta", "tp", "td"):
                for n in range(4):
                    row.pop()

        services.append(out_row)

    return services
