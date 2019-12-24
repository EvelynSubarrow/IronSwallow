import datetime
from collections import OrderedDict

from util import database

def json_default(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, datetime.date):
        return value.isoformat()
    else:
        raise ValueError

def _location_outline(target_list):
    pass

def station_board(cursor, tiplocs, intermediate_tiploc=None, passenger_only=True):
    cursor.execute("""SELECT
        sch.uid,sch.rid,sch.rsid,sch.ssd,sch.signalling_id,sch.status,sch.category,sch.operator,
        sch.is_active,sch.is_charter,sch.is_passenger,

        base.type,base.tiploc,base.activity,base.wta,base.wtp,base.wtd,base.pta,base.ptd,base.cancelled,
        orig.type,orig.tiploc,orig.activity,orig.wta,orig.wtp,orig.wtd,orig.pta,orig.ptd,orig.cancelled,
        inter.type,inter.tiploc,inter.activity,inter.wta,inter.wtp,inter.wtd,inter.pta,inter.ptd,inter.cancelled,
        dest.type,dest.tiploc,dest.activity,dest.wta,dest.wtp,dest.wtd,dest.pta,dest.ptd,dest.cancelled

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
        AND NOT sch.is_deleted ORDER BY base.wtd;""", (
        tiplocs,))

    services = []

    for row in cursor.fetchall():
        # Reverse so popping will retrieve from the front
        row = list(row)[::-1]
        out_row = OrderedDict()
        for key in ["uid", "rid", "rsid", "ssd", "signalling_id", "status", "category", "operator", "is_active", "is_charter", "is_passenger"]:
            out_row[key] = row.pop()
        services.append(out_row)

    return services
