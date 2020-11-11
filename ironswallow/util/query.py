import datetime
from collections import OrderedDict
from decimal import Decimal

from ironswallow.util import database

def json_default(value) -> str:
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    elif isinstance(value, datetime.date):
        return value.isoformat()
    elif isinstance(value, datetime.time):
        return value.isoformat()
    else:
        raise ValueError(type(value))

def compare_time(t1, t2) -> int:
    if not (t1 and t2):
        return 0
    t1,t2 = [a.hour*3600+a.minute*60+a.second for a in (t1,t2)]
    return (Decimal(t1)-Decimal(t2))/3600

def combine_darwin_time(working_time, darwin_time) -> datetime.datetime:
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


def last_retrieved(cursor) -> datetime.datetime:
    cursor.execute("SELECT time_acquired FROM last_received_sequence;")
    row = cursor.fetchone()
    if row:
        return row[0]

