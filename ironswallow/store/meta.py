import json, logging
from collections import OrderedDict

import psycopg2.extras

from ironswallow.util import query
from main import LOCATIONS

log = logging.getLogger("IronSwallow")

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
            c.execute("""UPDATE darwin_schedules SET destinations=darwin_schedules.destinations || %s::json[] WHERE rid=%s;""", (row["assoc_destinations"], row["main_rid"]))

        if not any([a.get("association_tiploc")==row["tiploc"] and a["source"]==row["category"] for a in row["assoc_origins"]]):
            c.execute("""UPDATE darwin_schedules SET origins=darwin_schedules.origins || %s::json[] WHERE rid=%s;""", (row["main_origins"],row["assoc_rid"]))

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
