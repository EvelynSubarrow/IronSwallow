import csv, logging
from datetime import datetime,timedelta

from sqlalchemy import PrimaryKeyConstraint, inspect
from sqlalchemy.dialects.postgresql import insert

import ironswallow.util.database as database
from IronSwallowORM import models

log = logging.getLogger("IronSwallow")

BPLAN_NETWORK_LOCATIONS = {}

LOCALISED_OTHER_REFERENCES = []

# TODO: eventually BPLAN will be updated - how are we going to remove retired data?
def parse_store_bplan():
    global BPLAN_NETWORK_LOCATIONS
    log.info("Collecting BPlan")

    with database.DatabaseConnection() as db_c:
        #session = models.sessionmaker(bind=db_c.engine)()
        bplan_nwk_batch = []
        bplan_plt_batch = []
        bplan_ref_batch = []

        with open("datasets/bplan.txt", encoding="windows-1252") as tsv:
            for line in csv.reader(tsv, delimiter="\t"):
                if line[0] == "NWK":
                    (record_type, action_code, origin_location, dest_location, running_line_code,
                     running_line_desc, start_date, end_date, initial_direction, final_direction, distance,
                     doo_p, doo_no_p, retb, zone, reversible, power, ra, max_tl) = line

                    running_line_code = running_line_code.rstrip()
                    running_line_desc = running_line_desc or None
                    distance = int(distance) if distance else None

                    # There are some with a time of 23:59:59. I hate it.
                    start_date = (datetime.strptime(start_date, "%d-%m-%Y %H:%M:%S") + timedelta(seconds=1)).date() if start_date else None
                    end_date = (datetime.strptime(end_date, "%d-%m-%Y %H:%M:%S") + timedelta(seconds=1)).date() if end_date else None
                    doo_p = doo_p == "Y"
                    doo_no_p = doo_no_p == "Y"
                    retb = retb == "Y"

                    bplan_nwk_batch.append(dict(origin=origin_location, destination=dest_location,
                        running_line_code=running_line_code, running_line_desc=running_line_desc, start_date=start_date,
                        end_date=end_date, initial_direction=initial_direction, final_direction=final_direction,
                        distance=distance, doo_passenger=doo_p, doo_non_passenger=doo_no_p, retb=retb, zone=zone,
                        reversible=reversible, power=power, route_allowance=ra))

                    for tl in [origin_location, dest_location]:
                        if tl not in BPLAN_NETWORK_LOCATIONS:
                            BPLAN_NETWORK_LOCATIONS[tl] = set()
                        BPLAN_NETWORK_LOCATIONS[tl] |= {running_line_code}

                elif line[0] == "PLT":
                    (record_type, action_code, tiploc, platform, start_date, end_date, length, power, doo_passenger,
                     doo_non_passenger) = line

                    platform = platform.rstrip() or None
                    start_date = (datetime.strptime(start_date, "%d-%m-%Y %H:%M:%S") + timedelta(seconds=1)).date() if start_date else None
                    end_date = (datetime.strptime(end_date, "%d-%m-%Y %H:%M:%S") + timedelta(seconds=1)).date() if end_date else None
                    doo_passenger = doo_passenger == "Y"
                    doo_non_passenger = doo_non_passenger == "Y"
                    length = int(length) if length else None

                    bplan_plt_batch.append(dict(tiploc=tiploc, platform=platform, start_date=start_date,
                                                end_date=end_date, length=length, power=power,
                                                doo_passenger=doo_passenger, doo_non_passenger=doo_non_passenger))

                elif line[0] == "REF":
                    (record_type, action_code, code_type, code, description) = line
                    if code_type=="ACT":
                        description = description[:52].rstrip()
                    bplan_ref_batch.append(dict(source="BPLAN", locale="en_gb", code_type=code_type, code=code,
                                                description=description))

        log.info("Merging BPlan")
        statement = insert(models.BPlanNetworkLink.__table__).on_conflict_do_nothing()
        db_c.sa_connection.execute(statement, bplan_nwk_batch)

        statement = insert(models.BPlanPlatform.__table__).on_conflict_do_nothing()
        db_c.sa_connection.execute(statement, bplan_plt_batch)

        dictified_other_refs = [dict(source=a, locale=b, code_type=c, code=d, description=e) for a, b, c, d, e in LOCALISED_OTHER_REFERENCES]

        statement = insert(models.LocalisedReference.__table__)
        ref_pks = [key.name for key in inspect(models.LocalisedReference).primary_key]
        statement = statement.on_conflict_do_update(index_elements=ref_pks, set_={"description": statement.excluded.description})
        db_c.sa_connection.execute(statement, bplan_ref_batch + dictified_other_refs)

        #session.commit()
