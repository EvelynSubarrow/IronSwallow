import csv, logging
from datetime import datetime,timedelta

import ironswallow.util.database as database
from IronSwallowORM import models

log = logging.getLogger("IronSwallow")

# TODO: eventually BPLAN will be updated - how are we going to remove retired data?
def parse_store_bplan():
    log.info("Merging BPlan")

    with database.DatabaseConnection() as db_c:
        session = models.sessionmaker(bind=db_c.engine)()

        with open("datasets/bplan.txt", encoding="windows-1252") as tsv:
            for line in csv.reader(tsv, delimiter="\t"):
                if line[0] == "NWK":
                    (record_type, action_code, origin_location, dest_location, running_line_code,
                     running_line_desc, start_date, end_date, initial_direction, final_direction, distance,
                     doo_p, doo_no_p, retb, zone, reversible, power, ra, max_tl) = line

                    running_line_desc = running_line_desc or None
                    distance = int(distance) if distance else None

                    # There are some with a time of 23:59:59. I hate it.
                    start_date = (datetime.strptime(start_date, "%d-%m-%Y %H:%M:%S") + timedelta(seconds=1)).date() if start_date else None
                    end_date = (datetime.strptime(end_date, "%d-%m-%Y %H:%M:%S") + timedelta(seconds=1)).date() if end_date else None
                    doo_p = doo_p == "Y"
                    doo_no_p = doo_no_p == "Y"
                    retb = retb == "Y"

                    session.merge(models.BPlanNetworkLink(origin=origin_location, destination=dest_location,
                        running_line_code=running_line_code, running_line_desc=running_line_desc, start_date=start_date,
                        end_date=end_date, initial_direction=initial_direction, final_direction=final_direction,
                        distance=distance, doo_passenger=doo_p, doo_non_passenger=doo_no_p, retb=retb, zone=zone,
                        reversible=reversible, power=power, route_allowance=ra))
        session.commit()
