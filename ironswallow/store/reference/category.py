from typing import Optional

from ironswallow.bplan import BPLAN_NETWORK_LOCATIONS
from ironswallow.store.darwin import OBSERVED_LOCATIONS

# Z - orphaned location (excepting signals)

# I - signal
# G - signalbox
# X - crossover
# R - level crossing
# D - siding

# S - NR station (can be a station with buses/ferries!)
# M - "metro" station (LT, TW, SJ, etc, but also heritage railways, for now)
# F - ferry terminal only
# B - bus only

def category_for(loc: dict) -> Optional[str]:
    # Might have to make this a little nicer at origin at some point, ah well
    corpus = (loc["name_corpus"] or '').upper()
    darwin = (loc["name_darwin"] or '').upper()
    crs_darwin = loc["crs_darwin"]
    tiploc = loc["tiploc"]
    operator = loc["operator"]

    # Signals (which usually have alphanumeric tiplocs) are specifically excluded from this determination because
    # they are not waypoints
    if tiploc not in BPLAN_NETWORK_LOCATIONS and tiploc not in OBSERVED_LOCATIONS and not tiploc[-1].isnumeric():
        return "Z"

    # Strong and stable operator based determinations
    elif operator == "ZB":
        return "B"
    elif operator == "ZF":
        return "F"
    elif operator in ["TW", "SJ", "NY", "ZM", "PC", "y", "SP"]:
        return "M"
    # LT manages some stations with mainline services, only categorise as metro if the power is 4th rail or ambiguous
    elif operator == "LT" and corpus.endswith("LT"):
        return "M"
    # Bus tiplocs always *end* (but never start) in BUS.
    elif "BUS" in corpus and tiploc.endswith("BUS"):
        return "B"
    elif "BUS" in corpus and ("STATION" in corpus or "STN" in corpus) and tiploc.endswith("BS"):
        return "B"

    # There's a few """bus""" locations that have non-buses. Not cool, NR
    # elif "(BUS)" in darwin and location_cats(loc.tiploc) in [["BS"], []]:
    #    loc.category = "B"

    elif operator is not None and crs_darwin is not None and darwin:
        return "S"
    # This seems pretty straightforward. Representative CORPUS example: WORK621 -> Worksop Signal Wp621
    elif ("SIGNAL" in corpus or "SIG" in corpus) and tiploc[-1].isnumeric():
        return "I"
    elif corpus.endswith("SIGNAL") or corpus.endswith("SIG"):
        return "I"
    elif "SIGNAL BOX" in corpus or "SIGNALBOX" in corpus:
        return "G"
    elif "CROSSOVER" in corpus or "XOVER" in corpus:
        return "X"
    elif "AHB" in corpus:
        return "R"
    elif "LEVEL CROSSING" in corpus:
        return "R"
    elif corpus.endswith("SDG") or corpus.endswith("SDGS") or corpus.endswith("SIDING") or corpus.endswith("SIDINGS"):
        return "D"
    elif corpus.endswith("JN") or corpus.endswith("JUNCTION") or corpus.endswith("JCN"):
        return "J"
    elif corpus.endswith("LOOP"):
        return "L"
    else:
        return None
