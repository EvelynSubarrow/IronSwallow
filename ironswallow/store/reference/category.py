from typing import Optional

from ironswallow.bplan import BPLAN_NETWORK_LOCATIONS, LOCALISED_OTHER_REFERENCES
from ironswallow.store.darwin import OBSERVED_LOCATIONS

FORCED_CATEGORIES = {
    "WEST530": "I",  # For some inexplicable reason Westerton Sig YH350 has a CRS assigned in Darwin
    "GROSNYM": "M",  # Grosmont North Yorkshire Moors Railway
    "YOKERCS": "T",  # Yoker Stabling & Cleaning - I guess this makes a bit more sense but still not really
}

LOCALISED_OTHER_REFERENCES.extend([
("IS", "en_gb", "LCAT", "Z", "Unreachable"), # Excl. signals - uncertain how they're pathed

("IS", "en_gb", "LCAT", "J", "Junction"),
("IS", "en_gb", "LCAT", "I", "Signal"),
("IS", "en_gb", "LCAT", "G", "Signalbox"),
("IS", "en_gb", "LCAT", "X", "Crossover"),
("IS", "en_gb", "LCAT", "R", "Level crossing"),
("IS", "en_gb", "LCAT", "D", "Siding"),
("IS", "en_gb", "LCAT", "T", "Depot"),
("IS", "en_gb", "LCAT", "Q", "Freight reception"),

("IS", "en_gb", "LCAT", "S", "Mainline station"),
("IS", "en_gb", "LCAT", "M", "Non-NR station"),
("IS", "en_gb", "LCAT", "F", "Ferry terminal"),
("IS", "en_gb", "LCAT", "B", "Bus stop"),


("IS", "nb_no", "LCAT", "Z", "Koplet fra jernbanenettverk"),

("IS", "nb_no", "LCAT", "J", "Sporvekseler"), # Knutepunkt might be preferable but it's possibly too obscure
("IS", "nb_no", "LCAT", "I", "Signal"), # The same!
("IS", "nb_no", "LCAT", "G", "Signalboks"),
("IS", "nb_no", "LCAT", "X", "Kryssveksel"),
("IS", "nb_no", "LCAT", "R", "Planovergang"),
("IS", "nb_no", "LCAT", "D", "Sidespor"),
("IS", "nb_no", "LCAT", "T", "Depot"), # France has a lot to answer for
("IS", "nb_no", "LCAT", "Q", "Godsterminal"),

("IS", "nb_no", "LCAT", "S", "Hovedlinje stasjon"),
("IS", "nb_no", "LCAT", "M", "Ikke-NR stasjon"),
("IS", "nb_no", "LCAT", "F", "Fergeterminal"),
("IS", "nb_no", "LCAT", "B", "Busstopp"),
])


def category_for(loc: dict) -> Optional[str]:
    # Might have to make this a little nicer at origin at some point, ah well
    corpus = (loc["name_corpus"] or '').upper()
    darwin = (loc["name_darwin"] or '').upper()
    crs_darwin = loc["crs_darwin"]
    tiploc = loc["tiploc"]
    operator = loc["operator"]

    if tiploc in FORCED_CATEGORIES:
        return FORCED_CATEGORIES[tiploc]
    # Signals (which usually have alphanumeric tiplocs) are specifically excluded from this determination because
    # they are not waypoints
    elif tiploc not in BPLAN_NETWORK_LOCATIONS and tiploc not in OBSERVED_LOCATIONS and not tiploc[-1].isnumeric():
        return "Z"

    # Strong and stable operator based determinations
    elif operator == "ZB":
        return "B"
    # If it's only reachable by ship, it's probably a ferry terminal!
    elif BPLAN_NETWORK_LOCATIONS.get(tiploc, set()) in [{"SHI"}, {"SHI", ""}]:
        return "F"
    elif operator == "ZF":
        return "F"
    elif operator in ["TW", "SJ", "NY", "ZM", "PC", "y", "SP"]:
        return "M"
    # LT manages some stations with mainline services, only categorise as metro if the power is 4th rail or ambiguous
    elif operator == "LT" and corpus.endswith("LT"):
        return "M"
    # Bus tiplocs always *end* (but never start) in BUS.
    elif tiploc.endswith("BUS"):
        return "B"
    elif "BUS" in corpus and ("STATION" in corpus or "STN" in corpus) and tiploc.endswith("BS"):
        return "B"

    # If the only way in and out is bus, it's... bus.
    elif BPLAN_NETWORK_LOCATIONS.get(tiploc, set()) in [{"BUS"}, {"BUS", ""}] and "BUS" in darwin:
        return "B"

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
