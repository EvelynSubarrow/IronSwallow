from typing import Optional

from ironswallow.bplan import BPLAN_NETWORK_LOCATIONS, LOCALISED_OTHER_REFERENCES
from ironswallow.store.darwin import OBSERVED_LOCATIONS

FORCED_CATEGORIES = {
    "WEST530": "I",  # For some inexplicable reason Westerton Sig YH350 has a CRS assigned in Darwin
    "GROSNYM": "M",  # Grosmont North Yorkshire Moors Railway
    "YOKERCS": "T",  # Yoker Stabling & Cleaning - I guess this makes a bit more sense but still not really

    "ELGHTMD": "T",  # Eastleigh TRSMD
    "ILFEMUD": "T",  # Ilford EMUD,
    "SLHRSTD": "T",  # Selhurst TRSMD

    "THBDTGE": "D",  # Tilgate Sidings Entry/exit
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
("IS", "en_gb", "LCAT", "V", "Viaduct"),

("IS", "en_gb", "LCAT", "S", "Mainline station"),
("IS", "en_gb", "LCAT", "M", "Non-NR station"),
("IS", "en_gb", "LCAT", "F", "Ferry terminal"),
("IS", "en_gb", "LCAT", "B", "Bus stop"),


("IS", "nb_no", "LCAT", "Z", "Koplet fra jernbanenettverk"),

("IS", "nb_no", "LCAT", "J", "Sporvekseler"), # "Knutepunkt" er kanskje bedre
("IS", "nb_no", "LCAT", "I", "Signal"), # The same!
("IS", "nb_no", "LCAT", "G", "Signalboks"),
("IS", "nb_no", "LCAT", "X", "Kryssveksel"),
("IS", "nb_no", "LCAT", "R", "Planovergang"),
("IS", "nb_no", "LCAT", "D", "Sidespor"),
("IS", "nb_no", "LCAT", "T", "Depot"), # France has a lot to answer for
("IS", "nb_no", "LCAT", "Q", "Godsterminal"),
("IS", "nb_no", "LCAT", "V", "Viadukt"),

("IS", "nb_no", "LCAT", "S", "Hovedlinje stasjon"),
("IS", "nb_no", "LCAT", "M", "Ikke-NR stasjon"),
("IS", "nb_no", "LCAT", "F", "Fergeterminal"),
("IS", "nb_no", "LCAT", "B", "Busstopp"),
])

def _unbracketise(name: str) -> str:
    name = name.rstrip()
    if name.endswith(")") or ("(" in name and ")" not in name):
        name = name.rsplit("(", 1)[0].rstrip()
    if name.upper().endswith(" GBRF"):
        name = name[:-5]
    return name

def category_for(loc: dict) -> Optional[str]:
    # Might have to make this a little nicer at origin at some point, ah well
    bplan_orig = (loc["name_bplan"] or '').rstrip()
    corpus_orig = (loc["name_corpus"] or '').rstrip()

    corpus = _unbracketise(corpus_orig)

    bplan = _unbracketise(bplan_orig.upper()) # Uppercased to make it more equivalent to CORPUS

    # utter nightmare sorry, this is really just for differentiating and pulling acronyms from BPLAN
    bplan_nbc = _unbracketise(bplan_orig).replace(".", "")

    netr_name = (bplan or corpus).replace(".", "")

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
    elif ("SIGNAL" in netr_name or "SIG" in netr_name) and tiploc[-1].isnumeric():
        return "I"
    elif corpus.endswith("SIGNAL") or corpus.endswith("SIG") or netr_name.endswith("STOP BOARD"):
        return "I"
    elif ("SIGNAL BOX" in netr_name or "SIGNALBOX" in netr_name or netr_name.endswith(" GROUND FRAME")
          or netr_name.endswith(" SB") or netr_name.endswith(" GF")):
        return "G"
    elif "CROSSOVER" in netr_name or "XOVER" in netr_name:
        return "X"
    elif "AHB" in netr_name:
        return "R"
    elif netr_name.endswith("LEVEL CROSSING") or netr_name.endswith(" L XING") or netr_name.endswith(" LC"):
        return "R"
    elif (netr_name.endswith(" FD") or netr_name.endswith(" CCD") or netr_name.endswith("FLT") or
          netr_name.endswith("CHP") or netr_name.endswith(" CT") or netr_name.endswith(" TERMINAL") or
          netr_name.endswith(" TERM")):
        return "Q"
    elif (netr_name.endswith("EMUD") or netr_name.endswith("DMUD") or netr_name.endswith("TMD") or
          netr_name.endswith("DEPOT") or netr_name.endswith("CARMD") or netr_name.endswith("RSMD") or
          netr_name.endswith("EMD") or netr_name.endswith("LMD") or bplan_nbc.endswith(" LIP") or
          netr_name.endswith("H S T D") or netr_name.endswith("HSTD") or netr_name.endswith("WRD") or
          netr_name.endswith("WRCS")):
        return "T"
    elif (netr_name.endswith("SDG") or netr_name.endswith("SDGS") or netr_name.endswith("SIDING") or
          netr_name.endswith("SIDINGS") or netr_name.endswith(" CS") or netr_name.endswith("CHS") or
          netr_name.endswith("WHS") or netr_name.endswith(" RS") or netr_name.endswith(" EXS") or
          netr_name.endswith("RECEPTION") or netr_name.endswith("RECP") or netr_name.endswith(" SS")):
        return "D"
    elif (netr_name.endswith("JN") or netr_name.endswith("JUNCTION") or netr_name.endswith("JCN") or
          netr_name.endswith("JCT")):
        return "J"
    elif corpus.endswith("LOOP"):
        return "L"
    else:
        return None
