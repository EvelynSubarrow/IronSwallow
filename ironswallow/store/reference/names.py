from typing  import Optional
import string, re
from datetime import datetime

FORCE_DARWIN_NAMES = {
    "RAINHMK",    # Rainham Kt→(Kent)
    "SLFDORD",    # Salford +Central
    "OXTEDBY",    # Oxted -Bay
    "STFD",       # Stratford +(London)
    "APDR",       # Appledore +(Kent)

    "ASFDMSX",    # Ashford -(Middlesex)+(Surrey)
    "ASHFKY",     # Ashford -(Kent)+International
    "BCSTN",      # Bicester +North
    "BLNGHM",     # Billingham -on Tees
    "BMLY",       # Bramley -(Hants)+(Hampshire)
    "BNTEY",      # Bentley +(Hampshire)
    "BOGSTON",    # Bogston -Port Glasgow
    "BRDS",       # -Birmingham Bordesley
    "BRGHLRD",    # London Road -Brighton+(Brighton)
    "BTLYSY",     # Bentley -(S. Yorks)+(South Yorkshire)
    "CALAFGV",    # +Calais Frethun
    "COOMBE",     # Coombe -(Cornwall)+Coombe Junction Halt
    "EBSFLTI",    # Ebbsfleet +International
    "ELTR",       # Elstree + & Borehamwood
    "FSHBORN",    # Fishbourne +(West Sussex)
    "HTRWAPT",    # Heathrow Express Terminal One -> Heathrow Airport T123
    "HTRWTM5",    # Heathrow T5 -> Heathrow Airport T5
    "LEIGHK",     # Leigh +(Kent)
    "LIVSTLL",    # Liverpool Street +Low Level
    "LTCE",       # Letchworth +Garden City
    "MNCRUFG",    # Manchester United -"Football Gd"+FC
    "OXENHLM",    # Oxenholme +Lake District
    "RANNOCH",    # Rannoch -For Kinloch Rannoch


    "PADTLL",     # Paddington +Low Level - It's a separate station per Darwin!

    "ABWDXR",     # Abbey wood -MTR+(Crossrail)
    "WCHAPXR",    # Whitechapel -MTR+(Crossrail)
    "FRNDXR",     # Farringdon -MTR+(Crossrail)
}

# Here mostly to deal with depots, the names of which are so wrong that they cannot be sourced or replaced or made nice
FULL_NAME_SUBSTITUTIONS = {

}

CORPUS_RE_SUBSTITUTIONS = [
    (r"\s+", " ", 0),

    (r"J(n|cn|ct|unction)", "Junction", 0),
    (r"R(oa)?d", "Road", 1),
    (r"\([Tt]ps Indic\. Only\)", "", 1),
    (r"\(tps Indic\. Only\)", "", 1),
    (r"Int(ernat'n)?l", "International", 1),
    (r"H\.?[Ll]\.?$", "High Level", 1),
    (r"L\.?[Ll]\.?$", "Low Level", 1),
    (r"Lt$", "LT", 1),
    (r"I\.o\.w\.?", "Isle of Wight", 1),
    (r"I\.o\.m\.?", "Isle of Man", 1),
    (r"D\.c\.", "D.C.", 1),
    (r"Dc$", "D.C.", 1),
    (r"A\.c", "A.C.", 1),
    (r"\.\.", ".", 1),  # Some of these are a bit ambiguous
    (r"Mt\.", "Mount", 1),
    (r"And ", "& ", 1),
    (r"Sig ", "Signal ", 1),
    (r"Ell$", "East London Line", 1),

    # Sidings
    (r"Cs$", "Carriage Sidings", 1),
    (r"Sdg", "Siding", 1),

    # TOCs/FOCs
    (r"Gbrf", "GBRf", 1),
    (r"Ews", "EWS", 1),

    # Depots etc
    (r"E\.?m\.?u\.?d\.?", "Electric Multiple Unit Depot", 1),
    (r"D\.?m\.?u\.?", "Diesel Multiple Unit", 1),
    (r"E\.?m\.?u\.?", "Electric Multiple Unit", 1),
    (r"T\.?m\.?d\.?", "Traction Maintenance Depot", 1),
    (r"M&ee", "Mechanical & Electrical Engineering", 1),
    (r"Carmd", "Carriage Maintenance Depot", 1),
    (r"T&rs\s?[Mm]d", "Traction & Rolling Stock Maintenance Depot", 1),
]

CORPUS_RE_SUBSTITUTIONS_PATTERNS = tuple((re.compile(p), s, v) for p, s, v in CORPUS_RE_SUBSTITUTIONS)

BPLAN_RE_SUBSTITUTIONS = [
    (r"J(n|cn|ct|unction)",             "Junction",                                       "Jn",       0),

    (r"E\.?M\.?U\.?D\.?",               "Electric Multiple Unit Depot",                   "EMUD",     1),
    (r"D\.?M\.?U\.?",                   "Diesel Multiple Unit",                           "DMU",      1),
    (r"E\.?M\.?U\.?",                   "Electric Multiple Unit",                         "EMU",      1),
    (r"T\.?M\.?D\.?",                   "Traction Maintenance Depot",                     "TMD",      1),
    (r"T\.?&[\.\s]?R\.?S\.?M\.?D\.?",   "Traction & Rolling Stock Maintenance Depot",     "TRSMD",    1),
    (r"P Way",                          "Permanent Way",                                  "Pmt. way", 1),
    (r"Car\. M\.D\.?",                  "Carriage Maintenance Depot",                     "CMD",      1),

    (r"Sdg",                            "Siding",                                         "Sdg",      1),
    (r"C\?.H\?.S\?.",                   "Carriage Holding Sidings",                       "CHSdg",    1),
]

BPLAN_RE_SUBSTITUTIONS_PATTERNS = tuple((re.compile(p), f, s, v) for p, f, s, v in BPLAN_RE_SUBSTITUTIONS)


def _case(st: Optional[str]) -> Optional[str]:
    if st is None: return None
    out = []

    for word in st.split(" "):
        if word.startswith("(") and len(word) > 1:
            out.append("(" + string.capwords(word[1:]))
            # Low level, East London Line, Sig→SIG eliminates ambiguity
        elif word[-1:].isnumeric() and word.isalnum():
            out.append(word.upper())
        else:
            out.append(string.capwords(word))

    return " ".join(out)

def name_for(loc: dict, cursor) -> tuple:
    if loc["name_bplan"] is None:
        return loc["name_darwin"], loc["name_darwin"]

    corpus = loc["name_corpus"]
    darwin = loc["name_darwin"]
    bplan = loc["name_bplan"]

    corpus_cased = _case(corpus)


    if loc["tiploc"] in FULL_NAME_SUBSTITUTIONS:
        return FULL_NAME_SUBSTITUTIONS[loc["tiploc"]]

    corpus_expanded = corpus_cased
    for pattern, sub, verbosity in CORPUS_RE_SUBSTITUTIONS_PATTERNS:
        if pattern.search(corpus_expanded):
            expanded_before = corpus_expanded
            corpus_expanded = pattern.sub(sub, corpus_expanded)
            cursor.execute("INSERT INTO swallow_debug VALUES ('NSUB', %s, %s, %s, %s) ON CONFLICT DO NOTHING;", (loc["tiploc"], pattern.pattern, datetime.utcnow(), expanded_before + " -> " + corpus_expanded))

    bplan_short = bplan
    bplan_full = bplan
    for pattern, sub_full, sub_short, verbosity in BPLAN_RE_SUBSTITUTIONS_PATTERNS:
        if pattern.search(bplan_full):
            expanded_before = bplan_full
            bplan_full = pattern.sub(sub_full, bplan_full)
            bplan_short = pattern.sub(sub_short, bplan_short)
            cursor.execute("INSERT INTO swallow_debug VALUES ('BSUS', %s, %s, %s, %s) ON CONFLICT DO NOTHING;", (loc["tiploc"], pattern.pattern, datetime.utcnow(), expanded_before + " -> " + bplan_full))


    return bplan_short or corpus_expanded, bplan_full or corpus_expanded
