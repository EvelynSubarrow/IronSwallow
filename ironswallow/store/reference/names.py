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
    "BRDS",       # -Birmingham Bordesley
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

RE_SUBSTITUTIONS = [
    (r"Jcn", "Junction", 0),
    (r"Jn", "Junction", 0),
    (r"Rd", "Road", 1),
    (r"\([Tt]ps Indic\. Only\)", "", 1),
    (r"\(tps Indic\. Only\)", "", 1),
    (r"Intl", "International", 1),
    (r"Internat'nl", "International", 1),  # THANK YOU GLASGOW PRESTWICK
    (r"H\.?[Ll]\.?$", "High Level", 1),
    (r"L\.?[Ll]\.?$", "Low Level", 1),
    (r"Lt$", "LT", 1),
    (r"I\.o\.w\.?", "Isle of Wight", 1),
    (r"I\.o\.m\.?", "Isle of Man", 1),
    (r"D\.c\.", "D.C.", 1),
    (r"Dc", "D.C.", 1),
    (r"A\.c", "A.C.", 1),
    (r"\.\.", ".", 1),  # Some of these are a bit ambiguous
    (r"Mt\.", "Mount", 1),
    (r"And ", "& ", 1),
    (r"Sig ", "Signal ", 1),
    (r"Ell$", "East London Line", 1)
]

RE_SUBSTITUTIONS_PATTERNS = tuple((re.compile(p), s, v) for p, s, v in RE_SUBSTITUTIONS)



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
    if loc["name_corpus"] is None:
        return loc["name_darwin"], loc["name_darwin"]

    corpus = loc["name_corpus"]
    darwin = loc["name_darwin"]

    corpus_cased = _case(corpus)
    if loc["tiploc"] in FORCE_DARWIN_NAMES:
        corpus_cased = darwin


    expanded = corpus_cased
    for pattern, sub, verbosity in RE_SUBSTITUTIONS_PATTERNS:
        if pattern.search(expanded):
            expanded_before = expanded
            expanded = pattern.sub(sub, expanded)
            cursor.execute("INSERT INTO swallow_debug VALUES ('NSUB', %s, %s, %s, %s);", (loc["tiploc"], pattern.pattern, datetime.utcnow(), expanded_before + " -> " + expanded))


    return corpus_cased, expanded
