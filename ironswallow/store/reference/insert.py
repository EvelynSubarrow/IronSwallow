from collections import OrderedDict
import json

from main import REASONS, LOCATIONS
from . import category
from . import names

from ironswallow.bplan import LOCALISED_OTHER_REFERENCES

LOCALISED_OTHER_REFERENCES.extend([
    ("IS", "en_gb", "OPCAT", "S", "Mainline operator"),
    ("IS", "en_gb", "OPCAT", "M", "Non-NR operator"),
    ("IS", "en_gb", "OPCAT", "O", "Non-rail operator"),
    ("IS", "en_gb", "OPCAT", "C", "Charter operator"),
    ("IS", "en_gb", "OPCAT", "B", "BPLAN operator (do not use)"),

    ("IS", "nb_no", "OPCAT", "S", "Hovedlinje togoperatør"),
    ("IS", "nb_no", "OPCAT", "M", "Ikke-NR togoperatør"),
    ("IS", "nb_no", "OPCAT", "O", "Ikke-jernbane operatør"),
    ("IS", "nb_no", "OPCAT", "C", "Chartertogoperatør"),
    ("IS", "nb_no", "OPCAT", "B", "Operatør fra BPLAN (bruk ikke)")
    ])

def toc_category_for(toc):
    if toc in ["NY", "PC", "ZM", "WR"]:
        return "C"
    if toc in ["LT", "SJ", "TW"]:
        return "M"
    if toc in ["ZB", "ZF"]:
        return "O"
    return "S"

def store(c, parsed) -> None:
    strip = lambda x: x.rstrip() or None if x else None

    c.execute("DELETE FROM swallow_debug WHERE subsystem='NSUB';")

    with open("datasets/corpus.json", encoding="iso-8859-1") as f:
        corpus = json.load(f)["TIPLOCDATA"]
    corpus = {a["TIPLOC"]: a for a in corpus}

    for reference in parsed["PportTimetableRef"]["list"]:
        if reference["tag"] == "LocationRef":
            corpus_loc = corpus.get(reference["tpl"], {})

            loc = OrderedDict([
                ("tiploc", reference["tpl"]),
                ("crs_darwin", reference.get("crs")),
                ("crs_corpus", strip(corpus_loc.get("3ALPHA"))),
                ("operator", reference.get("toc")),
                ("name_darwin", reference["locname"]*(reference["locname"]!=reference["tpl"]) or None),
                ("name_corpus", strip(corpus_loc.get("NLCDESC"))),
                ("category", None)
                ])
            loc.update(OrderedDict([
                ("name_short", loc["name_darwin"] or loc["name_corpus"]),
                ("name_full", loc["name_corpus"] or loc["name_darwin"]),
                ]))

            loc["category"] = category.category_for(loc)
            loc["name_short"], loc["name_full"] = names.name_for(loc, c)

            c.execute("""INSERT INTO darwin_locations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(tiploc) DO UPDATE SET
                (tiploc, crs_darwin, crs_corpus, operator, name_darwin, name_corpus, category, name_short, name_full)=
                (EXCLUDED.tiploc,EXCLUDED.crs_darwin,EXCLUDED.crs_corpus,
                EXCLUDED.operator,EXCLUDED.name_darwin,EXCLUDED.name_corpus, EXCLUDED.category,
                EXCLUDED.name_short, EXCLUDED.name_full);
                """, (loc["tiploc"], loc["crs_darwin"], loc["crs_corpus"], loc["operator"],
                    loc["name_short"], loc["name_full"],
                    json.dumps(loc), loc["category"], loc["name_darwin"], loc["name_corpus"]))

            LOCATIONS[reference["tpl"]] = loc

        if reference["tag"]=="TocRef":
            c.execute("""INSERT INTO darwin_operators VALUES (%s, %s, %s, %s) ON CONFLICT (operator)
                DO UPDATE SET (operator_name, url, category)=(EXCLUDED.operator_name, EXCLUDED.url, EXCLUDED.category);""",
                (reference["toc"], reference["tocname"], reference.get("url"), toc_category_for(reference["toc"])))

        if reference["tag"] in ["CancellationReasons", "LateRunningReasons"]:
            reason_type = "C"*(reference["tag"]=="CancellationReasons") or "D"
            for reason in reference["list"]:
                if reason["tag"]=="Reason":
                    c.execute("""INSERT INTO darwin_reasons VALUES (%s, %s, %s) ON CONFLICT (id, type) DO UPDATE
                        SET (type, message)=(EXCLUDED.type, EXCLUDED.message);""",
                        (reason["code"], reason_type, reason["reasontext"]))
                    REASONS[(reason["code"], reason_type)] = reason["reasontext"]

    c.execute("COMMIT;")
