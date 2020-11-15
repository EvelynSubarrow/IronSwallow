from typing  import Optional
import string


def _case(st: Optional[str]) -> Optional[str]:
    if st is None: return None
    out = []

    for word in st.split(" "):
        if word.startswith("(") and len(word) > 1:
            out.append("(" + string.capwords(word[1:]))
        else:
            out.append(string.capwords(word))

    return " ".join(out)

def name_for(loc: dict) -> tuple:
    if loc["name_corpus"] is None:
        return loc["name_darwin"], loc["name_darwin"]

    corpus = loc["name_corpus"]
    darwin = loc["name_darwin"]

    corpus_cased = _case(corpus)

    expanded = corpus_cased
    expanded = expanded.replace("Jcn", "Junction")
    expanded = expanded.replace("Jn", "Junction")
    expanded = expanded.replace("Rd", "Road")
    expanded = expanded.replace(" (Tps Indic. Only)", "")
    expanded = expanded.replace("Intl", "International")
    expanded = expanded.replace("Internat'nl", "International") # THANK YOU GLASGOW PRESTWICK VERY COOL
    expanded = expanded.replace("Hl", "High Level")
    expanded = expanded.replace("Lt", "LT")

    return corpus_cased, expanded
