from typing  import Optional
import string


def _case(st: Optional[str]) -> Optional[str]:
    return string.capwords(st.lower()) if st else None


def name_for(loc: dict) -> tuple:
    corpus = loc["name_corpus"]
    darwin = loc["name_darwin"]

    corpus_cased = _case(corpus)

    return corpus_cased, corpus_cased
