import re
import unicodedata
from rapidfuzz import process, fuzz
from pipeline.categories import get_training_keywords

_CANON_THRESHOLD = 80

_CANON_ALIASES = [
    "aldi technology support",
    "manda konsulting",
    "joanna kolodzinska",
    "ing bank śląski",
]

_LEGAL = re.compile(
    r'\b(spółka z ograniczoną odpowiedzialnością|'
    r'sp\.?\s*z\s*o\.?\s*o\.?|s\.?a\.?|sp\.?\s*[jk]\.?|a/s|gmbh|ltd|inc|llc)\b',
    re.IGNORECASE,
)
_POSTAL = re.compile(r'\b\d{2}-\d{3}\b|\b\d{5}\b')
_PHONE = re.compile(r'[+\d][\d\s]{6,}')
_URLISH = re.compile(r'\S+\.(eu|pl|com|de)\S*')
_CITIES = re.compile(
    r'\b(krak[oó]w[a]?|warsaw|warszawa|katowice|sopot|balice|pozna[ńn]|'
    r'frankfurt am main|copenhagen|amsterdam|issy-les-moul|issy-les-)\b',
    re.IGNORECASE,
)
_ADDR = re.compile(r'\b(ul\.|al\.?|street|str\.|via)\s+.*$', re.IGNORECASE)
_TAIL_COUNTRY = re.compile(
    r'\s+\b(pol|pl|po|fra|usa|de|n)\b\s*$', re.IGNORECASE)
_WS = re.compile(r'\s+')

_POLISH_MAP = str.maketrans({'ł': 'l', 'Ł': 'L'})


def _strip_diacritics(s: str) -> str:
    s = s.translate(_POLISH_MAP)
    return ''.join(
        c for c in unicodedata.normalize('NFKD', s)
        if not unicodedata.combining(c)
    )


def _match_keyword_strict(text: str) -> str | None:
    text = text or ""

    candidates = _CANON_ALIASES + get_training_keywords()
    
    hits = process.extract(
        text, candidates,
        scorer=fuzz.token_set_ratio,
        score_cutoff=_CANON_THRESHOLD,
        limit=None,
    )
    if not hits:
        return None
    
    hits.sort(key=lambda h: (-h[1], -len(h[0])))
    return hits[0][0]


def _clean_counterparty(raw: str) -> str:
    if not raw:
        return ''
    s = raw.lower()
    s = _URLISH.sub(' ', s)
    s = _PHONE.sub(' ', s)
    s = _ADDR.sub(' ', s)
    s = _LEGAL.sub(' ', s)
    s = _POSTAL.sub(' ', s)
    s = _CITIES.sub(' ', s)
    s = _TAIL_COUNTRY.sub(' ', s)
    s = _strip_diacritics(s)
    s = _WS.sub(' ', s).strip(' ,.-')
    return s


def canonical_counterparty(raw: str) -> str:
    """Fuzzy keyword if known, else regex-cleaned raw. Output is always ASCII."""
    if not raw:
        return ''
    s = _clean_counterparty(raw)
    kw = _match_keyword_strict(raw)
    if kw:
        return _strip_diacritics(kw)
    return s