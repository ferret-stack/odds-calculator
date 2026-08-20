"""
.env loading and odds-api.com key resolution/failover.

Split out of odds_calculator.py so the failover logic -- the part that
actually matters, and the part most likely to be exercised by a real quota
hit rather than a test -- can be tested without importing pandas, scipy or
selenium, none of which this needs.

WHY A HAND-ROLLED .env LOADER
------------------------------
python-dotenv would do this in one line, but it is not otherwise a
dependency of this repo (there is no requirements.txt), and the format
needed here -- KEY=VALUE lines, optional quotes, # comments -- is small
enough that adding a dependency for it is not worth it. Same call made about
GeckoDriverManager in scrapers/browser.py: no network/package cost paid by
callers that do not need the thing.

WHY TWO KEYS
------------
Three keys were previously hardcoded directly in odds_calculator.py and are
public in this repo's git history (flagged Day 1, carried over at Day 2 --
rotate them). Only two are kept going forward, on purpose: a primary and a
single failsafe. the-odds-api.com's tier here has a monthly request quota,
and this script is run against it repeatedly, so ODDS_API_KEY_BACKUP exists
to fall over automatically the moment the primary is exhausted rather than
stopping the pipeline mid-run or mid-matchweek. See .env.example.
"""

import os
from pathlib import Path


def load_dotenv(path=None):
    """
    Load KEY=VALUE pairs from a .env file into os.environ.

    A key already set in the real environment is never overridden -- a
    shell export or CI secret always wins over a stray .env file, matching
    standard dotenv behaviour. A missing file is not an error: .env is
    meant to be optional (copy .env.example to make one), and every value
    read from it has a real-environment fallback anyway.
    """
    path = Path(path) if path else Path(__file__).resolve().parent / '.env'
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, _, value = line.partition('=')
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def odds_api_keys():
    """
    The odds-api.com keys to try, in priority order.

    ODDS_API_KEY is the primary; ODDS_API_KEY_BACKUP is only reached once
    the primary reports itself exhausted (see request_with_key_failover()).
    Either or both may be unset -- an empty list means "not configured",
    which the caller reports rather than crashing on.
    """
    keys = []
    primary = os.environ.get('ODDS_API_KEY')
    if primary:
        keys.append(primary)
    backup = os.environ.get('ODDS_API_KEY_BACKUP')
    if backup:
        keys.append(backup)
    return keys


def quota_exhausted(status_code, body):
    """
    True when the-odds-api is saying THIS KEY specifically is done.

    429 is an unambiguous rate limit. the-odds-api returns 401 for both an
    exhausted monthly quota AND a plain bad/revoked key, distinguished only
    by `error_code` in the JSON body -- so a 401 counts as quota exhaustion
    only when the body actually says OUT_OF_USAGE_CREDITS. Any other 401 (a
    genuinely bad key) fails identically on the backup key, so it is
    reported as-is rather than masked by a failover that cannot fix it.

    `body` is already-parsed JSON (a dict) or None, so this is pure and
    testable against the exact response shapes the API documents without a
    network call.
    """
    if status_code == 429:
        return True
    if status_code == 401 and isinstance(body, dict):
        return str(body.get('error_code', '')).upper() == 'OUT_OF_USAGE_CREDITS'
    return False


def request_with_key_failover(get, url, params, keys, start_at=0):
    """
    Call get(url, params={**params, 'apiKey': key}) trying keys[start_at:]
    in order, advancing past any key whose response reports quota_exhausted().

    `get` is injected (normally requests.get) so this is testable with a
    fake that returns canned responses -- no network, no real key needed.

    Returns (response, index). `response` is the last one obtained: a
    success, or the final key's failure once every key is exhausted.
    `index` is which key produced it -- callers making several requests in a
    loop should pass the returned index back in as `start_at` on the next
    call. Once a key is known dead for this run, there is no reason to spend
    another request re-confirming that on every later call in the same loop.

    keys=[] returns (None, start_at) without calling `get` at all -- there is
    nothing to try.
    """
    response = None
    index = start_at
    for index in range(start_at, len(keys)):
        response = get(url, params={**params, 'apiKey': keys[index]})
        try:
            body = response.json()
        except ValueError:
            body = None
        if not quota_exhausted(response.status_code, body):
            return response, index
        if index + 1 < len(keys):
            print(f'  ⚠ API key {index + 1} is rate-limited or out of quota -- '
                  f'switching to the backup key')
    return response, index
