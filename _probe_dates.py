import requests
from datetime import date, timedelta
from pathlib import Path


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


e = load_env(Path(".env"))
aid, key = e["CALLRAIL_ACCOUNT_ID"], e["CALLRAIL_API_KEY"]
h = {"Authorization": f'Token token="{key}"'}
end = date.today()
start = end - timedelta(days=729)
r = requests.get(
    f"https://api.callrail.com/v3/a/{aid}/calls.json",
    params={
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "per_page": 1,
    },
    headers=h,
    timeout=60,
)
print(start, end, r.status_code, r.text[:200] if r.status_code != 200 else "ok")
