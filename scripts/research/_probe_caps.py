"""Same transport, two byte caps: 4 MiB (what the fetcher uses) vs 8 MiB."""
import logging, sys, time
sys.path.insert(0, "/opt/airflow")
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
from scrapers.fbref.camoufox_fetch import CamoufoxFbrefTransport
from scrapers.utils.proxy_manager import ProxyManager

cap_mib = int(sys.argv[1])
pm = ProxyManager(rotation_strategy="random")
pm.load_from_file_custom_format("/opt/airflow/proxys.txt")

def next_proxy():
    p = pm.get_proxy()
    proxy = {"server": f"{p.proxy_type.value}://{p.host}:{p.port}"}
    if p.username:
        proxy["username"] = p.username
        proxy["password"] = p.password or ""
    return proxy

t = CamoufoxFbrefTransport(proxy_provider=next_proxy, geoip=True, headless=True,
                           humanize=True, block_resources=True,
                           max_network_requests=40,
                           max_network_bytes=cap_mib * 1024 * 1024)
t0 = time.time()
html = t.fetch("https://fbref.com/en/comps/")
print(f"RESULT cap={cap_mib}MiB elapsed={time.time()-t0:.1f}s html={len(html) if html else None}", flush=True)
s = t.traffic_stats()
print("STATS", {k: s[k] for k in ("real_bytes_downloaded","budget_blocked_count","byte_budget_exhausted","byte_budget_failure","browser_navigation_attempts") if k in s}, flush=True)
t.close()
