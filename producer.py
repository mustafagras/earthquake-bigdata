"""
Producer: Kandilli Rasathanesi API'sinden deprem verisi çeker ve ``data/
stream_input/`` içine satır-bazlı JSON (newline-delimited JSON) dosyaları yazar.

Bu, streaming pipeline'ının "kaynağı"dır. Spark bu klasörü izler ve ortaya çıkan
her yeni dosyayı işler. Veriyi (ağ soketi yerine) dosyalara yazmak, pipeline'ı
hem Windows hem macOS üzerinde güvenilir kılan şeydir.

Tek başına çalıştırma:
    python producer.py                 # sonsuza dek stream (durdurmak için Ctrl+C)
    python producer.py --duration 60   # 60 saniye stream yapıp çık
"""
import argparse
import json
import logging
import time
from datetime import datetime, timedelta

import requests

# İşletim sistemi sertifika deposuna güven (kurumsal proxy / firewall TLS'ini ele alır).
try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Producer] %(levelname)s: %(message)s",
)
logger = logging.getLogger("producer")


def _get(session: requests.Session, path: str, params: dict) -> list[dict]:
    """Bir deprem-API ucuna GET isteği atar ve ``result`` listesini döndürür (yoksa [])."""
    url = f"{config.API_BASE_URL}/{path}"
    try:
        resp = session.get(url, params=params, timeout=20)
        resp.raise_for_status()
        result = resp.json().get("result", [])
        return result if isinstance(result, list) else []
    except (requests.RequestException, ValueError) as exc:
        logger.warning("API isteği başarısız (%s): %s", url, exc)
        return []


def fetch_live(session: requests.Session) -> list[dict]:
    """En güncel depremler (son 24 saat). Sakin dönemlerde boş olabilir."""
    return _get(session, f"{config.API_PROVIDER}/live", {"limit": 500})


def fetch_recent_archive(session: requests.Session) -> list[dict]:
    """Her zaman öğrenecek veri olması için son ARCHIVE_BACKFILL_DAYS günü geri doldurur."""
    today = datetime.now()
    start = today - timedelta(days=config.ARCHIVE_BACKFILL_DAYS)
    return _get(
        session,
        f"{config.API_PROVIDER}/archive",
        {
            "date": start.strftime("%Y-%m-%d"),
            "date_end": today.strftime("%Y-%m-%d"),
            "limit": 500,
        },
    )


def normalize(event: dict) -> dict:
    """Bir API kaydını, Spark'ın beklediği düz (flat) şemaya dönüştürür."""
    coords = (event.get("geojson") or {}).get("coordinates") or [0.0, 0.0]
    loc = event.get("location_properties") or {}
    epicenter = (loc.get("epiCenter") or {}).get("name", "")
    closest_city = (loc.get("closestCity") or {}).get("name", "")
    return {
        "earthquake_id": event.get("earthquake_id", ""),
        "provider": event.get("provider", config.API_PROVIDER),
        "title": event.get("title", ""),
        "magnitude": float(event.get("mag") or 0.0),
        "depth": float(event.get("depth") or 0.0),
        "latitude": float(coords[1]) if len(coords) > 1 else 0.0,
        "longitude": float(coords[0]) if len(coords) > 0 else 0.0,
        "region": epicenter or closest_city,
        "event_time": event.get("date_time") or event.get("date", ""),
    }


def write_batch(events: list[dict]) -> None:
    """Olayları, benzersiz adlı bir JSON-satır dosyasına atomik olarak yazar.

    Önce bir ``.tmp`` dosyasına yazarız, sonra adını değiştiririz. Böylece Spark
    yalnızca tamamen yazılmış nihai dosyayı görür – asla yarım yazılmış bir dosyayı görmez.
    """
    config.STREAM_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = int(time.time() * 1000)
    final = config.STREAM_INPUT_DIR / f"quakes_{stamp}.json"
    tmp = final.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev, ensure_ascii=False) + "\n")
    tmp.replace(final)
    logger.info("%d yeni olay yazıldı -> %s", len(events), final.name)


def run(duration: float | None = None) -> None:
    """Producer döngüsü: API'yi yoklar, yeni olayları dosyaya yazar, tekrar eder."""
    session = requests.Session()
    seen: set[str] = set()
    start = time.time()
    first_poll = True

    logger.info("Producer başlatıldı (sağlayıcı=%s).", config.API_PROVIDER)
    while True:
        raw = fetch_live(session)
        if first_poll and len(raw) < 10:
            # Canlı akış sakin -> demo'nun verisi olması için son geçmişi geri doldur.
            logger.info("Canlı akış seyrek; son %d gün arşivden geri dolduruluyor.",
                        config.ARCHIVE_BACKFILL_DAYS)
            raw = raw + fetch_recent_archive(session)
        first_poll = False

        fresh = []
        for ev in raw:
            eid = ev.get("earthquake_id")
            if eid and eid not in seen:
                seen.add(eid)
                fresh.append(normalize(ev))

        if fresh:
            write_batch(fresh)
        else:
            logger.info("Bu turda yeni olay yok.")

        if duration is not None and (time.time() - start) >= duration:
            logger.info("Producer süre sınırına ulaştı; duruyor.")
            return
        time.sleep(config.FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deprem verisi producer'ı.")
    parser.add_argument("--duration", type=float, default=None,
                        help="Çıkmadan önce çalışılacak saniye (varsayılan: sonsuz).")
    args = parser.parse_args()
    try:
        run(duration=args.duration)
    except KeyboardInterrupt:
        logger.info("Producer kullanıcı tarafından durduruldu.")
