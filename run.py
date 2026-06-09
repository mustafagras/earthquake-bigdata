"""
Tek komutluk pipeline çalıştırıcı (Windows ve macOS/Linux'ta çalışır).

    python run.py

Aşamalar:
  1. Producer'ı başlat (ayrı süreç) – Kandilli verisini dosyalara çeker.
  2. Spark streaming işini STREAM_DURATION_SECONDS kadar çalıştır – dosyalar -> parquet.
  3. Toplanan depremleri KMeans ile kümele (Spark MLlib).
  4. reports/ içine PNG grafikler üret.

Her çalıştırma temiz başlar: bayat checkpoint'ler ve önceki çıktı temizlenir;
böylece demo tekrar üretilebilir olur.
"""
import logging
import shutil
import subprocess
import sys
import time

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Pipeline] %(levelname)s: %(message)s",
)
logger = logging.getLogger("run")


def _reset_dirs() -> None:
    """Her çalıştırmanın tekrar üretilebilir olması için üretilen dosyaları temizler."""
    for path in (config.STREAM_INPUT_DIR, config.OUTPUT_DIR.parent,
                 config.CHECKPOINT_DIR, config.REPORTS_DIR):
        shutil.rmtree(path, ignore_errors=True)
    for path in (config.STREAM_INPUT_DIR, config.REPORTS_DIR):
        path.mkdir(parents=True, exist_ok=True)


def main() -> int:
    duration = config.STREAM_DURATION_SECONDS
    logger.info("=" * 60)
    logger.info("  Deprem Büyük Veri Pipeline'ı")
    logger.info("  Kandilli API -> Spark Streaming -> KMeans -> grafikler")
    logger.info("=" * 60)

    _reset_dirs()

    # --- Aşama 1: producer (akıştan biraz daha uzun çalışır) -----------------
    logger.info("[1/4] Producer başlatılıyor...")
    producer = subprocess.Popen(
        [sys.executable, str(config.PROJECT_ROOT / "producer.py"),
         "--duration", str(duration + 15)]
    )
    time.sleep(4)  # Spark izlemeye başlamadan önce ilk dosyanın oluşmasını bekle

    # --- Aşama 2: Spark structured streaming ---------------------------------
    logger.info("[2/4] Spark streaming %d saniye çalıştırılıyor...", duration)
    try:
        import streaming_job
        streaming_job.run(duration=duration)
    finally:
        producer.terminate()
        try:
            producer.wait(timeout=10)
        except subprocess.TimeoutExpired:
            producer.kill()

    # --- Aşama 3: makine öğrenmesi -------------------------------------------
    logger.info("[3/4] Depremler KMeans ile kümeleniyor...")
    import ml_clustering
    rows = ml_clustering.run()
    if rows == 0:
        logger.error("Hiç veri toplanmadı; görselleştirme atlanıyor.")
        logger.error("Kandilli API'sine internet bağlantınızı kontrol edip tekrar deneyin.")
        return 1

    # --- Aşama 4: görselleştirme ---------------------------------------------
    logger.info("[4/4] Grafikler üretiliyor...")
    import visualize
    visualize.run()

    logger.info("=" * 60)
    logger.info("Bitti! Grafikleri şurada açın: %s", config.REPORTS_DIR)
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
