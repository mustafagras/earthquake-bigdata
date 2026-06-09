"""
Deprem Büyük Veri Pipeline'ı için merkezi yapılandırma.

Veri kaynağını, demo'nun ne kadar süre çalışacağını, dosyaların nereye
yazılacağını veya ML küme sayısını değiştirmek için buradaki değerleri düzenleyin.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Veri kaynağı – Kandilli Rasathanesi (orhanaydogdu açık API'si üzerinden)
# ---------------------------------------------------------------------------
API_BASE_URL = "https://api.orhanaydogdu.com.tr/deprem"
API_PROVIDER = "kandilli"            # "kandilli" veya "afad"

# /live ucu yalnızca son 24 saati döndürür ve çoğu zaman boştur. Demo'nun her
# zaman öğrenecek verisi olması için producer, ilk yoklamada /archive ucundan
# son ARCHIVE_BACKFILL_DAYS günü geri doldurur (backfill).
ARCHIVE_BACKFILL_DAYS = 7

# ---------------------------------------------------------------------------
# Streaming davranışı
# ---------------------------------------------------------------------------
FETCH_INTERVAL_SECONDS = 10          # producer API'yi her N saniyede bir yoklar
TRIGGER_SECONDS = 5                  # Spark micro-batch aralığı
STREAM_DURATION_SECONDS = 60         # demo, ML/görselleştirmeden önce ne kadar stream yapar

# ---------------------------------------------------------------------------
# Dosya sistemi yerleşimi (data/ ve reports/ altındaki her şey yeniden üretilir)
# ---------------------------------------------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
STREAM_INPUT_DIR = DATA_DIR / "stream_input"        # producer -> JSON dosyaları
OUTPUT_DIR = DATA_DIR / "output" / "earthquakes"    # Spark -> parquet
CHECKPOINT_DIR = DATA_DIR / "checkpoint"            # Spark streaming durumu
REPORTS_DIR = PROJECT_ROOT / "reports"              # ML sonuçları + PNG grafikler
CLUSTERED_CSV = REPORTS_DIR / "clustered.csv"

# ---------------------------------------------------------------------------
# Makine öğrenmesi – depremlerin KMeans ile kümelenmesi
# ---------------------------------------------------------------------------
KMEANS_K = 5                         # bulunacak sismik küme sayısı

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
SPARK_APP_NAME = "EarthquakeBigDataPipeline"
SPARK_MASTER = "local[*]"            # Spark'ı tüm CPU çekirdeklerini kullanarak yerelde çalıştır
