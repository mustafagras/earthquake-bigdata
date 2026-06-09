"""
Makine öğrenmesi adımı: depremleri KMeans ile sismik kümelere ayırır.

Streaming işinin parquet'e yazdığı her şeyi okur, depremleri konum ve büyüklüğe
göre Spark MLlib kullanarak kümeler ve etiketlenmiş satırları görselleştirme
adımı için ``reports/clustered.csv`` dosyasına kaydeder.

Tek başına çalıştırma:
    python ml_clustering.py
"""
import logging

import common  # ortamı ve PySpark'ı yapılandırır – önce import edilmeli
import config

from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.clustering import KMeans

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ML] %(levelname)s: %(message)s",
)
logger = logging.getLogger("ml_clustering")


def run() -> int:
    """Toplanan depremleri kümeler. Kümelenen satır sayısını döndürür."""
    spark = common.get_spark("EarthquakeKMeans")

    if not config.OUTPUT_DIR.exists():
        logger.error("%s konumunda veri yok. Önce streaming işini çalıştırın.", config.OUTPUT_DIR)
        return 0

    df = spark.read.parquet(str(config.OUTPUT_DIR))
    # Kullanılabilir koordinatı/büyüklüğü olmayan kayıtları çıkar.
    df = df.na.drop(subset=["latitude", "longitude", "magnitude"]).filter(
        (col("latitude") != 0.0) | (col("longitude") != 0.0)
    )

    total = df.count()
    if total == 0:
        logger.error("Kümelenecek geçerli deprem kaydı yok.")
        return 0

    # KMeans için k <= nokta sayısı olmalı; küçük veri kümelerinde k'yı küçült.
    k = max(1, min(config.KMEANS_K, total))
    logger.info("%d deprem %d kümeye ayrılıyor...", total, k)

    assembler = VectorAssembler(
        inputCols=["latitude", "longitude", "magnitude"], outputCol="features_raw"
    )
    # Büyüklüğün enlem/boylam ölçeği yanında ezilmemesi için standartlaştır.
    scaler = StandardScaler(inputCol="features_raw", outputCol="features")
    kmeans = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster")
    model = Pipeline(stages=[assembler, scaler, kmeans]).fit(df)

    result = model.transform(df).select(
        "earthquake_id", "provider", "title", "magnitude", "depth",
        "latitude", "longitude", "region", "severity", "event_time", "cluster",
    )

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    # Pandas'a topla (küçük, demo boyutunda) ve tek bir düzenli CSV yaz.
    result.toPandas().to_csv(config.CLUSTERED_CSV, index=False)
    logger.info("Kümelenmiş veri kaydedildi -> %s", config.CLUSTERED_CSV)

    # Konsol için küme başına kısa bir özet yazdır.
    logger.info("Küme boyutları:")
    result.groupBy("cluster").count().orderBy("cluster").show()

    return total


if __name__ == "__main__":
    run()
