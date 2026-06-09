"""
Spark Structured Streaming işi.

Producer'ın ``data/stream_input/`` içine bıraktığı JSON dosyalarını okur, her
depremi zenginleştirir (şiddet etiketi, ayrıştırılmış zaman damgası), micro-
batch'i konsola yazdırır ve sonucu ``data/output/`` altında parquet olarak ekler.

DOSYA kaynağı kullanılır (soket değil). Dosyalar kalıcı olduğundan, tek bir
streaming sorgusu her işletim sisteminde güvenilir biçimde okur – bağlantı zaman
aşımı yok, IPv6 tuhaflıkları yok.

Tek başına çalıştırma:
    python streaming_job.py                # sonsuza dek çalış (durdurmak için Ctrl+C)
    python streaming_job.py --duration 60  # 60 saniye çalışıp dur
"""
import argparse
import logging

import common  # ortamı ve PySpark'ı yapılandırır – önce import edilmeli
import config

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, to_timestamp, when
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Streaming] %(levelname)s: %(message)s",
)
logger = logging.getLogger("streaming_job")

# Açık şema = producer'ın normalize ettiği kayıt. Streaming dosya kaynağı için
# açık bir şema zorunludur (Spark, bir akışta şemayı güvenle çıkaramaz).
SCHEMA = StructType(
    [
        StructField("earthquake_id", StringType()),
        StructField("provider", StringType()),
        StructField("title", StringType()),
        StructField("magnitude", DoubleType()),
        StructField("depth", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("region", StringType()),
        StructField("event_time", StringType()),
    ]
)


def enrich(df: DataFrame) -> DataFrame:
    """Okunabilir bir şiddet etiketi ve gerçek bir zaman damgası sütunu ekler."""
    df = df.withColumn(
        "severity",
        when(col("magnitude") < 2.0, "MINOR")
        .when(col("magnitude") < 3.0, "LIGHT")
        .when(col("magnitude") < 4.0, "MODERATE")
        .when(col("magnitude") < 5.0, "STRONG")
        .otherwise("MAJOR"),
    )
    # API zamanları ya "2026-06-02 00:34:05" ya da "2026.06.02 ..." biçiminde
    # verir; mümkün olduğunda event_ts'in her zaman dolması için iki biçimi de dene.
    df = df.withColumn(
        "event_ts",
        coalesce(
            to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("event_time"), "yyyy.MM.dd HH:mm:ss"),
        ),
    )
    return df


def run(duration: float | None = None) -> None:
    """Streaming sorgularını başlatır; verilen süre kadar (veya sonsuza dek) çalışır."""
    spark = common.get_spark()
    config.STREAM_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    stream = enrich(
        spark.readStream.format("json")
        .schema(SCHEMA)
        .option("maxFilesPerTrigger", 1)   # micro-batch başına bir dosya = anlaşılır demo
        .load(str(config.STREAM_INPUT_DIR))
    )

    # YALNIZCA native sink'ler (foreachBatch / foreach yok). Python'lu bir
    # foreachBatch, JVM'in py4j callback soketi üzerinden Python'a geri çağrı
    # yapmasını gerektirir; bu da yerel güvenlik yazılımı loopback TCP'yi
    # engellediğinde Windows'ta zaman aşımına uğrayabilir
    # ("Error while obtaining a new communication channel"). Zenginleştirme saf
    # Spark SQL olduğundan bu sink'ler tamamen JVM'de çalışır – callback yok,
    # zaman aşımı yok. Dosya kaynağı kalıcı olduğundan iki sorgu onu güvenle okuyabilir.

    # 1) Zenginleştirilmiş olayları parquet'e kalıcı yaz (ML adımının kullandığı veri).
    parquet_query = (
        stream.writeStream.format("parquet")
        .option("path", str(config.OUTPUT_DIR))
        .option("checkpointLocation", str(config.CHECKPOINT_DIR / "parquet"))
        .outputMode("append")
        .trigger(processingTime=f"{config.TRIGGER_SECONDS} seconds")
        .start()
    )

    # 2) Akışı canlı izleyebilmen için her micro-batch'i konsola yazdır.
    console_query = (
        stream.select("event_time", "magnitude", "depth", "severity", "region")
        .writeStream.format("console")
        .option("truncate", "false")
        .option("numRows", "10")
        .option("checkpointLocation", str(config.CHECKPOINT_DIR / "console"))
        .outputMode("append")
        .trigger(processingTime=f"{config.TRIGGER_SECONDS} seconds")
        .start()
    )

    logger.info("Streaming sorguları başladı; izlenen klasör: %s", config.STREAM_INPUT_DIR)
    try:
        if duration is not None:
            parquet_query.awaitTermination(duration)
            logger.info("Streaming süre sınırına ulaştı; duruyor.")
        else:
            parquet_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Streaming kullanıcı tarafından durduruldu.")
    finally:
        for q in (parquet_query, console_query):
            try:
                q.stop()
            except Exception:
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deprem Spark streaming işi.")
    parser.add_argument("--duration", type=float, default=None,
                        help="Durmadan önce çalışılacak saniye (varsayılan: sonsuz).")
    args = parser.parse_args()
    run(duration=args.duration)
