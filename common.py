"""
Ortak Spark yardımcıları.

Bu modül import edildiğinde, PySpark'ın hem Windows hem macOS/Linux üzerinde
sorunsuz çalışması için ortamı yapılandırır ve ardından oturum oluşturmak için
``get_spark()`` fonksiyonunu sunar.

ÖNEMLİ: Windows/Hadoop ortam değişkenleri, JVM başlamadan ÖNCE (yani
``SparkSession.getOrCreate()`` çağrılmadan önce) ayarlanmalıdır. Bu yüzden bu
modül, aşağıda ``pyspark`` import edilmeden önce, bu değişkenleri import
zamanında ayarlar.
"""
import os
import sys
import platform

import config


def _setup_windows_hadoop() -> None:
    """Windows'ta Hadoop'u, projeyle gelen winutils.exe / hadoop.dll'e yönlendirir.

    PySpark, Hadoop'un yerel dosya sistemi API'lerini kullanır; bunlar Windows'ta
    winutils.exe gerektiren yerel (native) kod çağırır (örn. parquet/checkpoint
    yazarken). macOS/Linux'ta bu fonksiyon hiçbir şey yapmaz.
    """
    if platform.system() != "Windows":
        return

    hadoop_home = config.PROJECT_ROOT / "hadoop"
    winutils = hadoop_home / "bin" / "winutils.exe"
    if not winutils.exists():
        setup = config.PROJECT_ROOT / "scripts" / "setup_winutils.py"
        raise FileNotFoundError(
            f"winutils.exe şurada bulunamadı: {winutils}.\n"
            f"Bir kez çalıştırın:  python {setup}"
        )

    hadoop_bin = str(hadoop_home / "bin")
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)

    # hadoop.dll'in Windows yükleyicisi tarafından bulunabilmesi için...
    path = os.environ.get("PATH", "")
    if hadoop_bin.lower() not in path.lower():
        os.environ["PATH"] = hadoop_bin + os.pathsep + path

    # ...ve JVM'in java.library.path'i tarafından bulunabilmesi için.
    # JAVA_TOOL_OPTIONS, JVM tarafından herhangi bir sınıf yüklenmeden önce
    # okunur; bu yüzden PySpark'ın yerel modda başlattığı alt JVM'e güvenilir
    # biçimde ulaşır.
    jto = os.environ.get("JAVA_TOOL_OPTIONS", "")
    if "-Djava.library.path" not in jto:
        os.environ["JAVA_TOOL_OPTIONS"] = (jto + f" -Djava.library.path={hadoop_bin}").strip()


_setup_windows_hadoop()

# Sürücü ve Python işçileri (worker) için AYNI yorumlayıcıyı kullan; böylece
# Spark, PATH üzerinden uyumsuz bir "python" seçmez.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402  (ortam ayarından sonra gelmeli)


def get_spark(app_name: str | None = None) -> SparkSession:
    """Bu proje için yapılandırılmış yerel bir SparkSession oluşturur (veya yeniden kullanır)."""
    spark = (
        SparkSession.builder.appName(app_name or config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
