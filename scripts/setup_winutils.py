"""
Windows'ta PySpark için winutils.exe ve hadoop.dll dosyalarını indirir.

PySpark, Windows'ta winutils.exe gerektiren Hadoop yerel dosya sistemi
API'lerini kullanır. Bu betik, (PySpark 3.5.x tarafından kullanılan) Hadoop
3.3.x serisi için doğru ikili dosyaları indirir ve <proje_kökü>/hadoop/bin/
içine yerleştirir.
Pipeline'ı başlatmadan önce bir kez çalıştırın:
    python scripts/setup_winutils.py
"""
import hashlib
import os
import platform
import sys
import urllib.request
from pathlib import Path

# https://github.com/cdarlint/winutils adresinden Hadoop 3.3.6 ikilileri (MIT lisansı)
BINARIES = {
    "winutils.exe": (
        "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
    ),
    "hadoop.dll": (
        "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"
    ),
}


def download(url: str, dest: Path) -> None:
    """Tek bir ikili dosyayı indirir ve hedefe kaydeder."""
    print(f"  {dest.name} indiriliyor ...", end=" ", flush=True)
    try:
        urllib.request.urlretrieve(url, dest)
        print("TAMAM")
    except Exception as exc:
        print(f"BAŞARISIZ\n  Hata: {exc}")
        raise


def main() -> None:
    if platform.system() != "Windows":
        print("Windows'ta değil – winutils gerekmez. Yapılacak bir şey yok.")
        return

    project_root = Path(__file__).resolve().parent.parent
    hadoop_bin = project_root / "hadoop" / "bin"
    hadoop_bin.mkdir(parents=True, exist_ok=True)

    for filename, url in BINARIES.items():
        dest = hadoop_bin / filename
        if dest.exists():
            print(f"  {filename} zaten mevcut, atlanıyor.")
        else:
            download(url, dest)

    print(f"\nwinutils kurulumu tamamlandı. HADOOP_HOME = {hadoop_bin.parent}")
    print("Pipeline, Windows'ta başlatıldığında HADOOP_HOME'u otomatik ayarlar.")


if __name__ == "__main__":
    main()
