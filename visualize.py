"""
Görselleştirme adımı: kümelenmiş depremleri PNG grafiklere dönüştürür.

``reports/clustered.csv`` dosyasını (ml_clustering.py tarafından üretilir) okur ve şunları yazar:
  - reports/clusters_map.png    Türkiye haritasında, kümeye göre renklendirilmiş depremler
  - reports/magnitude_hist.png  büyüklük dağılımı
  - reports/severity_counts.png şiddet etiketine göre deprem sayısı
  - reports/cluster_sizes.png   KMeans kümesi başına deprem sayısı

matplotlib'in etkileşimsiz "Agg" arka ucunu kullanır; böylece her işletim
sisteminde ekransız (headless) çalışır.

Tek başına çalıştırma:
    python visualize.py
"""
import logging

import matplotlib

matplotlib.use("Agg")  # GUI gerekmez; doğrudan PNG dosyalarına çiz
import matplotlib.pyplot as plt
import pandas as pd

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Viz] %(levelname)s: %(message)s",
)
logger = logging.getLogger("visualize")


def _save(fig, name: str) -> None:
    """Bir figürü reports/ klasörüne PNG olarak kaydeder."""
    path = config.REPORTS_DIR / name
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info("Kaydedildi: %s", path)


def run() -> None:
    """Kümelenmiş CSV'yi okur ve dört PNG grafiği üretir."""
    if not config.CLUSTERED_CSV.exists():
        logger.error("%s konumunda kümelenmiş veri yok. Önce ml_clustering.py çalıştırın.",
                     config.CLUSTERED_CSV)
        return

    df = pd.read_csv(config.CLUSTERED_CSV)
    if df.empty:
        logger.error("Kümelenmiş veri boş; çizilecek bir şey yok.")
        return

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Kümeye göre renklendirilmiş, büyüklüğe göre boyutlandırılmış deprem haritası.
    fig, ax = plt.subplots(figsize=(10, 7))
    scatter = ax.scatter(
        df["longitude"], df["latitude"],
        c=df["cluster"], cmap="tab10",
        s=(df["magnitude"].clip(lower=0) ** 2) * 12 + 10,
        alpha=0.7, edgecolors="black", linewidths=0.3,
    )
    ax.set_title("Deprem Kümeleri (KMeans) — Türkiye")
    ax.set_xlabel("Boylam")
    ax.set_ylabel("Enlem")
    ax.set_xlim(25, 45)   # Türkiye'nin yaklaşık sınırları
    ax.set_ylim(35, 43)
    ax.grid(True, linestyle="--", alpha=0.3)
    legend = ax.legend(*scatter.legend_elements(), title="Küme", loc="upper right")
    ax.add_artist(legend)
    _save(fig, "clusters_map.png")

    # 2) Büyüklük dağılımı.
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(df["magnitude"], bins=20, color="#c0392b", edgecolor="white")
    ax.set_title("Büyüklük Dağılımı")
    ax.set_xlabel("Büyüklük")
    ax.set_ylabel("Sayı")
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    _save(fig, "magnitude_hist.png")

    # 3) Şiddet sayıları.
    severity_order = ["MINOR", "LIGHT", "MODERATE", "STRONG", "MAJOR"]
    counts = df["severity"].value_counts().reindex(severity_order).fillna(0)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(counts.index, counts.values, color="#2980b9", edgecolor="white")
    ax.set_title("Şiddete Göre Depremler")
    ax.set_xlabel("Şiddet")
    ax.set_ylabel("Sayı")
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    _save(fig, "severity_counts.png")

    # 4) Küme boyutları.
    sizes = df["cluster"].value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(sizes.index.astype(str), sizes.values, color="#27ae60", edgecolor="white")
    ax.set_title("Küme Başına Deprem Sayısı")
    ax.set_xlabel("Küme")
    ax.set_ylabel("Sayı")
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    _save(fig, "cluster_sizes.png")

    logger.info("Tüm grafikler şuraya yazıldı: %s", config.REPORTS_DIR)


if __name__ == "__main__":
    run()
