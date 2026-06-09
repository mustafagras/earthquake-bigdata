# 🌍 Earthquake Big Data Pipeline

Kandilli Rasathanesi (Kandilli Observatory) deprem verilerini **canlı (streaming)**
olarak çeken, **Apache Spark Structured Streaming** ile işleyen, **Spark MLlib
KMeans** ile kümeleyen ve sonuçları **grafiklerle görselleştiren** basit ama
eğitici bir büyük veri (big data) projesidir.

Windows ve macOS/Linux üzerinde **tek komutla, sorunsuz** çalışır.

```
Kandilli API  ─▶  Producer  ─▶  JSON dosyaları  ─▶  Spark Streaming  ─▶  Parquet
                                                                            │
                                  PNG grafikler  ◀──  Görselleştirme  ◀──  KMeans (ML)
```

---

## Neden dosya tabanlı streaming?

Spark'ın `socket` kaynağı yalnızca test amaçlıdır; Windows'ta IPv6/port
sorunları ve bağlantı zaman aşımlarına yol açar. Bunun yerine **producer veriyi
JSON dosyalarına yazar**, Spark da o klasörü izler (`readStream.format("json")`).
Dosyalar kalıcı olduğu için tek bir streaming sorgusu her işletim sisteminde
güvenilir biçimde çalışır — ağ sorunu, zaman aşımı yoktur.

---

## Proje yapısı

| Dosya | Görevi |
|-------|--------|
| `config.py` | Tüm ayarlar (API, süreler, klasörler, küme sayısı). |
| `common.py` | Spark oturumu + Windows `winutils` kurulumu (ortak yardımcı). |
| `producer.py` | Kandilli API'den veri çeker, `data/stream_input/` içine JSON yazar. |
| `streaming_job.py` | Spark Structured Streaming: JSON → zenginleştir → Parquet. |
| `ml_clustering.py` | Spark MLlib KMeans ile depremleri kümeler → `reports/clustered.csv`. |
| `visualize.py` | matplotlib ile PNG grafikler üretir. |
| `run.py` | Hepsini sırayla çalıştıran tek komutluk orkestratör. |
| `scripts/setup_winutils.py` | Windows için `winutils.exe` indirir (gerekirse). |
| `scripts/run_all.sh` | macOS/Linux/Git Bash için kısayol (sonunda `run.py`'yi çağırır). |

---

## Kurulum

### Gereksinimler
- **Python 3.10+**
- **Java 17** (Spark için gerekli — `java -version` ile kontrol edin)

### Adımlar

```bash
# 1) Sanal ortam oluştur
python -m venv venv

# 2) Ortamı etkinleştir
#    Windows (PowerShell):
venv\Scripts\Activate.ps1
#    macOS/Linux:
source venv/bin/activate

# 3) Bağımlılıkları kur
pip install -r requirements.txt
```

> **Windows notu:** `winutils.exe` ve `hadoop.dll` `hadoop/bin/` içinde hazır
> gelir. Eksikse `python scripts/setup_winutils.py` ile indirilir. Proje bunları
> otomatik olarak ayarlar — elle bir şey yapmanıza gerek yok.

---

## Çalıştırma

Tek komut — her şeyi yapar (veri çekme → streaming → ML → grafikler):

```bash
python run.py
```

veya macOS/Linux/Git Bash'te:

```bash
bash scripts/run_all.sh
```

Yaklaşık **1-2 dakika** sürer. Bittiğinde grafikler `reports/` klasöründe olur.

### Aşamaları tek tek çalıştırmak (öğrenmek için)

```bash
python producer.py --duration 60     # 1) Veriyi dosyalara çek
python streaming_job.py --duration 60 # 2) Spark ile işle (başka terminalde)
python ml_clustering.py              # 3) KMeans kümeleme
python visualize.py                  # 4) Grafikleri üret
```

---

## Çıktılar

İşlenen veriler `data/output/` altında **Parquet** olarak, sonuçlar ise
`reports/` altında saklanır:

| Dosya | İçerik |
|-------|--------|
| `reports/clustered.csv` | Küme etiketli tüm depremler. |
| `reports/clusters_map.png` | Türkiye haritasında KMeans kümeleri (renkli, büyüklüğe göre boyutlu). |
| `reports/magnitude_hist.png` | Büyüklük dağılımı histogramı. |
| `reports/severity_counts.png` | Şiddet kategorisine göre deprem sayıları. |
| `reports/cluster_sizes.png` | Her kümedeki deprem sayısı. |

---

## Veri akışı ve zenginleştirme

Producer her kaydı şu sade şemaya indirger:
`earthquake_id, provider, title, magnitude, depth, latitude, longitude, region, event_time`

Spark işi her depreme şunları ekler:
- **severity** — büyüklüğe göre etiket: `MINOR < 2.0 ≤ LIGHT < 3.0 ≤ MODERATE < 4.0 ≤ STRONG < 5.0 ≤ MAJOR`
- **event_ts** — metin tarihten ayrıştırılmış gerçek zaman damgası.

> **Canlı veri ipucu:** Kandilli `/live` ucu yalnızca son 24 saati döndürür ve
> sakin dönemlerde boş olabilir. Bu durumda producer otomatik olarak son
> `ARCHIVE_BACKFILL_DAYS` günü arşivden çeker — böylece demo her zaman veriyle
> çalışır.

---

## Ayarları değiştirme

Her şey `config.py` içinde:

```python
FETCH_INTERVAL_SECONDS = 10     # producer ne sıklıkla API'yi yoklasın
STREAM_DURATION_SECONDS = 60    # demo ne kadar süre stream yapsın
KMEANS_K = 5                    # kaç sismik küme bulunsun
API_PROVIDER = "kandilli"       # "kandilli" veya "afad"
```

---

## Sık karşılaşılan sorunlar

| Sorun | Çözüm |
|-------|-------|
| `JAVA_HOME is not set` / Spark başlamıyor | Java 17 kurun. |
| `winutils.exe not found` (Windows) | `python scripts/setup_winutils.py` çalıştırın. |
| `No data was collected` | İnternet bağlantısını ve API erişimini kontrol edin. |
| TLS / sertifika hatası | `truststore` paketi OS sertifika deposunu kullanır; `pip install -r requirements.txt` ile kurulu olduğundan emin olun. |

---

Eğitim amaçlıdır. Veri kaynağı: [orhanaydogdu/deprem API](https://github.com/orhanayd/kandilli-rasathanesi-api) (Kandilli Rasathanesi).
