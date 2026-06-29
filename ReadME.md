# 🚀 AZRetail: End-to-End ELT Pipeline

Bu layihə, pərakəndə satış (retail) məlumatlarının müxtəlif mənbələrdən (CSV, mürəkkəb JSON və xarici API-lar) avtomatlaşdırılmış şəkildə çəkilməsi, təmizlənməsi və analitik hesabata (Star Schema) qədər gətirilməsini təmin edən tam konteynerləşdirilmiş **ELT (Extract, Load, Transform)** data lakehouse arxitekturasıdır.

Layihənin mərkəzində **Airflow** (Orchestration) və **dbt** (Transformation) dayanır. Dinamik konfiqurasiya, səhvlərə qarşı dayanıqlılıq (error handling), SCD Type 2 tarixçə izləməsi və inkremental data yükləməsi kimi müasir data mühəndisliyi təcrübələri tətbiq edilmişdir.

---

## 🚀 Layihəni Lokal Mühitdə İşə Salmaq

1. Docker Compose vasitəsilə həm də Dockerfile build edərək infrastrukturu ayağa qaldırın:
   ```
   docker compose up -d --build

---


## 🛠 Texnoloji Stack

* **Orchestration:** Apache Airflow 2.10.5 (Docker üzərində custom build)
* **Data Transformation:** dbt Core (v1.12.0) + dbt-postgres adapteri
* **Verilənlər Bazası (Data Warehouse):** PostgreSQL 16 (Həm Airflow backend-i, həm də DWH kimi `datalake` sxemi üzərində qurulub və bu schema postgres db up olarkən init.sql faylı vasitəsi ilə avtomatik yaranır)
* **Data Processing & Ingestion:** Python (Pandas, SQLAlchemy, Requests)
* **İnfrastruktur:** Docker & Docker Compose
* **Versiya Nəzarəti və Asılılıqlar:** Git

---

## 🏗 Arxitektura və Data Axını (Data Flow)

Layihə məlumatı **Medallion Architecture** (Bronze ➔ Silver ➔ Gold) prinsipləri əsasında emal edir.

Sadəcə mən kiçik bir dəyişiklik edərək `orders.json` faylındakı tarixləri `2026-01-01` və `2026-01-05` aralığında dəyişərək sırf bu tarixlər üzrə işlətdim dagları yaranan dagrun-ların sayını azaltdım yoxsa həmin sütuna görə bütün tarixlər (2024-2025) üçün dagrun yaranıb airflow serverinin işini uzadacaqdı.

### 1. 🟤 Bronze Qatı (Xam Məlumatların Qəbulu / Ingestion)
Məlumatlar yerli qovluqdan (`bind mount: ./data_lake`) və xarici API-dan Airflow tərəfindən oxunaraq heç bir tip dəyişikliyi edilmədən (`VARCHAR` formatında) verilənlər bazasına yüklənir.

* **Config-Driven Ingestion:** Python kodunu dəyişmədən, yalnız Airflow Variable daxilindəki `variables.json` faylına yeni məlumat əlavə etməklə sonsuz sayda yeni CSV/JSON mənbəsi sistemə paralel Task-lar kimi əlavə oluna bilir.
* **Xarici Valyuta API:** Valyuta məzənnələri (AZN kəsiyində) xarici REST API-dan çəkilərək `bronze_currency` cədvəlinə yüklənir.
* **Idempotent & Incremental Yükləmə:** Xüsusilə `orders` (JSON) məlumatları Airflow-un `{{ ds }}` (execution_date) parametri ilə işləyir. Bu sayədə `pre-filter` olunur dataframe-ə sırf həmin günün datası yüklənir. API-dan məlumatlar səhifələnərək (Pagination) oxunur, nested array-lər (items) Pandas ilə `explode` olunur. Məlumat bazaya yazılmazdan əvvəl həmin günün (`ds`) datası silinir və yenidən yazılır, bu da dublikatların yaranmasını sıfıra endirir.

### 2. ⚪ Silver Qatı (Təmizləmə və Standartlaşdırma)
Bronze qatındakı xam məlumatlar **dbt** vasitəsilə oxunur, təmizlənir və etibarlı (Single Source of Truth) cədvəllərə çevrilir:
* **Deduplikasiya:** `ROW_NUMBER()` funksiyası ilə ən son ingest olunmuş məlumat saxlanılaraq təkrar qeydlər ləğv edilir.
* **Məlumat Standartlaşdırması:** Tarix formatlarının (DD.MM.YYYY ➔ Timestamp) düzəldilməsi, Ölkə kodlarının (Azerbaijan, AZ ➔ AZ) və valyutaların standartlaşdırılması.
* **Incremental Materialization:** `silver_orders` cədvəli hər dəfə tamamilə yenidən qurulmur, yalnız Airflow-dan gələn günə (`execution_date`) uyğun data MD5 surrogate açarı (`order_item_key`) vasitəsilə `delete+insert` strategiyası ilə yüklənir.

### 3. 🟡 Gold Qatı (Biznes və Analitika)
Silver qatında təmizlənmiş məlumatlar biznes komandasının sorğuları üçün Star Schema formasında modelləşdirilir.
* **SCD Type 2 (Tarixçə İzləmə):** Müştəri və Məhsul ölçü cədvəllərində (Dimension tables) baş verən dəyişiklikləri izləmək üçün dbt-nin `{% snapshot %}` məntiqindən istifadə olunur. (Məsələn: Müştəri seqmentini dəyişdikdə köhnə rekord `valid_to` tarixi ilə bağlanır, yeni rekord `is_current = true` ilə yaranır).
* **Fact Cədvəlinin Zənginləşdirilməsi:** `gold_fct_order_items` cədvəli incremental olaraq yüklənir və bu zaman `bronze_currency` cədvəli ilə JOIN edilərək real vaxt məzənnəsinə əsasən hər bir sifarişin **AZN ekvivalenti (`unit_price_azn`)** dinamik olaraq hesablanır.

---

## ⚙️ Mühəndislik Həlləri (Key Engineering Features)

* **Robust API Fetching:** JSON ingestion skripti `backoff`, `timeout handling` və `max retries` məntiqi ilə təchiz olunub. Hər hansı şəbəkə qırılmasında pipeline çökmür, gözləyib yenidən cəhd edir.
* **Metadata İzləmə:** Bütün Bronze cədvəllərinə ingestion anında avtomatik olaraq `_batch_id`, `_ingested_at`, və `_source_name` meta sütunları əlavə edilir. Bu, məlumatın mənbəyini və vaxtını audit etməyə imkan verir.
* **Catchup & Backfill:** Airflow DAG `catchup=True` və `start_date=2026-01-01`, `end_date=2026-01-05` kimi tənzimlənmişdir. Bu, keçmiş tarixlərdəki məlumatların ardıcıl və təhlükəsiz şəkildə bazaya bərpa olunmasını (backfilling) təmin edir.

---

## 🚦 Airflow DAG Axını (Orchestration Flow)

Bütün ETL prosesi vahid bir yönləndirilmiş qraf (DAG) üzərində ardıcıllıqla icra edilir:

1. `from_raw_to_bronze_customers`, `from_raw_to_bronze_products`, `from_raw_to_bronze_orders` (Paralel Ingestion)
2. `api_currency` (Valyuta Ingestion)
3. `dbt_run_silver` (Məlumatların təmizlənməsi)
4. `dbt_snapshot` (SCD2 tarixçəsinin qeydə alınması)
5. `dbt_run_gold` (Biznes qatının və Star Schema-nın formalaşdırılması)
6. `dbt_test` (Data keyfiyyətinin və referens bütövlüyünün avtomatik yoxlanılması)

---

# 🏛️ Data Lakehouse Arxitektura qursaydım nə edərdim

## 1. Komponent-Komponent Miqrasiya Strategiyası

Mövcud infrastrukturdakı bottlenecks aradan qaldırmaq üçün hər bir qatın transformasiya planı:

| Mövcud Komponent | Hədəf Böyük Data Arxitekturası | Miqrasiya Səbəbi və Mexanizmi |
| :--- | :--- | :--- |
| **Local Disk Storage** | **MinIO (S3-Compatible Object Storage)** | Lokal fayl sistemi scalable deyil. MinIO paylanmış şəkildə qurula bilir, data `raw`, `bronze`, `silver`, `gold` bucket-lərində saxlanılır. |
| **PostgreSQL (DWH/Marts)** | **Delta Tables (və ya Apache Iceberg) + Parquet** | Postgres milyard sətirlik join və analitik sorğularda kilidlənir. Table formatlar sayəsində data MinIO üzərində sütunlu (columnar) Parquet faylları kimi saxlanılır bu da storage-ə qənaət edir və ACID transaction dəstəyi qazanır. Column Pruning sorğu zamanı bütün sətir və sütunlar deyil, SELECT daxilində yalnız adı çəkilən konkret sütunlar oxunur. Bu, gərəksiz I/O (giriş-çıxış) əməliyyatlarını tamamilə sıfırlayaraq sorğu sürətini kəskin artırır. Data əvvəlcədən müəyyən filtrlərə (məsələn, tarixlərə) görə qovluqlara bölünür (Partitioning). Sorğu atıldıqda, şərtə uyğun gəlməyən milyonlarca fayl və qovluq tamamilə pas keçilir, yalnız hədəf nöqtədən data oxunur. |
| **Python Raw Scripts** | **Apache Spark (PySpark / Spark Streaming)** | Pandas bütün datanı RAM-a yükləyir (OOM xətaları). Spark isə hesablamanı node-lar arasında paralel bölür. |
| **Airflow Python Loops** | **Airflow Dynamic Task Mapping** | Mövcud DAG-da `variables.json` faylı loop ilə oxunur. Bu, Airflow Scheduler-ə əlavə yük yaradır. Böyük miqyasda bu, runtime zamanı dinamik olaraq task yaradan **Dynamic Task Mapping** (`.expand()`) mexanizminə keçirilir. |

---

## 🛡️KAFKA Yüksək Əlçatanlıq (High Availability) və Replikasiya

Real istehsalat mühitində (Production) serverlərdən biri çökərsə, datanın itməməsi üçün replikasiya mexanizmindən istifadə olunur:

* **Replication Factor (Tövsiyə edilən: 3):** Hər bir partisiya 3 fərqli serverdə kopyalanır.
* **Leader & Follower:** Partisiyalardan biri **Leader** seçilir. Bütün oxuma və yazma sorğuları Leader üzərindən keçir. Digər iki server (**Follower**) yalnız datanı sinxron kopyalayır. Leader çökərsə, Kafka KRaft / Zookeeper vasitəsilə Follower-lərdən birini anında yeni Leader seçir.
* **`acks=all` Siyasəti:** Məlumatın tam təhlükəsiz yazılması üçün Producer mesajı göndərəndə, mesajın həm Leader, həm də Follower-lər tərəfindən diskə yazıldığına dair təsdiq (`acknowledgement`) gözləyir.

---

## 📊 Kafka Arxitektura Diaqramı

```text
[ 1. PRODUCERS LAYER ]
   Microservice          CDC (Log Reader)         External REST API
        │                       │                         │
        │ (Key: order_101)      │ (Key: order_102)        │ (Key: order_103)
        ▼                       ▼                         ▼
──────────────────────────────────────────────────────────────────────────
[ 2. DISTRIBUTED KAFKA CLUSTER ]

   ┌──────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────────┐
   │         BROKER 1         │  │         BROKER 2         │  │         BROKER 3         │
   │                          │  │                          │  │                          │
   │  retail.orders.v1        │  │  retail.orders.v1        │  │  retail.orders.v1        │
   │  ┌────────────────────┐  │  │  ┌────────────────────┐  │  │  ┌────────────────────┐  │
   │  │ Partition 0        │  │  │  │ Partition 1        │  │  │  │ Partition 2        │  │
   │  │ [LEADER]           │  │  │  │ [LEADER]           │  │  │  │ [LEADER]           │  │
   │  └─────────┬──────────┘  │  │  └─────────┬──────────┘  │  │  └─────────┬──────────┘  │
   │            │             │  │            │             │  │            │             │
   │  ┌─────────▼──────────┐  │  │  ┌─────────▼──────────┐  │  │  ┌─────────▼──────────┐  │
   │  │ Partition 1        │  │  │  │ Partition 2        │  │  │  │ Partition 0        │  │
   │  │ [FOLLOWER]         │  │  │  │ [FOLLOWER]         │  │  │  │ [FOLLOWER]         │  │
   │  └────────────────────┘  │  │  └────────────────────┘  │  │  └────────────────────┘  │
   └────────────┬─────────────┘  └────────────┬─────────────┘  └────────────┬─────────────
                │                             │                             │
                │ (Fetch data)                │ (Fetch data)                │ (Fetch data)
                ▼                             ▼                             ▼
──────────────────────────────────────────────────────────────────────────
[ 3. CONSUMERS LAYER ]  ➔  Consumer Group: `lakehouse-ingest-group`

   ┌──────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────────┐
   │  Spark Worker Pod 1      │  │  Spark Worker Pod 2      │  │  Spark Worker Pod 3      │
   └────────────┬─────────────┘  └────────────┬─────────────┘  └────────────┬─────────────
                │                             │                             │
                └──────────────────────┐      │      ┌──────────────────────┘
                                       ▼      ▼      ▼
──────────────────────────────────────────────────────────────────────────
[ 4. TARGET STORAGE ]
                         ┌──────────────────────────────────┐
                         │      MinIO Object Storage        │
                         │         (Table Format)           │
                         └──────────────────────────────────┘
```
---
# ⚡ Apache Spark: Milyardlarla Sətirlik Data Optimallaşdırılması

## 1. File & Format Seçimi (Columnar Efficiency)
* **Qərar:** CSV/JSON əvəzinə mütləq **Parquet** və ya **ORC** formatı, cədvəl memarlığı olaraq isə **Apache Iceberg / Delta Lake** istifadə edilməlidir.
* **Column Pruning:** Məlumat sətir yox, sütun bazlı saxlanılır. Sorğu zamanı `SELECT` daxilində yalnız adı çəkilən sütunlar oxunur, gərəksiz I/O tamamilə sıfırlanır.
* **Daxili Metadata (Pushdown Filters):** Parquet faylları daxilində hər blokun min/max statistikası saxlanılır. Spark sorğudakı `WHERE` şərtinə baxaraq uyğun gəlməyən bütöv fayl bloklarını oxumadan pas keçir (**Predicate Pushdown**).

---

## 2. Partitioning & Partition Pruning
* **Düzgün Partition Açarı:** Məlumatlar sorğularda ən çox filtr olunan sütunlara görə (məsələn, `order_date`, `country_code`) disklərdə qovluqlara bölünməlidir.
* **Partition Pruning (Budaqlama):** `WHERE order_date = '2026-06-30'` sorğusu işlədikdə, Spark milyardlarla sətir arasından digər günlərə/illərə aid olan milyonlarla faylı tamamilə görməzdən gəlir, yalnız hədəf günün qovluğunu oxuyur.
* **⚠️ Diqqət (Over-partitioning):** Data çox kiçik parçalara bölünməməlidir (hər partisiya minimum 128MB-512MB olmalıdır). Əks halda "Small File Problem" yaranır və Driver Node metadata yükündən çökür.

---

## 3. Join Strategiyaları (Shuffle vs Broadcast)

Milyardlıq cədvəllərdə fərqli node-lar arasında data yerdəyişməsi (**Network Shuffle**) ən böyük performans darboğazıdır.

* **Broadcast Hash Join (Böyük ➔ Kiçik):** Milyard sətirlik Fakt cədvəli ilə kiçik bir Dim cədvəli (məsələn, <100MB) join olunursa, Spark kiçik cədvəli bütün Worker node-ların RAM-ına kopyalayır (`broadcast`). Şəbəkə üzərində nəhəng Fakt cədvəlinin hərəkət etməsinə (Shuffle) ehtiyac qalmır və sorğu saniyələr daxilində bitir.
* **Shuffle Sort-Merge Join (Böyük ➔ Böyük):** İki nəhəng milyardlıq cədvəl join olunursa, Spark hər iki cədvəli join açarına (key) görə şəbəkədə yenidən paylayır (Shuffle) və sort edir. Bu, şəbəkəyə yük salsada, yaddaşın (RAM) izolyasiyasını təmin edir və cluster-in çökməsinin qarşısını alır.

---

## 4. AQE
Adaptive Query Execution (AQE): Spark 3+ ilə gələn AQE mütləq aktiv edilməlidir (spark.sql.adaptive.enabled=true). AQE runtime zamanı işin gedişatına baxaraq çox kiçik qalmış partisiyaları avtomatik birləşdirir və ya həddindən artıq böyük (skewed) partisiyaları dinamik olaraq parçalayır.

---

## 📝 Yekun Qeyd
Əslində plana bir dənə CI/CD mexanizmi əlavə etmək istəyirdim: Airflow DAG-ları hər dəfə işə düşməmişdən qabaq GitHub-dan ən son dbt modellərini, skriptləri və .py fayllarını avtomatik pull eləsin, sistem həmişə yeni qalsın. Sadəcə vaxt darlığından çatdıra bilmədim.

Hələlik mənzərə budur. Bəzi texniki detallar yaddan çıxmış ola bilər, amma əsas yanaşma və arxitektura tam olaraq budur. Daha çox vaxt sərf edib daha mükəmməl etmək olar)