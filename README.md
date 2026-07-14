---

```markdown
# Chess Titled Players Analytics Pipeline

**End-to-End Data Engineering Project**

![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE?style=flat-square)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square)

Proyek ini membangun **pipeline data otomatis** untuk mengumpulkan, menyimpan, dan menganalisis data historis pemain catur bertitel (Grandmaster, International Master, FIDE Master, dll) dari **Chess.com Public API**.

---

## 🎯 Latar Belakang & Tujuan

**Masalah**  
Data statistik pemain catur di Chess.com bersifat dinamis dan tidak ada sistem yang menyimpan historical data secara terstruktur. Hal ini menyulitkan analisis tren rating, aktivitas, dan performa pemain dari waktu ke waktu.

**Tujuan Proyek**  
Membangun pipeline **ELT (Extract - Load - Transform)** yang reliable, scalable, dan production-ready untuk:
- Mengambil data pemain bertitel secara berkala
- Menyimpan historical statistics
- Menyiapkan data berkualitas untuk analisis dan visualisasi

---

## 🛠 Tech Stack

| Layer              | Teknologi                          | Keterangan |
|--------------------|------------------------------------|----------|
| Orchestration      | Apache Airflow                     | Dockerized |
| Language           | Python 3.10+                       | Main development |
| Raw Storage        | MongoDB                            | Flexible schema |
| Data Warehouse     | PostgreSQL                         | Structured & analytics ready |
| Transformation     | Python + SQL                       | (rencana: dbt Core) |
| Infrastructure     | Docker Compose, Git                | Reproducible |
| Credential         | `.env` + docker secrets            | Secure |
| Visualization      | Streamlit                          | In Progress |

---

## 🏗 Arsitektur Pipeline

```mermaid
graph TD
    A[Chess.com Public API] --> B[Airflow DAG]
    B --> C[MongoDB - Raw Layer<br/>market_raw]
    C --> D[Transform Layer<br/>(Python + SQL)]
    D --> E[PostgreSQL - Serving Layer<br/>dim_players + fact_player_stats]
    E --> F[Streamlit Dashboard<br/>(In Progress)]
```

---

## ✨ Production-Grade Features

- **Idempotency** → Menggunakan upsert logic untuk menghindari data duplikat
- **Error Handling** → Retry mechanism + proper exception
- **Data Quality** → Script konsistensi antara MongoDB & PostgreSQL
- **Logging** → Structured logging
- **Security** → Credential management menggunakan `.env`
- **Reproducibility** → Full Docker environment
- **Modular Design** → Pisah logic extract, transform, dan load

---

## 📁 Project Structure

```
chess-titled-players-pipeline/
├── dags/                  # Airflow DAGs
├── scripts/               # Utility scripts (cek konsistensi, dll)
├── include/               # Custom modules (extract, transform, load)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## 🚀 Cara Menjalankan Project

```bash
# 1. Clone repository
git clone <your-repo-url>
cd chess-titled-players-pipeline

# 2. Setup environment
cp .env.example .env

# 3. Jalankan Airflow
docker-compose up -d

# 4. Akses Airflow UI
# Buka browser → http://localhost:8080
```

---

## 📊 Roadmap Pengerjaan

| Status       | Minggu | Pekerjaan |
|--------------|--------|---------|
| ✅ Done      | 1      | Setup Airflow + Docker Compose |
| 🔄 In Progress | 2    | Extract Layer + Rate Limit Handling |
| ⏳ Planned   | 3      | Load Layer (MongoDB & PostgreSQL) |
| ⏳ Planned   | 4      | Transform & Data Modeling |
| ⏳ Planned   | 5      | Data Quality & Consistency Check |
| ⏳ Planned   | 6      | Dashboard + Dokumentasi Lengkap |

---

## 📌 Lessons Learned & Challenges

*(Akan di-update secara berkala)*

- Menangani rate limit Chess.com API dengan baik
- Strategi menyimpan JSON fleksibel di MongoDB vs struktur di PostgreSQL
- Pentingnya idempotency pada pipeline yang berjalan secara berkala
- Perbedaan pendekatan antara raw storage dan serving layer

---

**Project ini masih dalam tahap pengembangan.**  
Tujuan utama adalah membangun pemahaman mendalam mengenai **production-grade data pipeline** menggunakan tools industri.

---

**Made with ❤️ for learning purpose**

---

### Cara Pakai:
1. Copy seluruh teks di atas
2. Paste ke `README.md`
3. Ganti `<your-repo-url>` dengan link repo kamu
4. Update bagian roadmap sesuai progress kamu

---
