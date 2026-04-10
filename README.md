# worldcup-2026-predictions
---

## 📊 Dataset Summary

| Source | Table | Rows | Description |
|--------|-------|------|-------------|
| Kaggle | results | 49,215 | International matches 1872-2024 |
| Kaggle | goalscorers | 47,601 | Goals scored per match |
| Kaggle | players_24 | 180,021 | FIFA 24 player ratings |
| Kaggle | teams_24 | 6,947 | FIFA 24 team ratings |
| Kaggle | shootouts | 675 | Penalty shootout results |
| Kaggle | fifa_world_cup_summary | 22 | WC tournament history |
| API-Football | current_player_stats | 171 | 2024/25 season stats |
| Seed | world_cup_2026_groups | 48 | WC 2026 group assignments |
| Seed | world_cup_2026_fixtures | 72 | WC 2026 group stage matches |

---

## 🔮 Prediction Results

### 🏆 Tournament Winner
| Rank | Team | Finals Probability |
|------|------|--------------------|
| 1 | 🇫🇷 France | 65.1% |
| 2 | 🏴󠁧󠁢󠁥󠁮󠁧󠁿 England | 63.9% |
| 3 | 🇩🇪 Germany | 62.7% |
| 4 | 🇪🇸 Spain | 64.4% |
| 5 | 🇧🇷 Brazil | 63.1% |
| 6 | 🇦🇷 Argentina | 62.4% |

### ⚽ Golden Boot
| Rank | Player | Country | Predicted Goals |
|------|--------|---------|----------------|
| 1 | C. Ronaldo | Portugal | 51.99 |
| 2 | H. Kane | England | 35.95 |
| 3 | L. Messi | Argentina | 31.73 |
| 4 | R. Lukaku | Belgium | 26.55 |
| 5 | K. Mbappé | France | 22.20 |

### ⭐ Golden Ball
| Rank | Player | Country | Impact Score |
|------|--------|---------|-------------|
| 1 | C. Ronaldo | Portugal | 52.42 |
| 2 | H. Kane | England | 30.88 |
| 3 | L. Messi | Argentina | 30.12 |

### 🎯 Top Assists
| Rank | Player | Country | Combined Score |
|------|--------|---------|---------------|
| 1 | L. Messi | Argentina | 27.7 |
| 2 | M. Olise | France | 26.68 |
| 3 | J. Kimmich | Germany | 25.24 |
| 4 | H. Kane | England | 24.73 |
| 5 | Mohamed Salah | Egypt | 24.26 |

### 🌟 Best Young Player (Age ≤ 23)
| Rank | Player | Country | Age |
|------|--------|---------|-----|
| 1 | Lamine Yamal | Spain | 18 |
| 2 | F. Wirtz | Germany | 22 |
| 3 | J. Bellingham | England | 22 |

### 🐣 Biggest Underdog Threats
| Rank | Team | Max Upset % | Deep Run Score |
|------|------|-------------|----------------|
| 1 | New Zealand | 96.0% | 65.5 |
| 2 | Egypt | 87.6% | 62.0 |
| 3 | Saudi Arabia | 92.4% | 60.6 |

---

## 🧪 Data Quality

- ✅ Unique surrogate keys on all dimension and fact tables
- ✅ Not null checks on all critical columns
- ✅ No duplicate players in dim_players (custom test)
- ✅ Accepted values for match_result
- ✅ Age validation on historical goal matching (15+ years old)
- ✅ Deduplication of multi-league players using GROUP BY + MAX()

---

## 🔑 Key Technical Challenges Solved

**1. Player Name Matching**
- "h. kane" ≠ "Harry Kane" ≠ "harry edward kane"
- Built 5-method cascade name matching system

**2. Duplicate Players**
- Same player in multiple leagues
- Fixed with GROUP BY + MAX() deduplication

**3. Missing Star Players**
- Messi (MLS) and Ronaldo (Saudi League) outside our API leagues
- Added via missing_players.csv seed

**4. Inconsistent Team Names**
- "IR Iran" vs "Iran" vs "Islamic Republic of Iran"
- Built standardize_team_name() macro with 15+ mappings

**5. Duplicate Match Records**
- Tahiti vs New Caledonia played twice same day (1974)
- Added home_score + away_score to surrogate key

---

## 🚀 How to Reproduce

### Prerequisites
- Google Cloud Platform account
- dbt Cloud account (free tier)
- Docker Desktop installed
- Kaggle account + API key
- API-Football account (free tier)

### 1. Clone the Repository
```bash
git clone https://github.com/Nosakharey/worldcup-2026-predictions.git
cd worldcup-2026-predictions
```

### 2. Provision Infrastructure with Terraform
```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### 3. Start Kestra
```bash
cd kestra
docker-compose up -d
```
- Access Kestra UI at http://localhost:8080
- Add KV store secrets: `gcp_creds`, `api_football_key`
- Run flow: `worldcup2026.worldcup_data_pipeline`
- Run flow: `worldcup2026.fetch_player_stats`

### 4. Run dbt
```bash
dbt deps
dbt seed
dbt run
dbt test
```

### 5. Train ML Models & Generate Predictions
Run the SQL scripts in BigQuery in this order:
1. Train all 7 ML models (CREATE MODEL statements)
2. Run all 7 prediction queries (ML.PREDICT statements)

### 6. View Dashboard
🔗 [World Cup 2026 Predictions Dashboard](https://lookerstudio.google.com/reporting/27e8c29a-42af-4216-8d97-0dd16c9ed809)

---

## 👤 Author

**Agbonze Nosa Godwin**
- GitHub: [@Nosakharey](https://github.com/Nosakharey)
- Course: Data Engineering Zoomcamp 2026

---

## 📄 License
MIT License