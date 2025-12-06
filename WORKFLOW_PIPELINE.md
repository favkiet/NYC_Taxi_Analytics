Chào bạn! Rất vui được hỗ trợ bạn với đồ án này. Để đề xuất phù hợp nhất, tôi sẽ xem qua codebase hiện tại của bạn trước.

[3 tools called]

Rất tuyệt vời! Bạn đã có một infrastructure Big Data khá hoàn chỉnh rồi. Dựa trên codebase hiện tại và yêu cầu của đề tài, tôi sẽ đề xuất **hướng tiếp cận toàn diện với trọng tâm so sánh thực nghiệm Big Data vs Non-Big Data**.

---

# 🚀 ĐỀ XUẤT HƯỚNG TIẾP CẬN: Dự Đoán Nhu Cầu Taxi NYC

## 📊 **TỔNG QUAN KIẾN TRÚC**

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              KIẾN TRÚC HỆ THỐNG                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌────────────────┐     ┌──────────────────────────────────────────────────────┐   │
│  │  NYC TLC Data  │────▶│              DATA PROCESSING LAYER                    │   │
│  │  (Parquet)     │     │  ┌─────────────────┐    ┌─────────────────┐          │   │
│  └────────────────┘     │  │ NON-BIG DATA    │    │   BIG DATA       │          │   │
│                         │  │ ─────────────── │    │  ─────────────── │          │   │
│                         │  │ • Pandas        │    │  • Spark         │          │   │
│                         │  │ • DuckDB        │    │  • Delta Lake    │          │   │
│                         │  │ • Polars        │    │  • MinIO         │          │   │
│                         │  └────────┬────────┘    └────────┬────────┘          │   │
│                         └───────────┼──────────────────────┼───────────────────┘   │
│                                     │                      │                       │
│                                     ▼                      ▼                       │
│                         ┌──────────────────────────────────────────────────────┐   │
│                         │              FEATURE STORE                            │   │
│                         │  • Aggregated pickup counts by zone/hour             │   │
│                         │  • Temporal features (day_of_week, holiday, etc.)    │   │
│                         │  • Lag features (demand t-1, t-24, t-168)            │   │
│                         └───────────────────────┬──────────────────────────────┘   │
│                                                 │                                   │
│                         ┌───────────────────────▼──────────────────────────────┐   │
│                         │              ML MODELS                                │   │
│                         │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐     │   │
│                         │  │   XGBoost   │ │   ARIMA     │ │    LSTM     │     │   │
│                         │  │  (Tabular)  │ │(Time Series)│ │(Deep Learn) │     │   │
│                         │  └─────────────┘ └─────────────┘ └─────────────┘     │   │
│                         └───────────────────────┬──────────────────────────────┘   │
│                                                 │                                   │
│                         ┌───────────────────────▼──────────────────────────────┐   │
│                         │        SERVING & VISUALIZATION                        │   │
│                         │  • FastAPI + Redis Cache                             │   │
│                         │  • Superset Dashboard                                │   │
│                         │  • Folium + Plotly Map                               │   │
│                         └──────────────────────────────────────────────────────┘   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 **PHASE 1: THU THẬP & XỬ LÝ DỮ LIỆU**

### 1.1. Chiến lược dữ liệu

| Dataset Size | Mục đích | Cách tiếp cận |
|-------------|----------|---------------|
| **1 tháng** (~3-5GB) | Development, testing | Pandas/Polars |
| **1 năm** (~40-60GB) | So sánh hiệu năng | Pandas vs Spark |
| **3-5 năm** (~150-250GB) | Full production | Spark + Delta Lake |

### 1.2. Schema dữ liệu cần thiết cho Demand Forecasting

```python
# Các trường cần thiết từ NYC TLC Data
essential_fields = [
    'tpep_pickup_datetime',   # Thời gian đón khách (KEY!)
    'PULocationID',           # Zone đón khách (KEY!)
    'passenger_count',        # Số hành khách
    'trip_distance',          # Để validate quality
]

# Derived features
derived_features = [
    'pickup_hour',            # 0-23
    'pickup_day_of_week',     # 0-6 (Mon-Sun)
    'pickup_date',            # Date only
    'is_weekend',             # Boolean
    'is_rush_hour',           # Boolean (7-9am, 5-8pm)
]
```

### 1.3. So sánh thực nghiệm #1: Data Processing

```
┌────────────────────────────────────────────────────────────────────────┐
│               THỰC NGHIỆM 1: DATA PROCESSING BENCHMARK                 │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   Metrics đo lường:                                                    │
│   • Thời gian xử lý (Processing Time)                                 │
│   • RAM usage (Peak Memory)                                           │
│   • CPU utilization                                                    │
│                                                                        │
│   ┌─────────────┬─────────────┬─────────────┬─────────────┐           │
│   │   Data Size │   Pandas    │   Polars    │   Spark     │           │
│   ├─────────────┼─────────────┼─────────────┼─────────────┤           │
│   │   1 month   │     ✓       │     ✓       │     ✓       │           │
│   │   6 months  │     ?       │     ✓       │     ✓       │           │
│   │   1 year    │     OOM?    │     ✓       │     ✓       │           │
│   │   3 years   │     OOM     │     ?       │     ✓       │           │
│   └─────────────┴─────────────┴─────────────┴─────────────┘           │
│                                                                        │
│   * OOM = Out of Memory                                               │
│   * ? = Cần test trên laptop của bạn                                  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 **PHASE 2: FEATURE ENGINEERING**

### 2.1. Demand Aggregation Strategy

```
┌───────────────────────────────────────────────────────────────────────┐
│                    FEATURE ENGINEERING PIPELINE                        │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   RAW TRIP DATA                                                       │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │ pickup_datetime    │ PULocationID │ passenger_count │ ...    │   │
│   │ 2024-01-15 08:23:00│     132      │       2         │        │   │
│   │ 2024-01-15 08:24:00│     132      │       1         │        │   │
│   │ 2024-01-15 08:24:30│     132      │       3         │        │   │
│   │ 2024-01-15 08:25:00│     237      │       1         │        │   │
│   └──────────────────────────────────────────────────────────────┘   │
│                              │                                        │
│                              ▼ Aggregate by (zone, hour)              │
│                                                                       │
│   AGGREGATED DEMAND TABLE                                             │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │ zone_id │ date       │ hour │ pickup_count │ avg_passengers │   │
│   │   132   │ 2024-01-15 │  8   │     423      │      1.8       │   │
│   │   132   │ 2024-01-15 │  9   │     512      │      2.1       │   │
│   │   237   │ 2024-01-15 │  8   │     287      │      1.5       │   │
│   └──────────────────────────────────────────────────────────────┘   │
│                              │                                        │
│                              ▼ Add temporal & lag features            │
│                                                                       │
│   FINAL FEATURE TABLE                                                 │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │ zone │ date │ hour │ demand │ dow │ is_wknd │ lag_1h │ lag_24h│   │
│   │ 132  │01-15 │   8  │  423   │  1  │    0    │  380   │   415  │   │
│   │ 132  │01-15 │   9  │  512   │  1  │    0    │  423   │   498  │   │
│   └──────────────────────────────────────────────────────────────┘   │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

### 2.2. Feature List Chi Tiết

```python
# TEMPORAL FEATURES
temporal_features = {
    'hour': 'Giờ trong ngày (0-23)',
    'day_of_week': 'Ngày trong tuần (0=Mon, 6=Sun)',
    'day_of_month': 'Ngày trong tháng (1-31)',
    'week_of_year': 'Tuần trong năm (1-52)',
    'month': 'Tháng (1-12)',
    'is_weekend': 'Cuối tuần hay không',
    'is_holiday': 'Ngày lễ hay không (US holidays)',
    'is_rush_hour': 'Giờ cao điểm (7-9am, 5-8pm)',
}

# LAG FEATURES (Historical demand)
lag_features = {
    'demand_lag_1h': 'Nhu cầu 1 giờ trước',
    'demand_lag_2h': 'Nhu cầu 2 giờ trước',
    'demand_lag_24h': 'Nhu cầu cùng giờ hôm qua',
    'demand_lag_168h': 'Nhu cầu cùng giờ tuần trước',
    'demand_rolling_mean_6h': 'Trung bình 6 giờ gần nhất',
    'demand_rolling_mean_24h': 'Trung bình 24 giờ gần nhất',
}

# ZONE FEATURES
zone_features = {
    'zone_id': 'Mã khu vực (1-265)',
    'borough': 'Quận (Manhattan, Brooklyn, etc.)',
    'zone_type': 'Loại zone (Airport, Business, Residential)',
}
```

---

## 🎯 **PHASE 3: MÔ HÌNH DỰ ĐOÁN**

### 3.1. Chiến lược mô hình hóa

```
┌────────────────────────────────────────────────────────────────────────┐
│                       MODEL ARCHITECTURE                               │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  APPROACH 1: Zone-Agnostic Model (Đơn giản)                           │
│  ├─ Một model duy nhất cho tất cả zones                               │
│  ├─ Zone_id như một feature                                           │
│  └─ Pros: Đơn giản, học được pattern chung                            │
│                                                                        │
│  APPROACH 2: Per-Zone Models (Chính xác hơn)                          │
│  ├─ Mỗi zone có model riêng                                           │
│  ├─ 265 models cho 265 zones                                          │
│  └─ Pros: Capture được đặc thù từng khu vực                           │
│                                                                        │
│  APPROACH 3: Clustered Models (Cân bằng) ⭐ RECOMMENDED               │
│  ├─ Cluster zones theo behavior tương tự                              │
│  ├─ Một model cho mỗi cluster (5-10 clusters)                         │
│  └─ Pros: Cân bằng giữa độ chính xác và complexity                    │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### 3.2. So sánh thực nghiệm #2: Model Comparison

```
┌───────────────────────────────────────────────────────────────────────┐
│               THỰC NGHIỆM 2: MODEL COMPARISON                         │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   ┌─────────────────────────────────────────────────────────────┐     │
│   │  MODEL       │ Use Case        │ Pros           │ Cons      │     │
│   ├─────────────────────────────────────────────────────────────┤     │
│   │  XGBoost     │ Tabular data    │ Fast, accurate │ No native │     │
│   │  (Baseline)  │ với features    │ interpretable  │ time-aware│     │
│   ├─────────────────────────────────────────────────────────────┤     │
│   │  ARIMA/      │ Pure time       │ Statistical    │ Không có  │     │
│   │  SARIMA      │ series          │ sound          │ external  │     │
│   │              │                 │                │ features  │     │
│   ├─────────────────────────────────────────────────────────────┤     │
│   │  Prophet     │ Time series     │ Easy to use    │ May over- │     │
│   │  (Facebook)  │ with seasonality│ handles holiday│ smooth    │     │
│   ├─────────────────────────────────────────────────────────────┤     │
│   │  LSTM        │ Sequential      │ Can learn      │ Need lots │     │
│   │  (Optional)  │ patterns        │ complex pattern│ of data   │     │
│   └─────────────────────────────────────────────────────────────┘     │
│                                                                       │
│   Metrics:                                                            │
│   • MAE (Mean Absolute Error)                                         │
│   • RMSE (Root Mean Squared Error)                                    │
│   • MAPE (Mean Absolute Percentage Error)                             │
│   • Training Time                                                     │
│   • Inference Time                                                    │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

### 3.3. Đề xuất Model Pipeline

```python
# RECOMMENDED MODEL STACK
models_to_compare = {
    'baseline': {
        'name': 'Historical Average',
        'description': 'Trung bình cùng giờ, cùng thứ trong quá khứ',
        'complexity': '⭐',
    },
    'statistical': {
        'name': 'SARIMA',
        'description': 'Seasonal ARIMA cho time series',
        'complexity': '⭐⭐',
    },
    'gradient_boosting': {
        'name': 'XGBoost/LightGBM',
        'description': 'Tabular ML với feature engineering',
        'complexity': '⭐⭐⭐',
    },
    'deep_learning': {
        'name': 'LSTM/Transformer',
        'description': 'Deep learning cho sequence',
        'complexity': '⭐⭐⭐⭐',
    },
}
```

---

## 🎯 **PHASE 4: SO SÁNH BIG DATA VS NON-BIG DATA**

### 4.1. Thiết kế thực nghiệm chính

```
┌────────────────────────────────────────────────────────────────────────┐
│          THỰC NGHIỆM 3: BIG DATA VS NON-BIG DATA (TRỌNG TÂM)          │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   SCENARIO A: NON-BIG DATA APPROACH                                   │
│   ┌─────────────────────────────────────────────────────────────┐     │
│   │  • Tools: Pandas, DuckDB, Polars                            │     │
│   │  • Storage: Local Parquet files                             │     │
│   │  • Processing: Single machine, in-memory                    │     │
│   │  • Scaling: Chunked processing nếu cần                      │     │
│   │  • Orchestration: Simple Python scripts / Prefect           │     │
│   └─────────────────────────────────────────────────────────────┘     │
│                                                                        │
│   SCENARIO B: BIG DATA APPROACH                                       │
│   ┌─────────────────────────────────────────────────────────────┐     │
│   │  • Tools: Spark, Delta Lake                                 │     │
│   │  • Storage: MinIO (S3-compatible Object Storage)            │     │
│   │  • Processing: Distributed (even on single machine)         │     │
│   │  • Scaling: Designed for horizontal scaling                 │     │
│   │  • Orchestration: Airflow                                   │     │
│   └─────────────────────────────────────────────────────────────┘     │
│                                                                        │
│   COMPARISON DIMENSIONS:                                               │
│   ┌─────────────────────────────────────────────────────────────┐     │
│   │  Dimension          │  Small Data   │  Medium Data │ Large  │     │
│   │                     │  (1 month)    │  (1 year)    │(3 yrs) │     │
│   ├─────────────────────────────────────────────────────────────┤     │
│   │  Processing Time    │  Pandas wins  │  ~Equal      │ Spark  │     │
│   │  Memory Usage       │  Both OK      │  Pandas OOM? │ Spark  │     │
│   │  Setup Complexity   │  Pandas wins  │  Pandas wins │ ~Equal │     │
│   │  Maintainability    │  Pandas wins  │  ~Equal      │ Spark  │     │
│   │  Scalability        │  ~Equal       │  Spark wins  │ Spark  │     │
│   └─────────────────────────────────────────────────────────────┘     │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### 4.2. Benchmark Script Structure (Đề xuất)

```python
# benchmark/compare_approaches.py

class DataProcessingBenchmark:
    """So sánh Pandas vs Polars vs Spark"""
    
    def __init__(self, data_sizes=['1month', '6month', '1year']):
        self.data_sizes = data_sizes
        self.results = {}
    
    def benchmark_aggregation(self):
        """
        Task: GROUP BY (zone_id, hour) và COUNT(*)
        Đây là operation chính của demand forecasting
        """
        pass
    
    def benchmark_feature_engineering(self):
        """
        Task: Tạo lag features, rolling features
        """
        pass
    
    def collect_metrics(self):
        """
        Metrics:
        - wall_clock_time
        - peak_memory_mb
        - cpu_percent
        """
        pass

# Output: Table + Charts so sánh
```

### 4.3. Kịch bản so sánh chi tiết

```
┌────────────────────────────────────────────────────────────────────────┐
│                    BENCHMARK SCENARIOS                                 │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  SCENARIO 1: Data Ingestion                                           │
│  ─────────────────────────────                                        │
│  • Task: Đọc parquet files và filter invalid records                  │
│  • Pandas: pd.read_parquet() + filter                                 │
│  • Spark: spark.read.parquet() + filter                               │
│                                                                        │
│  SCENARIO 2: Aggregation (Core Operation)                             │
│  ─────────────────────────────────────────                            │
│  • Task: Count pickups per (zone, hour)                               │
│  • Pandas: groupby(['zone', 'hour']).size()                          │
│  • Spark: GROUP BY zone, hour                                         │
│                                                                        │
│  SCENARIO 3: Feature Engineering                                      │
│  ──────────────────────────────                                       │
│  • Task: Create lag features (shift operations)                       │
│  • Pandas: df.groupby('zone')['demand'].shift(1)                     │
│  • Spark: Window functions với LAG()                                  │
│                                                                        │
│  SCENARIO 4: Join Operations                                          │
│  ──────────────────────────                                           │
│  • Task: Join với zone metadata                                       │
│  • Pandas: pd.merge()                                                 │
│  • Spark: df.join()                                                   │
│                                                                        │
│  SCENARIO 5: End-to-End Pipeline                                      │
│  ─────────────────────────────                                        │
│  • Task: Full ETL + Feature Engineering                               │
│  • Compare total time, resource usage                                 │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🎯 **PHASE 5: SERVING & VISUALIZATION**

### 5.1. API Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        SERVING ARCHITECTURE                            │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐           │
│   │   FastAPI   │◀────▶│    Redis    │◀────▶│  PostgreSQL │           │
│   │   Server    │      │   (Cache)   │      │  (Features) │           │
│   └──────┬──────┘      └─────────────┘      └─────────────┘           │
│          │                                                             │
│          ▼                                                             │
│   ┌──────────────────────────────────────────────────────┐            │
│   │                    API ENDPOINTS                      │            │
│   │                                                      │            │
│   │  GET /api/v1/demand/predict                         │            │
│   │      ?zone_id=132&datetime=2024-01-15T08:00:00      │            │
│   │      → Returns: { "predicted_demand": 423 }         │            │
│   │                                                      │            │
│   │  GET /api/v1/demand/heatmap                         │            │
│   │      ?datetime=2024-01-15T08:00:00                  │            │
│   │      → Returns: [{ "zone_id": 132, "demand": 423 }] │            │
│   │                                                      │            │
│   │  GET /api/v1/zones/{zone_id}/history                │            │
│   │      → Returns: Historical demand for a zone        │            │
│   │                                                      │            │
│   └──────────────────────────────────────────────────────┘            │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### 5.2. Dashboard Components

```
┌────────────────────────────────────────────────────────────────────────┐
│                        DASHBOARD DESIGN                                │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   ┌──────────────────────────────────────────────────────────────┐    │
│   │                     NYC TAXI DEMAND DASHBOARD                 │    │
│   ├──────────────────────────────────────────────────────────────┤    │
│   │                                                              │    │
│   │   ┌─────────────────────┐  ┌─────────────────────────────┐  │    │
│   │   │                     │  │                             │  │    │
│   │   │   HEATMAP           │  │   TIME SERIES CHART         │  │    │
│   │   │   (Folium/Plotly)   │  │   (Selected Zone)           │  │    │
│   │   │                     │  │                             │  │    │
│   │   │   Màu theo demand   │  │   - Actual vs Predicted     │  │    │
│   │   │   intensity         │  │   - 24h/7d/30d view         │  │    │
│   │   │                     │  │                             │  │    │
│   │   └─────────────────────┘  └─────────────────────────────┘  │    │
│   │                                                              │    │
│   │   ┌─────────────────────┐  ┌─────────────────────────────┐  │    │
│   │   │                     │  │                             │  │    │
│   │   │   TOP 10 ZONES      │  │   MODEL PERFORMANCE         │  │    │
│   │   │   (Bar Chart)       │  │   (Metrics Cards)           │  │    │
│   │   │                     │  │                             │  │    │
│   │   │   Highest demand    │  │   MAE: 12.3                 │  │    │
│   │   │   right now         │  │   RMSE: 18.7                │  │    │
│   │   │                     │  │   MAPE: 8.2%                │  │    │
│   │   └─────────────────────┘  └─────────────────────────────┘  │    │
│   │                                                              │    │
│   └──────────────────────────────────────────────────────────────┘    │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 📋 **ROADMAP TRIỂN KHAI CHI TIẾT**

```
┌────────────────────────────────────────────────────────────────────────┐
│                         IMPLEMENTATION ROADMAP                         │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  TUẦN 1-2: DATA EXPLORATION & BASELINE                                │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                                 │
│  □ Download 1 tháng data (Yellow + Green taxi)                        │
│  □ EDA notebook: Hiểu distribution, patterns                          │
│  □ Implement Pandas baseline pipeline                                 │
│  □ Create aggregated demand table                                     │
│  □ Simple baseline model (Historical average)                         │
│                                                                        │
│  TUẦN 3-4: FEATURE ENGINEERING                                        │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━                                         │
│  □ Implement full feature engineering (Pandas)                        │
│  □ Add external data: holidays, weather (optional)                    │
│  □ Create train/validation/test split (temporal)                      │
│  □ Replicate feature engineering in Spark                             │
│                                                                        │
│  TUẦN 5-6: MODEL DEVELOPMENT                                          │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━                                          │
│  □ XGBoost model development                                          │
│  □ Hyperparameter tuning (Optuna)                                     │
│  □ SARIMA model (for comparison)                                      │
│  □ Model evaluation & comparison                                      │
│                                                                        │
│  TUẦN 7-8: BIG DATA BENCHMARK                                         │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━                                         │
│  □ Scale up data (1 year, then 3 years)                               │
│  □ Run Pandas pipeline on larger data                                 │
│  □ Run Spark pipeline on same data                                    │
│  □ Collect & analyze benchmark metrics                                │
│  □ Document findings                                                   │
│                                                                        │
│  TUẦN 9-10: SERVING & VISUALIZATION                                   │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                                    │
│  □ FastAPI service                                                    │
│  □ Redis caching layer                                                │
│  □ Superset dashboard                                                 │
│  □ Interactive map (Folium/Plotly)                                    │
│                                                                        │
│  TUẦN 11-12: DOCUMENTATION & PRESENTATION                             │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                              │
│  □ Final benchmark report                                             │
│  □ System documentation                                               │
│  □ Demo preparation                                                   │
│  □ Presentation slides                                                │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 💡 **TIPS CHO LAPTOP macOS**

### Resource Management

```yaml
# docker-compose resource limits (thêm vào mỗi service)
services:
  spark-master:
    deploy:
      resources:
        limits:
          memory: 4G  # Giới hạn RAM cho Spark
          cpus: '2'
```

### Chiến lược tiết kiệm tài nguyên

| Tip | Mô tả |
|-----|-------|
| **Không chạy tất cả services** | Chỉ start services cần thiết cho task hiện tại |
| **Batch processing by chunks** | Xử lý data theo từng chunk nhỏ |
| **Use Polars thay vì Pandas** | Polars nhanh hơn 10-50x và memory efficient hơn |
| **DuckDB cho analytics** | In-process OLAP database, không cần server |
| **Sample data cho development** | Chỉ dùng full data khi benchmark |

### Workflow đề xuất

```bash
# Development mode (nhẹ)
make batch_down stream_down airflow_down
# Chỉ dùng local Python + Polars + DuckDB

# Benchmark mode (cần so sánh Spark)
make batch_up
# Chạy Spark jobs

# Full demo mode
make run_all
```

---

## 📊 **KẾT QUẢ MONG ĐỢI**

### 1. Technical Deliverables
- [ ] EDA Notebook với insights về NYC taxi patterns
- [ ] Feature Engineering pipeline (Pandas + Spark versions)
- [ ] ML Models (XGBoost, SARIMA, optional LSTM)
- [ ] API Service (FastAPI + Redis)
- [ ] Dashboard (Superset + Folium map)

### 2. Benchmark Report
- [ ] Processing time comparison (Pandas vs Polars vs Spark)
- [ ] Memory usage comparison
- [ ] Scalability analysis
- [ ] Recommendations: Khi nào dùng Big Data, khi nào không

### 3. Key Findings Template

```markdown
## Key Findings

### When to use Big Data tools (Spark):
- Data > X GB (xác định qua benchmark)
- Need for incremental processing
- Production system with continuous data flow
- Team collaboration on data pipelines

### When NOT to use Big Data tools:
- Data < Y GB (fits in memory)
- One-off analysis
- Rapid prototyping
- Simple aggregations
```

---

Bạn muốn tôi đi sâu vào phần nào trước? Tôi có thể:
1. **Bắt đầu với EDA notebook** - Phân tích dữ liệu
2. **Setup benchmark framework** - Tạo cấu trúc so sánh
3. **Design feature engineering pipeline** - Chi tiết feature store
4. **Thiết kế ML pipeline** - Model training workflow