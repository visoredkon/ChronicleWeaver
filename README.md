# ChronicleWeaver

Sistem **Publish-Subscribe Log Aggregator** dengan **idempotent consumer** dan **deduplication** yang dibangun menggunakan `Python`, `FastAPI`, dan `asyncio`. Sistem dirancang *crash-tolerant* dengan *persistent deduplication store* menggunakan `SQLite`.

## Arsitektur Sistem
![ChronicleWeaver Architecture](architecture.svg)

Sistem ChronicleWeaver terdiri dari dua *main services*:
### 1. Aggregator Service
*Service* utama yang menerima dan memproses *event* dengan fitur:
- **Statistics**          : Monitoring *received*, *processed*, dan *dropped events*
- **Consumer Service**    : *Background worker* yang memproses *event*
- **Deduplication Store** : *Persistent storage* untuk mencegah *event* duplikat
- **Event Publisher API** : Menerima *single* atau *batch events* melalui HTTP POST
- **Event Queue**         : *In-memory queue* untuk *pipelining* antara *publisher* dan *consumer*

### 2. Publisher Service
*Service* untuk *stress testing* yang mengirim 5000 *events* ke *aggregator* dengan 20% *duplicate events* untuk validasi sistem.

## Asumsi Sistem
### 1. Lingkungan dan Deployment
- Semua komponen berjalan di *local environment* menggunakan Docker
- Sistem dirancang untuk *single instance* (tidak *distributed*)
- Komunikasi antar *services* menggunakan Docker *internal networking*

### 2. Timing dan Synchronization
- *Timestamp* dikirim oleh *publisher* dan diasumsikan sudah sinkron
- Tidak ada validasi *timestamp* atau koreksi *clock skew*
- Sistem menggunakan waktu *host* yang sama untuk semua komponen

### 3. Event Delivery dan Ordering
- *Duplicate events* dapat terjadi karena *network retries* atau *publisher logic*
- Tidak ada jaminan *ordering* antar *events*

## Build dan Run
### Menggunakan Docker Compose (*recommended*)
#### 1. Build Images
Dari *root directory project*:
```fish
docker compose -f docker/docker-compose.yml build
```

Atau *build* dengan *cache bypass*:
```fish
docker compose -f docker/docker-compose.yml build --no-cache
```

#### 2. Run Services
*Start* semua *services* (*aggregator* + *publisher*):
```fish
docker compose -f docker/docker-compose.yml up
```

*Run* in *background* (*detached mode*):
```fish
docker compose -f docker/docker-compose.yml up -d
```

#### 3. View Logs
Lihat *logs* semua *services*:
```fish
docker compose -f docker/docker-compose.yml logs -f
```

Lihat *logs service* spesifik:
```fish
docker compose -f docker/docker-compose.yml logs -f aggregator
docker compose -f docker/docker-compose.yml logs -f publisher
```

#### 4. Stop Services
*Stop* tanpa *remove containers*:
```fish
docker compose -f docker/docker-compose.yml stop
```

*Stop* dan *remove containers*:
```fish
docker compose -f docker/docker-compose.yml down
```

*Stop* dan *remove containers* + *volumes* (hapus data):
```fish
docker compose -f docker/docker-compose.yml down -v
```

### Menggunakan Docker Manual
#### 1. Build Aggregator Image
Dari *root directory project*:
```fish
docker build -f docker/aggregator.Dockerfile -t chronicleweaver-aggregator:latest .
```

#### 2. Build Publisher Image
```fish
docker build -f docker/publisher.Dockerfile -t chronicleweaver-publisher:latest .
```

#### 3. Create Network
```fish
docker network create chronicleweaver-network
```

#### 4. Run Aggregator Container
```fish
docker run -d \
  --name chronicleweaver-aggregator \
  --network chronicleweaver-network \
  -p 8000:8000 \
  -v chronicleweaver-data:/app/data \
  chronicleweaver-aggregator:latest
```

#### 5. Run Publisher Container (Stress Test)
*Wait aggregator ready*, kemudian:
```fish
docker run \
  --name chronicleweaver-publisher \
  --network chronicleweaver-network \
  -e AGGREGATOR_HOST=chronicleweaver-aggregator \
  -e AGGREGATOR_PORT=8000 \
  chronicleweaver-publisher:latest
```

### Local Development (Tanpa Docker)
#### 1. Install Dependencies
Menggunakan `uv` (recommended):
```fish
uv sync
```

Atau menggunakan `pip`:
```fish
pip install -e .
```

#### 2. Run Aggregator
```fish
fastapi dev src/aggregator/app/main.py --host 0.0.0.0 --port 8000
```

Atau *production mode*:
```fish
fastapi run src/aggregator/app/main.py --host 0.0.0.0 --port 8000
```

#### 3. Run Publisher (Terminal Terpisah)
```fish
python -m src.publisher.app.main
```

Atau dengan environment variables:
```fish
AGGREGATOR_HOST=localhost AGGREGATOR_PORT=8000 python -m src.publisher.app.main
```

## API Endpoints
### 1. POST `/publish`
*Publish single* atau *batch events* ke *aggregator*.

**Request Body:**
```json
{
  "events": [
    {
      "event_id": "event-001",
      "topic": "user-actions",
      "source": "web-app",
      "payload": {
        "message": "User logged in",
        "timestamp": "2025-10-28T10:00:00Z"
      },
      "timestamp": "2025-10-28T10:00:00Z"
    }
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Published 1 events",
  "events_count": 1
}
```

### 2. GET `/events?topic={topic}`
*Retrieve* semua *unique events* untuk *topic* tertentu.

**Query Parameters:**
- `topic` (*optional*): *Filter events* berdasarkan *topic*. Jika tidak diberikan, *return* semua *events*.

**Response:**
```json
{
  "count": 10,
  "events": [
    {
      "event_id": "event-001",
      "topic": "user-actions",
      "source": "web-app",
      "payload": {
        "message": "User logged in",
        "timestamp": "2025-10-28T10:00:00Z"
      },
      "timestamp": "2025-10-28T10:00:00Z"
    }
  ]
}
```

### 3. GET `/stats`
*Retrieve system statistics* dan *monitoring information*.

**Response:**
```json
{
  "received": 5000,
  "unique_processed": 4000,
  "duplicated_dropped": 1000,
  "topics": ["user-actions", "system-events"],
  "uptime": 3600
}
```

**Field Explanations:**
- `received`: Total *events* yang diterima sistem
- `unique_processed`: Total *unique events* yang berhasil diproses
- `duplicated_dropped`: Total *duplicate events* yang di-*drop*
- `topics`: *List* semua *topics* yang pernah diproses
- `uptime`: *System uptime* dalam *seconds*

### 4. GET `/health`
*Health check endpoint* untuk *monitoring*.

**Response:**
```json
{
  "message": "healthy"
}
```

### 5. GET `/`
*Root endpoint* untuk verifikasi *service running*.

**Response:**
```json
{
  "message": "ChronicleWeaver is running..."
}
```

### 6. GET `/docs` dan `/redoc`
*Auto-generated API documentation* menggunakan *Swagger UI* dan *ReDoc*.

## Environment Variables
### Aggregator Service
- `APP_PORT`: Port untuk *aggregator service* (*default*: `8000`)
- `DEDUPLICATION_DB_PATH`: *Path* untuk SQLite *database* (*default*: `/app/data/chronicle.db`)

### Publisher Service
- `AGGREGATOR_HOST`: *Hostname aggregator service* (*default*: `localhost`)
- `AGGREGATOR_PORT`: Port *aggregator service* (*default*: `8000`)

### Contoh Kustomisasi
Buat file `.env` di folder `docker/`:

```env
APP_PORT=8080
DEDUPLICATION_DB_PATH=/app/data/chronicle.db

AGGREGATOR_HOST=aggregator
AGGREGATOR_PORT=8080
```

Kemudian update `docker/docker-compose.yml` untuk load `.env` file dengan menambahkan:
```yaml
services:
  aggregator:
    env_file:
      - .env
```

## Testing
### Run All Tests
```fish
pytest
```

### Run Tests dengan Verbose Output
```fish
pytest -v
```

### Run Specific Test File
```fish
pytest tests/test_01_deduplication.py
pytest tests/test_05_batch_stress.py
```

### Test Suites
1. **test_01_deduplication.py**: *Deduplication validation*
2. **test_02_deduplication_persistance.py**: *Persistence after restart*
3. **test_03_event_schema_validation.py**: *Schema validation tests*
4. **test_04_data_consistency.py**: *Data consistency* antara GET `/events` dan `/stats`
5. **test_05_batch_stress.py**: *Small batch stress tests* untuk mengukur performa
