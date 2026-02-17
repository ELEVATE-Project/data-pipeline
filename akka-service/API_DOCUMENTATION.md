# Akka Service API Documentation

## Base URL
```
http://localhost:8080/api
```

## Authentication
All endpoints require the `Authorization` header:
```
Authorization: your-api-token
```

---

## CSV Management

### Upload CSV File
```http
POST /api/csv/upload
Content-Type: multipart/form-data
Authorization: your-api-token

Body: file=<csv-file>
```

**Response (200 OK):**
```
CSV file uploaded successfully as: dd-MM-yyyy-HH-mm-ss.csv
```

**Error (400):**
- CSV headers don't match expected format
- File is not a valid CSV

---

### List Uploaded CSV Files
```http
GET /api/csv/list
Authorization: your-api-token
```

**Response (200 OK):**
```
filename1.csv
filename2.csv
filename3.csv
```

---

## Health Check

### Full Health Check (All Services)
```http
GET /api/health
Authorization: your-api-token
```

**Response (200 OK):**
```json
{
  "flink": {
    "cluster": {
      "status": "HEALTHY",
      "taskmanagers": 2,
      "slotsTotal": 4,
      "slotsAvailable": 2,
      "jobsRunning": 3,
      "jobsFinished": 5,
      "jobsCancelled": 0,
      "jobsFailed": 0,
      "flinkVersion": "1.14.0"
    },
    "jobs": [
      { "name": "ProjectsStreamJob", "status": "RUNNING" },
      { "name": "SurveysStreamJob", "status": "RUNNING" }
    ]
  },
  "kafka": {
    "status": "HEALTHY",
    "broker": "localhost:9092"
  },
  "metabase": {
    "status": "HEALTHY",
    "url": "https://qa.elevate.metabase.shikshalokam.org/api/health"
  },
  "timestamp": "2026-02-17T10:30:45.123Z"
}
```

**Response time:** ~40 seconds

---

### Individual Service Health Checks

#### Flink Health
```http
GET /api/health/flink
Authorization: your-api-token
```

**Response (200 OK):** FlinkHealth object | **Time:** ~10-15 sec

---

#### Kafka Health
```http
GET /api/health/kafka
Authorization: your-api-token
```

**Response (200 OK):**
```json
{
  "status": "HEALTHY",
  "broker": "localhost:9092"
}
```

**Time:** ~25-35 sec | **Note:** Tests full producer-consumer round-trip

---

#### Metabase Health
```http
GET /api/health/metabase
Authorization: your-api-token
```

**Response (200 OK):**
```json
{
  "status": "HEALTHY",
  "url": "https://qa.elevate.metabase.shikshalokam.org/api/health"
}
```

**Time:** ~2-5 sec

---

## Error Responses

### 401 Unauthorized
```json
{
  "status": 401,
  "message": "Invalid or missing token"
}
```

### 500 Internal Server Error
```json
{
  "status": 500,
  "message": "Error description"
}
```

---

## Quick Examples

### Upload CSV
```bash
curl -X POST http://localhost:8080/api/csv/upload \
  -H "Authorization: your-token" \
  -F "file=@data.csv"
```

### List CSVs
```bash
curl http://localhost:8080/api/csv/list \
  -H "Authorization: your-token"
```

### Check Kafka (fast)
```bash
curl http://localhost:8080/api/health/kafka \
  -H "Authorization: your-token" | jq
```

### Full Health Check
```bash
curl http://localhost:8080/api/health \
  -H "Authorization: your-token" | jq
```

---

## Status Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 401 | Unauthorized (missing/invalid token) |
| 400 | Bad request (invalid file format) |
| 500 | Server error |

---

## Performance

| Endpoint | Response Time |
|----------|---|
| `/api/csv/upload` | <1 sec |
| `/api/csv/list` | <1 sec |
| `/api/health/metabase` | 2-5 sec |
| `/api/health/flink` | 10-15 sec |
| `/api/health/kafka` | 25-35 sec |
| `/api/health` (all) | ~40 sec |

---

## Configuration

Required in `application.conf`:
```properties
security.api-token = "your-api-token"
file.sinkDirectory = "/path/to/csv/files"
services.kafka.broker-list = "localhost:9092"
services.flink.url = "https://flink-url"
metabase.url = "https://metabase-url"
```