```markdown
# ✈️ OpenSky Live Flight Streaming Pipeline

A real-time flight data streaming pipeline that extracts, processes, and serves live aircraft telemetry from the OpenSky Network API. Built with Python, Kafka, Redis, and WebSockets for scalable real-time data processing.

## 🌟 Features

- **Real-time Data Extraction**: Polls OpenSky Network API at optimal intervals (5s authenticated / 10s anonymous)
- **Streaming Pipeline**: Kafka-based message queue for reliable, scalable data streaming
- **In-Memory Caching**: Redis for low-latency flight data access and real-time queries
- **WebSocket Server**: Real-time flight updates pushed to connected clients
- **Batch Storage**: Hour-partitioned Parquet files for analytics and historical data
- **Interactive Dashboard**: Streamlit-based real-time flight tracker with maps and alerts
- **Fault Tolerance**: Automatic retry with exponential backoff
- **Data Deduplication**: Avoids redundant processing of unchanged flight states
- **Emergency Alerts**: Real-time notifications for squawk 7700 and low altitude events

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────┐     ┌─────────┐     ┌──────────────┐
│  OpenSky    │────▶│  Python  │────▶│  Kafka  │────▶│   Consumer   │
│    API      │     │ Pipeline │     │  Queue  │     │   Processors │
└─────────────┘     └──────────┘     └─────────┘     └──────────────┘
                          │                │                  │
                          ▼                ▼                  ▼
                    ┌──────────┐    ┌──────────┐    ┌────────────────┐
                    │  Redis   │    │  Parquet │    │   WebSocket    │
                    │  Cache   │    │  Storage │    │   Server       │
                    └──────────┘    └──────────┘    └────────────────┘
                          │                                      │
                          └──────────────┬───────────────────────┘
                                         ▼
                                  ┌─────────────┐
                                  │  Streamlit  │
                                  │  Dashboard  │
                                  └─────────────┘
```

## 📦 Prerequisites

- Python 3.8+
- Docker & Docker Compose (optional, for Kafka/Redis)
- OpenSky Network account (optional, for authenticated access)

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/opensky-streaming-pipeline.git
cd opensky-streaming-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set Up Environment Variables

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your credentials
# OPENSKY_USER=your_username
# OPENSKY_PASSWORD=your_password
# KAFKA_BROKER=localhost:9092
# REDIS_URL=redis://localhost:6379
```

### 4. Start Services (Docker Compose)

```bash
docker-compose up -d
```

### 5. Run the Pipeline

```bash
# Run the main streaming pipeline
python stream_pipeline.py
```

### 6. Launch Dashboard (in new terminal)

```bash
streamlit run dashboard.py
```

### 7. Access Services

- **Dashboard**: http://localhost:8501
- **WebSocket**: ws://localhost:8765
- **Kafka**: localhost:9092
- **Redis**: localhost:6379

## 📊 Data Flow

### 1. Extraction
- Polls OpenSky API every 5-10 seconds
- Fetches all aircraft states within bounding box
- Handles rate limiting with exponential backoff

### 2. Processing
- Deduplicates flight states based on `last_contact`
- Enriches data with ingestion timestamp
- Converts timestamps to UTC datetime

### 3. Streaming
- **Kafka**: Publishes each flight update to `opensky-flights` topic
- **Redis**: Caches current flight state with 5-minute TTL
- **WebSocket**: Broadcasts updates to connected clients

### 4. Storage
- **Parquet Files**: Batched writes every 100 records or 5 seconds
- **Partitioning**: `year/month/day/hour` for efficient querying

### 5. Monitoring
- Real-time alerts for emergency squawks (7700)
- Low altitude warnings (< 500 feet)
- Active flight tracking in Redis

## 📁 Project Structure

```
opensky-streaming-pipeline/
├── stream_pipeline.py      # Main streaming pipeline
├── consumer.py              # Kafka consumer for real-time processing
├── dashboard.py             # Streamlit dashboard
├── extract.py               # Original extraction script
├── requirements.txt         # Python dependencies
├── docker-compose.yml       # Docker services configuration
├── .env.example             # Environment variables template
├── Dockerfile               # Pipeline container
├── Dockerfile.dashboard     # Dashboard container
└── README.md               # This file
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENSKY_USER` | OpenSky API username | Empty (anonymous) |
| `OPENSKY_PASSWORD` | OpenSky API password | Empty (anonymous) |
| `KAFKA_BROKER` | Kafka broker address | `localhost:9092` |
| `KAFKA_TOPIC` | Kafka topic name | `opensky-flights` |
| `REDIS_URL` | Redis connection URL | `redis://localhost:6379` |
| `WEBSOCKET_PORT` | WebSocket server port | `8765` |

### Bounding Box

Default bounding box covers:
- Latitude: -35.0 to 40.0
- Longitude: -20.0 to 104.0

Modify in `stream_pipeline.py`:
```python
BOUNDING_BOX = {
    "lamin": -35.0, "lomin": -20.0,
    "lamax": 40.0, "lomax": 104.0,
}
```

## 📈 Performance Tuning

### Polling Intervals
- **Authenticated**: 5 seconds (respects API limits)
- **Anonymous**: 10 seconds
- **Exponential Backoff**: Max 60 seconds on errors

### Batch Settings
- **Batch Size**: 100 flights
- **Batch Timeout**: 5 seconds
- **Parquet Compression**: Snappy

### Kafka Settings
- **Compression**: Snappy
- **Batching**: 100 messages or 100ms linger
- **Partition Key**: `icao24` for consistent ordering per aircraft

## 🛠️ Advanced Usage

### Custom Consumer

Create your own consumer to process flight data:

```python
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer, JSONDeserializer

consumer = DeserializingConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'latest',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': JSONDeserializer(),
})
consumer.subscribe(['opensky-flights'])

while True:
    msg = consumer.poll(1.0)
    if msg:
        flight = msg.value()
        # Your custom processing
        print(f"Flight {flight['callsign']} at {flight['latitude']}, {flight['longitude']}")
```

### WebSocket Client

Connect to the WebSocket server:

```javascript
const ws = new WebSocket('ws://localhost:8765');

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'update') {
        console.log('Flight update:', data.data);
        // Update your UI
    }
};

// Subscribe to initial state
ws.send(JSON.stringify({ type: 'subscribe' }));
```

### Querying Parquet Data

```python
import pandas as pd
from pathlib import Path

# Read all parquet files for a specific day
files = Path("live_flights_lake").glob("year=2026/month=09/day=08/*.parquet")
df = pd.concat([pd.read_parquet(f) for f in files])
print(f"Loaded {len(df)} flight records")
```

### Redis Queries

```python
import redis
import json

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Get all active flights
active_flights = r.zrange('active_flights', 0, -1, desc=True)
for icao in active_flights[:10]:
    flight_data = r.get(f'flight:{icao}')
    if flight_data:
        flight = json.loads(flight_data)
        print(f"{flight['callsign']} - {flight['latitude']}, {flight['longitude']}")

# Get flight track
track = r.zrange('flight_track:abc123', 0, -1)
for point in track:
    print(json.loads(point))
```

## 🔍 Monitoring & Logging

### Check Pipeline Status

```bash
# View Kafka consumer groups
docker-compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list

# Check Redis active flights
docker-compose exec redis redis-cli ZCARD active_flights

# View logs
docker-compose logs -f pipeline
```

### Metrics Available in Redis

| Key | Description |
|-----|-------------|
| `flight_stats:total` | `total_flights` counter |
| `flight_stats:countries` | Flight counts by country |
| `active_flights` | Sorted set of active flights |
| `flight:{icao24}` | Individual flight data (TTL: 5 min) |
| `flight_track:{icao24}` | Flight track history (last 1000 positions) |

### Logging Levels

```python
# Set logging level in code
logging.basicConfig(level=logging.DEBUG)  # For debugging
logging.basicConfig(level=logging.INFO)   # Default
logging.basicConfig(level=logging.WARNING) # For production
```

## 🚨 Alert System

The pipeline automatically detects:
- **Squawk 7700**: Emergency (published to Redis channel `alerts`)
- **Low Altitude**: Below 500 feet (logs warning)
- **Rapid Descent**: Detected via vertical rate (coming soon)

### Subscribe to Alerts

```python
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
pubsub = r.pubsub()
pubsub.subscribe('alerts')

for message in pubsub.listen():
    if message['type'] == 'message':
        alert = json.loads(message['data'])
        print(f"🚨 ALERT: {alert['type']} - {alert['flight']['callsign']}")
```

## 🧪 Testing

```bash
# Run tests (coming soon)
pytest tests/

# Manual test with sample data
python -c "from stream_pipeline import FlightCache; import asyncio; asyncio.run(FlightCache().connect())"

# Test API connection
python -c "import requests; print(requests.get('https://opensky-network.org/api/states/all').status_code)"
```

## 📊 Dashboard Features

- **Live Flight Positions**: Folium map with real-time aircraft positions
- **Flight Statistics**: Total, active, and country breakdown
- **Emergency Alerts**: Visual warnings for squawk 7700
- **Flight Details Table**: Sortable table with all flight data
- **Auto-Refresh**: Updates in real-time via Redis
- **Responsive Design**: Works on desktop and mobile

## 🐳 Docker Deployment

### Build Images

```bash
# Build pipeline image
docker build -t opensky-pipeline -f Dockerfile .

# Build dashboard image
docker build -t opensky-dashboard -f Dockerfile.dashboard .
```

### Run in Production

```bash
# Start all services
docker-compose up -d

# Scale consumers (optional)
docker-compose up -d --scale consumer=3

# View logs
docker-compose logs -f
```

### Environment Variables for Docker

```yaml
# docker-compose.yml
environment:
  - OPENSKY_USER=${OPENSKY_USER}
  - OPENSKY_PASSWORD=${OPENSKY_PASSWORD}
  - KAFKA_BROKER=kafka:9092
  - REDIS_URL=redis://redis:6379
  - WEBSOCKET_PORT=8765
```

## 🔐 Security Considerations

- **API Credentials**: Never commit credentials to version control. Use `.env` files.
- **Redis**: Consider adding password protection in production
- **Kafka**: Use SSL/TLS for production deployments
- **WebSocket**: Implement authentication for public-facing deployments
- **Rate Limiting**: Respect OpenSky API limits to avoid being banned

## 📈 Scaling the Pipeline

### Horizontal Scaling
1. **Multiple Pipeline Instances**: Run multiple pipeline instances with different bounding boxes
2. **Kafka Partitions**: Increase partitions for higher throughput
3. **Consumer Groups**: Add more consumers for parallel processing
4. **Redis Cluster**: Use Redis Cluster for distributed caching

### Vertical Scaling
1. **Batch Size**: Increase batch size for higher throughput
2. **Parquet Compression**: Use different compression algorithms
3. **Thread Pools**: Implement async I/O for better performance

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Contribution Guidelines
- Follow PEP 8 style guide
- Add tests for new features
- Update documentation
- Keep commits atomic and well-described

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [OpenSky Network](https://opensky-network.org/) for providing the flight data API
- Apache Kafka for distributed streaming
- Redis for in-memory data caching
- Streamlit for interactive dashboard framework
- All open-source contributors who made this possible

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/opensky-streaming-pipeline/issues)
- **Documentation**: [Wiki](https://github.com/yourusername/opensky-streaming-pipeline/wiki)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/opensky-streaming-pipeline/discussions)

## 🗺️ Roadmap

- [ ] ML-based anomaly detection
- [ ] Flight path prediction
- [ ] Historical playback feature
- [ ] Mobile app companion
- [ ] Integration with AIS data (ships)
- [ ] Weather data integration
- [ ] Performance monitoring dashboard
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Kubernetes deployment manifests

---

**Built with ❤️ for the aviation and data engineering community**

⭐ Star this repository if you find it useful!
```
