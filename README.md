
## Project Overview

NexLib is a Python library for industrial data processing and communication. It provides modular components organized into distinct packages:

- **NexCore**: Core utilities (logging, configuration, caching, utilities)
- **NexSig**: Signal/messaging components (RabbitMQ, MQTT, REST, Kafka, notifications)
- **NexSto**: Storage backends (MongoDB, MariaDB, PostgreSQL, Redis)
- **NexDrv**: Device drivers (OPC UA, RTSP, Modbus, PLC)
- **NexExt**: Extensions (FTP, reporting/plotting)

## Architecture

The library follows a modular architecture where components are loosely coupled and communicate through:

1. **Shared queues**: `queue.Queue()` instances passed between components for data flow
2. **Threading locks**: `threading.Lock()` for thread-safe operations
3. **Configuration-driven**: JSON configuration files in `NexCore/` directory control component behavior

### Key Design Patterns

- **Thread-based processing**: Most components inherit from `threading.Thread` for concurrent operation
- **Queue-based messaging**: Components communicate via shared `queue.Queue` instances
- **Configuration abstraction**: `NexCore.xCfg.Cfg` class handles JSON configuration loading
- **Logging integration**: `NexCore.xLog.NexLog` provides centralized logging with `loguru`

## Development Commands

### Environment Setup
```bash
# Install dependencies (Python 3.9+ required, 3.11.5+ preferred)
pip install -r requirements.txt

# Activate virtual environment if using one
source .venv/bin/activate  # macOS/Linux
```

### Testing
```bash
# Run main test suite
python test_main.py

# Run specific test modules
python test_send.py
python test_recv.py

# Run core listener (RabbitMQ + MongoDB integration)
python core_listener.py
```

### Configuration Files

Configuration files are located in `NexCore/`:
- `core.json`: Main application configuration
- `rabbitmq_cfg.json`: RabbitMQ connection settings
- `mongo_cfg.json`: MongoDB connection settings

## Key Components

### NexCore.xCfg
Handles JSON configuration loading with the pattern:
```python
from NexCore.xCfg import Cfg
config = Cfg('NexCore', 'config_file.json').get_cfg()
```

### NexCore.xLog
Centralized logging using loguru:
```python
from NexCore.xLog import NexLog
logger = NexLog('ComponentName').get_logger()
```

### NexSig.xRabbit
RabbitMQ communication with support for multiple exchange types and connection patterns.

### NexSto.xMongo
MongoDB operations with threading support and shared queue processing.

## Integration Patterns

The typical integration pattern involves:
1. Creating shared `queue.Queue()` and `threading.Lock()` instances
2. Passing these to component constructors
3. Starting components as threads
4. Components process data from queues and communicate results back

Example from `core_listener.py`:
```python
inMsgQueue = queue.Queue()
msgLock = threading.Lock()

# Initialize MongoDB processor
mongo = Mongo(inMsgQueue, msgLock)
mongo.start()

# Initialize RabbitMQ receiver
mq = MqRecv(host, port, exchange, queue, user, pwd, inMsgQueue, msgLock)
mq.start_consuming()
```
