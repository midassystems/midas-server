# Midas Server

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Overview

The **Midas Server** is a core component of the Midas Trading System, providing backend services for both historical and live trading data. It handles data storage, API requests for backtesting results, live trading sessions, and overall system integration. The server is designed to be Docker-deployable for both local and remote setups, ensuring flexibility and ease of use.

## Features

- **Historical Data Storage**: Efficient storage and retrieval of historical market data for backtesting.
- **Trading Data Management**: Process and store live trading session results.
- **REST API**: Expose endpoints for interaction with the Midas Trading System's components.
- **Docker Deployment**: Simplified deployment using Docker Compose.
- **Scalability**: Support for both local and remote environments.

## Deployment

### Prerequisites

**Docker and Docker Compose**:

- Ensure Docker is installed on your system. Refer to [Docker Installation Guide](https://docs.docker.com/get-docker/).
- Install Docker Compose if not included with your Docker installation.

### Environment Variables

The server uses environment variables to configure its behavior. These can be defined in a `.env` file in the root directory.

Example `.env` file:

```env
# Postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_HOST=postgres
TRADING_DATABASE_URL=postgres://postgres:password@postgres:5432/trading_data
HISTORICAL_DATABASE_URL=postgres://postgres:password@postgres:5432/market_data
INSTRUMENT_DATABASE_URL=postgres://postgres:password@postgres:5432/market_data

# Api
HISTORICAL_PORT="8080"
TRADING_PORT="8081"
INSTRUMENT_PORT="8082"
HISTORICAL_URL=http://127.0.0.1:8080
INSTRUMENT_URL=http://127.0.0.1:8082


# Dir
LOG_DIR=./logs
DATA_DIR=./data
SCRIPTS_DIR=./scripts
BIN_DIR=./bin
POSTGRES_DIR=./postgres
LIB_DIR=./bin
RAW_DIR=./data
PROCESSED_DIR=../data/processed_data/
```

### Setup and Run

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/midassystems/midas-server.git
   cd midas-server
   ```

2. **Environment Variables**:

   The server uses environment variables to configure its behavior. These can be defined in a `.env` file in the root directory.

   Example `.env` file:

   ```env
   # Postgres
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=password
   POSTGRES_HOST=postgres
   TRADING_DATABASE_URL=postgres://postgres:password@postgres:5432/trading_data
   HISTORICAL_DATABASE_URL=postgres://postgres:password@postgres:5432/market_data
   INSTRUMENT_DATABASE_URL=postgres://postgres:password@postgres:5432/market_data

   # Api
   HISTORICAL_PORT="8080"
   TRADING_PORT="8081"
   INSTRUMENT_PORT="8082"
   HISTORICAL_URL=http://127.0.0.1:8080
   INSTRUMENT_URL=http://127.0.0.1:8082


   # Dir
   LOG_DIR=./logs
   DATA_DIR=./data
   SCRIPTS_DIR=./scripts
   BIN_DIR=./bin
   POSTGRES_DIR=./postgres
   LIB_DIR=./bin
   RAW_DIR=./data
   PROCESSED_DIR=../data/processed_data/
   ```

3. **Start the Server**:
   Use the provided `docker-compose.yml` file to deploy the server.

```bash
docker compose --profile prod up --build -d
```

## API Endpoints

The following endpoints will be updated soon with detailed paths and functionality:

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
