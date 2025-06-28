# MQS - Simple Message Queue Service

MQS is a lightweight, self-hosted message queue service built with Go. It provides a simple HTTP API for creating topics, publishing messages, and managing webhooks for message delivery.

## Features

- **Topics**: Create topics to group related messages.
- **Messages**: Publish JSON-based messages to topics.
- **Webhooks**: Subscribe to topics with webhooks to receive messages as they are published.
- **Scalable**: Run multiple message publishers concurrently.
- **OpenAPI Specification**: A full OpenAPI v3 specification is provided in `openapi.yaml`.

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

### Running with Docker

The easiest way to get MQS up and running is with Docker Compose. A `docker-compose.yml` file is provided for your convenience.

1.  **Clone the repository:**

    ```sh
    git clone https://github.com/SeaRoll/mqs.git
    cd mqs
    ```

2.  **Start the services:**

    ```sh
    docker-compose up -d --build
    ```

This will start the MQS server and a PostgreSQL database. The server will be available at `http://localhost:8080`. Endpoints are available at `/docs` for the OpenAPI documentation.

## Configuration

MQS is configured using environment variables:

| Variable       | Description                                        | Default |
| -------------- | -------------------------------------------------- | ------- |
| `DATABASE_URL` | The connection string for the PostgreSQL database. |         |
| `PORT`         | The port for the HTTP server to listen on.         | `8080`  |
| `LISTEN_POOL`  | The number of concurrent message publishers.       | `10`    |

## Project Structure

```
.
├── cmd
│   └── main.go         # Main application entry point
├── migrations
│   └── 00001_init.sql  # Database migrations
├── docker-compose.yml  # Docker Compose configuration
├── Dockerfile          # Dockerfile for the MQS service
├── go.mod              # Go module definition
├── go.sum              # Go module checksums
├── mqs.go              # Core application logic and API handlers
└── openapi.yaml        # OpenAPI v3 specification
```

## Contributing

Contributions are welcome! Please feel free to submit a pull request.

## License

This project is open-source and available under the [MIT License](LICENSE).
