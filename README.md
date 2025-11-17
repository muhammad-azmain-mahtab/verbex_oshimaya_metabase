# Verbex Webhook Receiver for Metabase

A FastAPI application that receives webhook events from Verbex AI and stores order data in PostgreSQL with dynamic column creation for PCA (Post Call Analysis) fields.

## Features

- **Webhook Processing:** Receives and processes Verbex webhook events (CallHandler.CallStarted, CallHandler.CallEnded, callAnalysis.pcaCompleted)
- **API Integration:** Fetches post-call analysis data from Verbex API
- **Dynamic Schema:** Automatically creates database columns for each PCA field
- **Order Items Tracking:** Stores individual products and quantities in a separate `order_items` table
- **Sequential Order Numbers:** Generates unique order numbers with format `108YYYYMMDD` + 11-digit sequential counter
- **Japanese Time:** Uses Japan Standard Time (JST, UTC+9) for all timestamps
- **Deduplication:** Prevents duplicate webhook processing using traceId

## Prerequisites

- Docker & Docker Compose
- PostgreSQL database (with `oshimaya-metabase` database created)
- Verbex API key

## Setup

### 1. Environment Variables

Create a `.env` file in the project root with the following variables:

```env
VERBEX_API_KEY=<your-verbex-api-key>
DB_HOST=<database-host>
DB_PORT=<database-port>
DB_NAME=oshimaya-metabase
DB_USER=<database-user>
DB_PASSWORD=<database-password>
DB_SCHEMA=oshimaya-metabase
```

### 2. Create Required Tables

Before running the application, create the `orders` and `order_items` tables:

```sql
-- Create orders table
CREATE TABLE IF NOT EXISTS public.orders (
    id SERIAL PRIMARY KEY,
    order_number VARCHAR(255) UNIQUE NOT NULL,
    order_date_time TIMESTAMP NOT NULL,
    total NUMERIC(10, 2) NOT NULL DEFAULT 0,
    call_id VARCHAR(255),
    agent_id VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order_items table for individual products
CREATE TABLE IF NOT EXISTS public.order_items (
    id SERIAL PRIMARY KEY,
    order_number VARCHAR(255) NOT NULL,
    product_name TEXT NOT NULL,
    quantity INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_number) REFERENCES public.orders(order_number) ON DELETE CASCADE
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_orders_order_number ON public.orders(order_number);
CREATE INDEX IF NOT EXISTS idx_orders_call_id ON public.orders(call_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_number ON public.order_items(order_number);
CREATE INDEX IF NOT EXISTS idx_order_items_product_name ON public.order_items(product_name);
```

## Running the Application

### With Docker Compose

```bash
docker compose up --force-recreate --build
```

## API Endpoints

### Health Check
```bash
GET http://localhost:8000/health
```

### Webhook Receiver
```bash
POST http://localhost:8000/webhooks/verbex
```

**Required Headers:**
- `X-Webhook-Event`: Event name (e.g., `CallHandler.CallEnded`)
- `X-Webhook-Traceid`: Webhook unique identifier
- `X-Webhook-Timestamp`: ISO 8601 timestamp

**Example Request:**
```bash
curl -X POST http://localhost:8000/webhooks/verbex \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Event: CallHandler.CallEnded" \
  -H "X-Webhook-Traceid: abc123" \
  -H "X-Webhook-Timestamp: 2025-11-17T20:30:00Z" \
  -d '{
    "organizationId": "org123",
    "traceId": "abc123",
    "eventName": "CallHandler.CallEnded",
    "timestamp": 1734444600,
    "payload": {
      "call_id": "call123",
      "agent_id": "agent456",
      "status": "completed"
    }
  }'
```

### API Documentation
```
http://localhost:8000/docs
```

Interactive Swagger UI for testing endpoints.

## How It Works

1. **Webhook Reception:** Application receives webhook events from Verbex
2. **Deduplication:** Checks if traceId has been processed before
3. **API Fetch:** Retrieves post-call analysis data from Verbex API
4. **Data Parsing:** Extracts PCA fields and product items
5. **Order Number Generation:** Creates sequential order number with current date (JST) and global counter
6. **Total Calculation:** Extracts product quantities from items field (e.g., "りんご3,みかん5") and multiplies sum by 1990
7. **Dynamic Column Creation:** Automatically creates database columns for new PCA fields
8. **Database Storage:** Saves order data to `orders` table with individual products in `order_items` table

## Order Number Format

Format: `108YYYYMMDD` + 11-digit sequential counter

Examples:
- First order on Nov 17, 2025: `10820251117**00000000001**`
- Second order on Nov 17, 2025: `10820251117**00000000002**`
- First order on Nov 18, 2025: `10820251118**00000000003**` (counter continues globally)

## Database Schema Notes

- **Dynamic Columns:** PCA fields from the Verbex API are automatically converted to database columns
  - Column names are sanitized: spaces/hyphens → underscores, lowercase
  - Example: `prefecture_of_the_orderer`, `orderer_last_name`, `items`, etc.
- **Items Parsing:** The `items` field (e.g., "りんご0,みかん3") is parsed into product name and quantity
- **Order Items Table:** Each product is stored separately in `order_items` with the same `order_number`
- **Timestamps:** All timestamps use Japanese time (UTC+9) and are stored as naive timestamps

## Troubleshooting

### Table Not Found Error
Ensure the `orders` and `order_items` tables exist in your PostgreSQL database with the schema above.

### Wrong Timestamp
Check that the database is using the correct timezone. The application converts to JST before storing.

### Duplicate Webhook Processing
The application stores processed traceIds in memory. For persistent deduplication across container restarts, consider adding a `webhooks_log` table.

### Dynamic Column Issues
If new PCA fields aren't being created, check database permissions for the user. The user needs `ALTER TABLE` permissions.

## Environment Details

- **Python Version:** 3.11
- **Framework:** FastAPI 0.104.1
- **Database:** PostgreSQL
- **Timezone:** Japanese Standard Time (UTC+9)
- **Port:** 8000 (default)

## License

Internal use only
