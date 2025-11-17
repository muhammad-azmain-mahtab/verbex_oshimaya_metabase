from fastapi import FastAPI, Request, HTTPException, Header
from pydantic import BaseModel, Field
from typing import Optional, Any, Dict
from datetime import datetime, timezone, timedelta
import logging
import httpx
import os
import psycopg2


app = FastAPI(
    title="Verbex Webhook Receiver",
    description="Webhook endpoint for receiving Verbex events",
    version="1.0.0"
)
logging.basicConfig(level=logging.INFO)


# Get from environment
VERBEX_API_KEY = os.getenv('VERBEX_API_KEY')
VERBEX_API_URL = "https://api.verbex.ai"
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT'))
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')

# Log database configuration on startup
logging.info(f"Database configuration loaded: host={DB_HOST}, port={DB_PORT}, database={DB_NAME}, user={DB_USER}, schema={DB_SCHEMA}")

# Japanese timezone (JST = UTC+9)
JST = timezone(timedelta(hours=9))

processed_trace_ids = set()


class WebhookPayload(BaseModel):
    call_id: str = Field(..., description="Call ID")
    agent_id: str = Field(..., description="AI agent ID")
    status: str = Field(..., description="Status")
    reason: Optional[str] = Field(None, description="Reason (PCA events only)")
    timestamp: Optional[int] = Field(None, description="UNIX timestamp in milliseconds (PCA events only)")
    
    class Config:
        extra = "allow"  # Allow unknown fields


class WebhookBody(BaseModel):
    organizationId: str = Field(..., description="Organization ID")
    traceId: str = Field(..., description="Webhook unique identifier (used for deduplication)")
    eventName: str = Field(..., description="Event type (CallHandler.CallStarted, CallHandler.CallEnded, callAnalysis.pcaCompleted)")
    timestamp: float = Field(..., description="UNIX timestamp in seconds")
    payload: WebhookPayload
    
    class Config:
        extra = "allow"  # Allow unknown fields


@app.post('/webhooks/verbex')
async def verbex_webhook(
    body: WebhookBody,
    x_webhook_event: Optional[str] = Header(None, description="Event name (e.g., CallHandler.CallEnded)"),
    x_webhook_traceid: Optional[str] = Header(None, description="Webhook unique identifier"),
    x_webhook_timestamp: Optional[str] = Header(None, description="Timestamp in ISO 8601 format"),
    request: Request = None
):
    """
    Webhook endpoint for receiving Verbex events.
    
    **Endpoint Settings:**
    - HTTP Method: POST
    - Protocol: HTTPS (required)
    - Content-Type: application/json
    
    **Custom Headers (Required):**
    - X-Webhook-Event: Event name (e.g., CallHandler.CallEnded)
    - X-Webhook-Traceid: Webhook unique identifier (same as traceId in body)
    - X-Webhook-Timestamp: Timestamp in ISO 8601 format
    
    **Supported Events:**
    - CallHandler.CallStarted
    - CallHandler.CallEnded
    - callAnalysis.pcaCompleted
    """
    try:
        # Validate required headers
        if not x_webhook_event or not x_webhook_traceid or not x_webhook_timestamp:
            raise HTTPException(
                status_code=400,
                detail='Missing required headers: X-Webhook-Event, X-Webhook-Traceid, X-Webhook-Timestamp'
            )
        
        # Validate traceId consistency between header and body
        if x_webhook_traceid != body.traceId:
            raise HTTPException(
                status_code=400,
                detail='Mismatch: X-Webhook-Traceid header does not match traceId in body'
            )
        
        # Deduplication
        if x_webhook_traceid in processed_trace_ids:
            logging.info(f'Duplicate webhook: {x_webhook_traceid}')
            return {'message': 'Already processed', 'traceId': x_webhook_traceid}
        
        # Store complete payload
        webhook_data = {
            'headers': {
                'eventName': x_webhook_event,
                'traceId': x_webhook_traceid,
                'timestamp': x_webhook_timestamp,
                'userAgent': request.headers.get('user-agent', 'unknown') if request else 'unknown'
            },
            'body': body.dict(),
            'receivedAt': datetime.utcnow().isoformat()
        }
        
        # Save and process
        await save_webhook_to_database(webhook_data)
        processed_trace_ids.add(x_webhook_traceid)
        await process_webhook_event(webhook_data)
        
        return {
            'success': True,
            'traceId': x_webhook_traceid,
            'eventName': x_webhook_event,
            'message': 'Webhook processed successfully'
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f'Error processing webhook: {str(e)}')
        raise HTTPException(status_code=500, detail='Internal server error')


async def process_webhook_event(webhook_data: Dict[str, Any]):
    """
    Process webhook events based on event type.
    
    Supported Events:
    
    1. **CallHandler.CallStarted**
       - Triggered when a call begins
       - Payload: call_id, agent_id, status
    
    2. **CallHandler.CallEnded**
       - Triggered when a call ends
       - Payload: call_id, agent_id, status
    
    3. **callAnalysis.pcaCompleted**
       - Triggered when PCA (Post Call Analysis) completes
       - Payload: call_id, agent_id, status, reason, timestamp (milliseconds)
    """
    event_name = webhook_data['headers']['eventName']
    payload = webhook_data['body']['payload']
    call_id = payload.get('call_id')
    
    if event_name == 'CallHandler.CallStarted':
        await handle_call_started(payload, call_id)
    elif event_name == 'CallHandler.CallEnded':
        await handle_call_ended(payload, call_id)
    elif event_name == 'callAnalysis.pcaCompleted':
        await handle_pca_completed(payload, call_id)
    else:
        logging.warning(f'Unknown event: {event_name}')


async def handle_call_started(payload: Dict[str, Any], call_id: str):
    """Handle CallHandler.CallStarted event."""
    logging.info(f"Call started: {call_id} (agent: {payload.get('agent_id')})")


async def handle_call_ended(payload: Dict[str, Any], call_id: str):
    """Handle CallHandler.CallEnded event."""
    agent_id = payload.get('agent_id')
    logging.info(f"Call ended: {call_id} (agent: {agent_id})")
    # Fetch call data from Verbex API
    api_response = await fetch_call_data(call_id, agent_id)
    if api_response:
        logging.info(f"Call data retrieved from API")
        # Parse the response
        parsed_data = parse_verbex_response(api_response)
        # Save to database
        saved = await save_order_to_database(parsed_data)
        if saved:
            logging.info(f"Order data from call {call_id} saved successfully")
        else:
            logging.warning(f"Failed to save order data from call {call_id}")


async def handle_pca_completed(payload: Dict[str, Any], call_id: str):
    """Handle callAnalysis.pcaCompleted event."""
    agent_id = payload.get('agent_id')
    logging.info(f"PCA completed: {call_id} (agent: {agent_id}, reason: {payload.get('reason')})")
    # Fetch call data from Verbex API
    api_response = await fetch_call_data(call_id, agent_id)
    if api_response:
        logging.info(f"Call data retrieved from API")
        # Parse the response
        parsed_data = parse_verbex_response(api_response)
        # Save to database
        saved = await save_order_to_database(parsed_data)
        if saved:
            logging.info(f"Order data from call {call_id} saved successfully")
        else:
            logging.warning(f"Failed to save order data from call {call_id}")


async def get_next_order_number(conn) -> str:
    """
    Get the next sequential order number.
    Format: 108YYYYMMDD + 11-digit sequential counter (00000000001, 00000000002, etc.)
    Counter increments globally regardless of date, starting from 1.
    Uses Japanese time (JST, UTC+9) for the date portion.
    
    Args:
        conn: Database connection
        
    Returns:
        Next sequential order number
    """
    cursor = conn.cursor()
    today = datetime.now(JST).strftime('%Y%m%d')
    
    try:
        # Get the highest sequence number from all orders (global counter)
        query = """
            SELECT MAX(CAST(SUBSTRING(order_number, 12) AS BIGINT)) 
            FROM public.orders 
            WHERE order_number LIKE '108%'
        """
        cursor.execute(query)
        result = cursor.fetchone()
        last_seq = result[0] if result[0] is not None else 0
        
        next_seq = last_seq + 1
        seq_str = str(next_seq).zfill(11)  # Pad to 11 digits
        order_number = f"108{today}{seq_str}"
        
        logging.info(f"[get_next_order_number] Generated order_number: {order_number} (sequence: {next_seq}, JST date: {today})")
        return order_number
        
    except Exception as e:
        logging.error(f"[get_next_order_number] Error getting next order number: {str(e)}")
        raise
    finally:
        cursor.close()


async def fetch_call_data(call_id: str, agent_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch post-call analysis data from Verbex API.
    
    Args:
        call_id: The call ID from the webhook payload
        agent_id: The AI agent ID from the webhook payload
        
    Returns:
        Call data from the API or None if request fails
    """
    if not VERBEX_API_KEY:
        logging.error('VERBEX_API_KEY environment variable not set')
        return None
    
    try:
        async with httpx.AsyncClient() as client:
            url = f"{VERBEX_API_URL}/v2/ai-agents/{agent_id}/postcall-analysis/results/{call_id}"
            headers = {
                "Authorization": f"Bearer {VERBEX_API_KEY}",
                "accept": "*/*"
            }
            
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            
            api_response = response.json()
            logging.info(f"Successfully fetched call data for {call_id}")
            return api_response
            
    except httpx.TimeoutException:
        logging.error(f"Timeout fetching call data for {call_id}")
        return None
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error fetching call data for {call_id}: {e.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logging.error(f"Error fetching call data for {call_id}: {str(e)}")
        return None


def parse_verbex_response(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse Verbex post-call analysis API response and extract order data.
    Preserves original PCA field names in pca_data dictionary.
    Extracts product quantities from 'items' field and calculates total.
    
    Args:
        api_response: Response from Verbex API with structure {data: {items: [...]}}
        Example items field: "りんご0,みかん3,りんご14"
        
    Returns:
        Dictionary with fixed fields and pca_data containing all PCA items with original names,
        plus calculated total from product quantities
    """
    extracted_data = {
        'call_id': None,
        'agent_id': None,
        'total': 0,
        'pca_data': {},  # Store all PCA items with original field names
        'order_items': []  # Store extracted product items with name and quantity
    }
    
    try:
        data = api_response.get('data', {})
        extracted_data['call_id'] = data.get('call_id')
        extracted_data['agent_id'] = data.get('ai_agent_id')
        
        items = data.get('items', [])
        
        # Store each PCA item with its original name from the API
        for item in items:
            item_name = item.get('name')
            item_result = item.get('result')
            
            if item_name and item_result is not None:  # Allow empty strings
                extracted_data['pca_data'][item_name] = item_result
                logging.info(f"Extracted PCA field '{item_name}': {item_result}")
        
        # Extract product quantities from 'items' field if it exists
        # Example: "りんご0,みかん3,りんご14" -> extract 0, 3, 14 and sum
        items_field = extracted_data['pca_data'].get('items')
        if items_field and isinstance(items_field, str):
            import re
            total_quantity = 0
            products = items_field.split(',')
            
            logging.info(f"[parse] Processing items field: {items_field}")
            logging.info(f"[parse] Found {len(products)} product entries")
            
            for product_entry in products:
                product_entry = product_entry.strip()
                if not product_entry:
                    continue
                
                # Extract the trailing number from product entry
                # Example: "りんご0" -> 0, "みかん3" -> 3
                match = re.search(r'(\d+)$', product_entry)
                
                if match:
                    quantity = int(match.group(1))
                    product_name = product_entry[:match.start()].strip()
                    total_quantity += quantity
                    # Store product item for order_items table
                    extracted_data['order_items'].append({
                        'product_name': product_name,
                        'quantity': quantity
                    })
                    logging.info(f"[parse] Extracted product: '{product_name}' with quantity: {quantity}")
                else:
                    logging.warning(f"[parse] Could not extract quantity from product entry: {product_entry}")
            
            # Calculate total: sum of all quantities * 1990
            extracted_data['total'] = total_quantity * 1990
            logging.info(f"[parse] Total quantity sum: {total_quantity}, Calculated total: {total_quantity} * 1990 = {extracted_data['total']}")
        else:
            # Fallback: check if product_name_quantity exists (old behavior)
            product_qty = extracted_data['pca_data'].get('product_name_quantity')
            if product_qty:
                try:
                    quantity = float(product_qty)
                    extracted_data['total'] = 1990 * quantity
                    logging.info(f"[parse] Using product_name_quantity field: 1990 * {quantity} = {extracted_data['total']}")
                except (ValueError, TypeError) as e:
                    logging.error(f"[parse] Error converting product_quantity to float: {str(e)}")
                    extracted_data['total'] = 0
            else:
                logging.info(f"[parse] No 'items' or 'product_name_quantity' field found, total set to 0")
                extracted_data['total'] = 0
        
        return extracted_data
        
    except Exception as e:
        logging.error(f"[parse] Error parsing Verbex response: {str(e)}")
        logging.exception(f"[parse] Exception traceback:")
        return extracted_data


async def save_order_to_database(parsed_data: Dict[str, Any]) -> bool:
    """
    Save order data from parsed Verbex API response to PostgreSQL orders table.
    Dynamically creates columns for each PCA field and stores data in individual columns.
    
    Args:
        parsed_data: Parsed data from Verbex API with structure:
            {
                'call_id': str,
                'agent_id': str,
                'total': float,
                'pca_data': {
                    'prefecture_of_the_orderer': str,
                    'order_city': str,
                    'orderer_last_name': str,
                    ... (all other PCA fields with original names)
                }
            }
        
    Returns:
        True if saved successfully, False otherwise
    """
    if not DB_PASSWORD:
        logging.error('DB_PASSWORD environment variable not set')
        return False
    
    logging.info(f"[save_order] Starting database save with parsed_data: {parsed_data}")
    logging.debug(f"[save_order] DB connection params: host={DB_HOST}, port={DB_PORT}, database={DB_NAME}, user={DB_USER}, schema={DB_SCHEMA}")
    
    conn = None
    cursor = None
    
    try:
        logging.info(f"[save_order] Attempting to connect to database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info(f"[save_order] Database connection established successfully")
        
        cursor = conn.cursor()
        logging.debug(f"[save_order] Cursor created")
        
        # Get PCA data keys
        pca_data = parsed_data.get('pca_data', {})
        pca_keys = list(pca_data.keys())
        logging.info(f"[save_order] Found {len(pca_keys)} PCA fields: {pca_keys}")
        
        # Dynamically create columns for each PCA field if they don't exist
        logging.info(f"[save_order] Creating columns for PCA fields if needed")
        for field_name in pca_keys:
            # Sanitize column name: replace hyphens and spaces with underscores, convert to lowercase
            col_name = field_name.replace('-', '_').replace(' ', '_').lower()
            alter_query = f'ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS "{col_name}" TEXT;'
            logging.debug(f"[save_order] Executing: {alter_query}")
            try:
                cursor.execute(alter_query)
            except psycopg2.Error as e:
                logging.warning(f"[save_order] Warning creating column {col_name}: {str(e)}")
        
        conn.commit()
        logging.info(f"[save_order] Column creation committed")
        
        # Generate sequential order_number: "108" + YYYYMMDD + 11-digit counter
        order_number = await get_next_order_number(conn)
        logging.info(f"[save_order] Generated order_number: {order_number}")
        
        # Build dynamic INSERT query with all PCA columns
        # Fixed columns: order_number, order_date_time, total, call_id, agent_id
        # Dynamic columns: one for each PCA field
        fixed_cols = ['order_number', 'order_date_time', 'total', 'call_id', 'agent_id']
        dynamic_cols = [field_name.replace('-', '_').replace(' ', '_').lower() for field_name in pca_keys]
        all_cols = fixed_cols + dynamic_cols 
        
        # Build placeholders
        placeholders = ', '.join(['%s'] * len(all_cols))
        cols_str = ', '.join([f'"{col}"' for col in all_cols])
        
        orders_insert_query = f"""
            INSERT INTO public.orders ({cols_str})
            VALUES ({placeholders})
            ON CONFLICT (order_number) DO UPDATE SET
                total = EXCLUDED.total,
                call_id = EXCLUDED.call_id,
                agent_id = EXCLUDED.agent_id,
                {', '.join([f'"{col}" = EXCLUDED."{col}"' for col in dynamic_cols])},
                updated_at = CURRENT_TIMESTAMP
        """
        logging.debug(f"[save_order] SQL query prepared with {len(all_cols)} columns")
        
        # Build values tuple in same order as columns
        orders_values = [
            order_number,
            datetime.now(JST).replace(tzinfo=None),  # Convert to naive datetime (JST without tzinfo)
            parsed_data.get('total', 0),
            parsed_data.get('call_id'),
            parsed_data.get('agent_id')
        ]
        
        # Add PCA field values in order
        for field_name in pca_keys:
            orders_values.append(pca_data.get(field_name))
        
        orders_values = tuple(orders_values)
        
        logging.info(f"[save_order] Executing INSERT with values: order_number={order_number}, total={orders_values[2]}, call_id={orders_values[3]}, agent_id={orders_values[4]}, pca_data_keys={pca_keys}")
        
        cursor.execute(orders_insert_query, orders_values)
        logging.info(f"[save_order] Query executed successfully. Row count: {cursor.rowcount}")
        
        # Log saved PCA fields
        pca_fields_count = len(pca_data)
        logging.info(f"[save_order] Order {order_number} saved with {pca_fields_count} PCA fields:")
        for field_name, field_value in pca_data.items():
            logging.info(f"[save_order]   - {field_name}: {field_value}")
        
        logging.info(f"[save_order] Committing transaction for order {order_number}")
        conn.commit()
        logging.info(f"[save_order] Order {order_number} committed to database successfully")
        
        # Save order items to order_items table
        order_items = parsed_data.get('order_items', [])
        if order_items:
            logging.info(f"[save_order] Saving {len(order_items)} items to order_items table")
            for item in order_items:
                product_name = item.get('product_name')
                quantity = item.get('quantity')
                
                item_insert_query = """
                    INSERT INTO public.order_items (order_number, product_name, quantity)
                    VALUES (%s, %s, %s)
                """
                try:
                    cursor.execute(item_insert_query, (order_number, product_name, quantity))
                    logging.info(f"[save_order] Saved item: {product_name} (qty: {quantity})")
                except psycopg2.Error as e:
                    logging.error(f"[save_order] Error saving order item {product_name}: {str(e)}")
            
            conn.commit()
            logging.info(f"[save_order] All order items committed to database")
        
        return True
        
    except psycopg2.IntegrityError as e:
        logging.warning(f"[save_order] Integrity error saving order: {str(e)}")
        logging.warning(f"[save_order] Integrity error details: {e.diag if hasattr(e, 'diag') else 'N/A'}")
        if conn:
            conn.rollback()
            logging.info(f"[save_order] Transaction rolled back due to integrity error")
        return False
    except psycopg2.Error as e:
        logging.error(f"[save_order] Database error saving order: {str(e)}")
        logging.error(f"[save_order] Error code: {e.pgcode if hasattr(e, 'pgcode') else 'N/A'}")
        logging.error(f"[save_order] Error diagnostics: {e.diag if hasattr(e, 'diag') else 'N/A'}")
        if conn:
            try:
                conn.rollback()
                logging.info(f"[save_order] Transaction rolled back due to database error")
            except Exception as rollback_err:
                logging.error(f"[save_order] Error during rollback: {str(rollback_err)}")
        return False
    except Exception as e:
        logging.error(f"[save_order] Unexpected error saving order to database: {str(e)}")
        logging.exception(f"[save_order] Exception traceback:")
        if conn:
            try:
                conn.rollback()
                logging.info(f"[save_order] Transaction rolled back due to unexpected error")
            except:
                pass
        return False
    finally:
        if cursor:
            cursor.close()
            logging.debug(f"[save_order] Cursor closed")
        if conn:
            conn.close()
            logging.info(f"[save_order] Database connection closed")


async def save_webhook_to_database(webhook_data: Dict[str, Any]):
    """
    Save raw webhook data to database for auditing.
    """
    # Implement webhook audit table save if needed
    logging.info('Webhook received and validated')


@app.get('/health')
async def health_check():
    """Health check endpoint."""
    return {'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()}


@app.get('/')
async def root():
    """Root endpoint with API information."""
    return {
        'service': 'Verbex Webhook Receiver',
        'version': '1.0.0',
        'endpoints': {
            'webhook': '/webhooks/verbex',
            'health': '/health',
            'docs': '/docs'
        }
    }
