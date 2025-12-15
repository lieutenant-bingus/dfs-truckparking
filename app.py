from flask import Flask, request, jsonify, send_from_directory
import time
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

EVENT_LOG_FILE = "dfs_events.log"
BASE_DIR = os.path.dirname(__file__)

# Database connection string from environment variable
DATABASE_URL = os.environ.get("DATABASE_URL")

# In-memory cache of latest event per widget ID
latest_by_id = {}  # widget_id -> last event dict

# Track when each spot became occupied (for "occupied since" feature)
occupied_since = {}  # widget_id -> timestamp (ms) when spot became FULL


def get_db_connection():
    """Get a database connection"""
    if not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def init_db(conn=None):
    """Initialize database tables. Can use existing connection or create new one."""
    should_close = False
    if not conn:
        conn = get_db_connection()
        should_close = True
    
    if not conn:
        print("No database configured, using in-memory storage only")
        return False
    
    try:
        cur = conn.cursor()
        
        # Table for current widget state
        cur.execute("""
            CREATE TABLE IF NOT EXISTS widget_state (
                widget_id TEXT PRIMARY KEY,
                event_data JSONB NOT NULL,
                occupied_since BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Table for historical data (for charts)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS event_history (
                id SERIAL PRIMARY KEY,
                widget_name TEXT,
                object_count INTEGER,
                timestamp BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cur.close()
        if should_close:
            conn.close()
        print("Database tables initialized")
        return True
    except Exception as e:
        print(f"Database init error: {e}")
        if should_close and conn:
            conn.close()
        return False


def save_state(retry=True):
    """Save current state to database for persistence across restarts"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        for wid, event in latest_by_id.items():
            occ_since = occupied_since.get(wid)
            cur.execute("""
                INSERT INTO widget_state (widget_id, event_data, occupied_since)
                VALUES (%s, %s, %s)
                ON CONFLICT (widget_id) 
                DO UPDATE SET event_data = %s, occupied_since = %s, updated_at = CURRENT_TIMESTAMP
            """, (wid, json.dumps(event), occ_since, json.dumps(event), occ_since))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error saving state: {e}")
        if conn:
            conn.close()
        # If table doesn't exist, try to create it and retry once
        if retry and "does not exist" in str(e):
            print("Tables missing, attempting to create...")
            if init_db():
                save_state(retry=False)


def save_history(event, retry=True):
    """Save event to history table for charts"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        name = event.get("name", "")
        # Only save MEGA-ZONE events for occupancy tracking
        if "mega-zone" in name.lower():
            data = event.get("data", {})
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO event_history (widget_name, object_count, timestamp)
                VALUES (%s, %s, %s)
            """, (name, data.get("object_count", 0), event.get("_received_ts")))
            conn.commit()
            cur.close()
        conn.close()
    except Exception as e:
        print(f"Error saving history: {e}")
        if conn:
            conn.close()
        # If table doesn't exist, try to create it and retry once
        if retry and "does not exist" in str(e):
            print("Tables missing, attempting to create...")
            if init_db():
                save_history(event, retry=False)


def load_state():
    """Load state from database on startup"""
    global latest_by_id, occupied_since
    
    conn = get_db_connection()
    if not conn:
        print("No database connection, starting with empty state")
        return
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT widget_id, event_data, occupied_since FROM widget_state")
        rows = cur.fetchall()
        
        for row in rows:
            wid = row["widget_id"]
            latest_by_id[wid] = row["event_data"]
            if row["occupied_since"]:
                occupied_since[wid] = row["occupied_since"]
        
        cur.close()
        conn.close()
        print(f"Loaded state from database: {len(latest_by_id)} widgets, {len(occupied_since)} occupied spots")
    except Exception as e:
        print(f"Error loading state: {e}")
        if conn:
            conn.close()


# Initialize database and load state on startup
init_db()
load_state()


def is_occupied(event):
    """Check if a spot is occupied based on object_count"""
    data = event.get("data", {})
    count = data.get("object_count", 0)
    return count is not None and int(count) >= 1


@app.route("/dfs", methods=["POST"])
def dfs_webhook():
    body = request.get_data()

    try:
        event = request.get_json(force=True)
    except Exception:
        event = {"raw": body.decode("utf-8", errors="replace")}

    event["_received_ts"] = int(time.time() * 1000)

    wid = event.get("id")
    if wid is not None:
        # Check for OPEN -> FULL transition to track occupancy start time
        prev_event = latest_by_id.get(wid)
        was_occupied = is_occupied(prev_event) if prev_event else False
        now_occupied = is_occupied(event)
        
        if now_occupied and not was_occupied:
            # Just became occupied - record the time
            occupied_since[wid] = event["_received_ts"]
        elif not now_occupied and was_occupied:
            # Just became open - clear the occupied time
            occupied_since.pop(wid, None)
        
        latest_by_id[wid] = event
        
        # Persist state to database
        save_state()
        
        # Save to history for charts
        save_history(event)

    # Also log to file as backup
    try:
        with open(EVENT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    except OSError:
        pass

    print("\n=== New DFS POST ===")
    print(event)

    return jsonify({"status": "ok"}), 200


@app.route("/api/widgets", methods=["GET"])
def api_widgets():
    result = []
    for wid, ev in latest_by_id.items():
        data = ev.get("data", {})
        result.append({
            "id": wid,
            "name": ev.get("name"),
            "operator_attribute": ev.get("operator_attribute"),
            "value": data.get("value"),
            "object_count": data.get("object_count"),
            "data_start_timestamp": ev.get("data_start_timestamp"),
            "data_end_timestamp": ev.get("data_end_timestamp"),
            "received_ts": ev.get("_received_ts"),
            "occupied_since": occupied_since.get(wid),  # When spot became FULL
        })
    return jsonify(result)


@app.route("/api/history", methods=["GET"])
def api_history():
    """Return historical occupancy data from the database"""
    history = []
    
    # Try database first
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT widget_name, object_count, timestamp 
                FROM event_history 
                ORDER BY timestamp DESC 
                LIMIT 100
            """)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            # Reverse to get chronological order
            for row in reversed(rows):
                history.append({
                    "timestamp": row["timestamp"],
                    "occupied": row["object_count"],
                    "name": row["widget_name"]
                })
            
            return jsonify(history)
        except Exception as e:
            print(f"Error fetching history: {e}")
            if conn:
                conn.close()
    
    # Fallback to file if no database
    try:
        if os.path.exists(EVENT_LOG_FILE):
            with open(EVENT_LOG_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        event = json.loads(line.strip())
                        name = event.get("name", "")
                        if "mega-zone" in name.lower():
                            data = event.get("data", {})
                            history.append({
                                "timestamp": event.get("_received_ts"),
                                "occupied": data.get("object_count", 0),
                                "name": name
                            })
                    except json.JSONDecodeError:
                        continue
    except OSError:
        pass
    
    return jsonify(history[-100:])


@app.route("/", methods=["GET"])
def dashboard():
    return send_from_directory(BASE_DIR, "dashboard.html")


@app.route("/logo.png", methods=["GET"])
def logo():
    return send_from_directory(BASE_DIR, "logo.png")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
