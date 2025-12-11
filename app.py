from flask import Flask, request, jsonify, send_from_directory
import time
import os
import json

app = Flask(__name__)

EVENT_LOG_FILE = "dfs_events.log"
BASE_DIR = os.path.dirname(__file__)

# In-memory cache of latest event per widget ID
latest_by_id = {}  # widget_id -> last event dict


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
        latest_by_id[wid] = event

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
        })
    return jsonify(result)


@app.route("/", methods=["GET"])
def dashboard():
    return send_from_directory(BASE_DIR, "dashboard.html")


@app.route("/logo.png", methods=["GET"])
def logo():
    return send_from_directory(BASE_DIR, "logo.png")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
