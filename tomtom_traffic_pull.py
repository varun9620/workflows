"""
TomTom Traffic Flow data collector
-----------------------------------
Pulls current speed, free-flow speed, and travel times for a list of
lat/lon points, and appends the results to a CSV file with a timestamp.

Uses a thread pool to send multiple requests concurrently, which is
much faster than calling the API one point at a time (2000 points
sequentially can take 10-15+ minutes; with 10 concurrent workers this
typically drops to 1-2 minutes, network permitting).

Run this on a schedule (every 15-30 min) using cron (Linux/Mac) or
Task Scheduler (Windows) to build up a time series for your study.

SETUP:
1. Install the requests library:  pip install requests
2. Paste your TomTom API key below.
3. Fill in POINTS with your study locations (lat, lon).
4. Run:  python tomtom_traffic_pull.py
"""

import requests
import csv
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---- CONFIG ----------------------------------------------------------
# Reads keys from environment variables (set as GitHub Secrets, never
# hardcoded here). Supports one or two keys -- if a second key is set,
# the point list is split roughly in half, each half queried with its
# own key, which roughly doubles your effective daily call budget since
# each TomTom key has its own separate quota.
API_KEYS = [
    k for k in [
        os.environ.get("TOMTOM_API_KEY_1"),
        os.environ.get("TOMTOM_API_KEY_2"),
    ] if k
]
if not API_KEYS:
    # Local-only fallback -- replace with your key ONLY for a quick local
    # test, and never commit a real key here.
    API_KEYS = [os.environ.get("TOMTOM_API_KEY", "PASTE_YOUR_TOMTOM_API_KEY_HERE")]

# Points are loaded from a CSV file (output of extract_points_from_road_network.py
# or select_temporal_sample.py). The CSV must have at least: label, lat, lon
POINTS_CSV = "temporal_sample_points.csv"

OUTPUT_FILE = "tomtom_traffic_log.csv"

# Cap total points queried per run (across all keys combined). None = no limit.
MAX_POINTS_PER_RUN = None
BASE_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

# Number of concurrent requests PER KEY. TomTom doesn't publish a strict
# requests-per-second cap on the free tier, but 10-15 is a safe,
# well-behaved level that won't trigger throttling/errors.
MAX_WORKERS = 10

# ---- SCRIPT ------------------------------------------------------------

def fetch_point(label, lat, lon, api_key):
    """Call the TomTom Flow Segment Data API for one point using a given key."""
    params = {"point": f"{lat},{lon}", "key": api_key}
    try:
        resp = requests.get(BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("flowSegmentData", {})

        current_speed = data.get("currentSpeed")
        free_flow_speed = data.get("freeFlowSpeed")
        current_tt = data.get("currentTravelTime")
        free_flow_tt = data.get("freeFlowTravelTime")

        # --- Derived congestion metrics ---
        # congestion_ratio: 1.0 = free flow, 0.0 = fully jammed (speed near zero)
        # congestion_pct: % speed reduction vs free-flow (0% = no congestion,
        #                 higher % = more congested)
        # delay_s: extra travel time caused by congestion, in seconds
        congestion_ratio = None
        congestion_pct = None
        delay_s = None
        if current_speed is not None and free_flow_speed and free_flow_speed > 0:
            congestion_ratio = round(current_speed / free_flow_speed, 3)
            congestion_pct = round((1 - congestion_ratio) * 100, 1)
        if current_tt is not None and free_flow_tt is not None:
            delay_s = current_tt - free_flow_tt

        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "segment_label": label,
            "lat": lat,
            "lon": lon,
            "current_travel_time_s": current_tt,
            "free_flow_travel_time_s": free_flow_tt,
            "delay_s": delay_s,
            "current_speed_kmh": current_speed,
            "free_flow_speed_kmh": free_flow_speed,
            "congestion_ratio": congestion_ratio,
            "congestion_pct": congestion_pct,
            "confidence": data.get("confidence"),
            "road_class": data.get("frc"),
        }
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {label}: {e}")
        return None


def load_points_from_csv(path):
    """Read (label, lat, lon) tuples from a CSV file."""
    points = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            points.append((row["label"], float(row["lat"]), float(row["lon"])))
    return points


def split_points_across_keys(points, num_keys):
    """Split a point list into num_keys roughly-equal chunks."""
    chunk_size = -(-len(points) // num_keys)  # ceil division
    return [points[i:i + chunk_size] for i in range(0, len(points), chunk_size)]


def main():
    if not os.path.isfile(POINTS_CSV):
        print(f"ERROR: points file '{POINTS_CSV}' not found. "
              f"Run extract_points_from_road_network.py / "
              f"select_temporal_sample.py first, or point "
              f"POINTS_CSV at your own points CSV.")
        return

    points = load_points_from_csv(POINTS_CSV)
    if MAX_POINTS_PER_RUN:
        points = points[:MAX_POINTS_PER_RUN]

    chunks = split_points_across_keys(points, len(API_KEYS))
    print(f"Loaded {len(points)} points from {POINTS_CSV}. "
          f"Using {len(API_KEYS)} API key(s), "
          f"split into chunks of ~{len(chunks[0]) if chunks else 0} points each.")

    rows = []
    done = 0
    total = len(points)

    for key_index, (chunk, api_key) in enumerate(zip(chunks, API_KEYS), start=1):
        print(f"\n--- Key {key_index}: fetching {len(chunk)} points ---")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_point, label, lat, lon, api_key): label
                for label, lat, lon in chunk
            }
            for future in as_completed(futures):
                result = future.result()
                done += 1
                if result:
                    rows.append(result)
                    if done % 50 == 0 or done == total:
                        print(f"[{done}/{total}] {result['segment_label']}: "
                              f"speed={result['current_speed_kmh']} km/h, "
                              f"congestion={result['congestion_pct']}%")

    if not rows:
        print("No data collected this run.")
        return

    file_exists = os.path.isfile(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    print(f"\nSaved {len(rows)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
