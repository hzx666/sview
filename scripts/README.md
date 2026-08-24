# Bench / ops scripts (run from repo root)

python scripts/bench_online.py --base-url http://139.224.211.114:8080 --h264 "oss://..." --h265 "oss://..."
python scripts/bench_online_rounds.py --base-url http://139.224.211.114:8080 --rounds 10 --h264 "..." --h265 "..."
python scripts/bench_cpu_transcode.py   # local offline CPU bench (needs mcap in repo root)

Scripts add the repo root to sys.path so they can import bench_types / sview_worker.
