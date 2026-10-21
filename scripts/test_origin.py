#!/usr/bin/env python3
"""从 T 秒起转的 origin 辅助函数与队列 JSON。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sview.jobs.job_status import (
    REDIS_QUEUE_KEY,
    cancel_encode_request,
    complete_segment_requests,
    create_encode_request,
    enqueue_segment_window,
    enqueue_sparse_job,
    get_encode_request,
    job_id_from_source,
    parse_queue_item,
    sparse_cache_job_id,
)
from sview.jobs.origin import (
    cut_log_time_ns,
    estimate_remaining_frames,
    origin_job_id,
    parse_start_sec,
)


def test_parse_and_job_id():
    assert parse_start_sec(None) == 0
    assert parse_start_sec("-1") == 0
    assert parse_start_sec("3000") == 3000
    assert origin_job_id("bag", 0) == "bag"
    assert origin_job_id("bag", 3000) == "bag_o3000"
    assert origin_job_id("bag", 2999.6) == "bag_o3000"
    stem = job_id_from_source("oss://b/teleoperation/Unitree/foo.mcap")
    assert origin_job_id(stem, 50) == f"{stem}_o50"


def test_remaining_frames():
    # 1 小时包，跳到 50 分钟 → 大约还剩 10 分钟
    t0 = 1_000_000_000
    t1 = t0 + 3600 * 1_000_000_000
    n = estimate_remaining_frames(108_000, 3000, t0, t1, fps=30)
    assert n is not None
    assert 17_000 < n < 19_500
    assert estimate_remaining_frames(100, 0, t0, t1) == 100
    assert estimate_remaining_frames(900, 10, None, None, fps=30) == 600


def test_queue_json_origin():
    item = parse_queue_item(
        '{"v":1,"kind":"job","source":"oss://b/a.mcap","job_id":"a_o3000","origin_sec":3000}'
    )
    assert item["kind"] == "job"
    assert item["job_id"] == "a_o3000"
    assert item["origin_sec"] == 3000
    plain = parse_queue_item("oss://b/a.mcap")
    assert plain["kind"] == "job"
    assert plain["origin_sec"] == 0
    bench = parse_queue_item('{"v":1,"kind":"bench","source":"oss://b/a.mcap","bench_type":3}')
    assert bench["kind"] == "bench"


def test_sparse_queue_item_and_priority_dedupe():
    class FakeRedis:
        def __init__(self):
            self.hashes = {"sview:job:j": {"frame_count": "100"}}
            self.values = {}
            self.lists = {}
            self.sets = {}

        def hgetall(self, key):
            return self.hashes.get(key, {})

        def hincrby(self, key, field, amount):
            row = self.hashes.setdefault(key, {})
            value = int(row.get(field, 0)) + amount
            row[field] = str(value)
            return value

        def hset(self, key, *args, mapping=None):
            row = self.hashes.setdefault(key, {})
            if mapping is not None:
                row.update(mapping)
            elif len(args) == 2:
                row[args[0]] = args[1]

        def set(self, key, value, nx=False, ex=None):
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True

        def lpush(self, key, value):
            self.lists.setdefault(key, []).insert(0, value)

        def expire(self, key, ttl):
            return True

        def smembers(self, key):
            return self.sets.get(key, set())

        def sadd(self, key, value):
            self.sets.setdefault(key, set()).add(value)

        def srem(self, key, value):
            self.sets.setdefault(key, set()).discard(value)

        def delete(self, key):
            self.values.pop(key, None)
            self.hashes.pop(key, None)
            self.sets.pop(key, None)

        def scan(self, cursor=0, match=None, count=100):
            return 0, []

    redis = FakeRedis()
    queued = enqueue_segment_window(
        redis,
        "j",
        "oss://b/a.mcap",
        frame_index=35,
        prefetch_segments=3,
        segment_frames=30,
        client_epoch=7,
    )
    assert queued == [30, 60, 90]
    items = [parse_queue_item(raw) for raw in redis.lists[REDIS_QUEUE_KEY]]
    assert [item["seg_start"] for item in items] == [30, 60, 90]
    assert items[0]["priority"] == 0
    assert items[1]["priority"] == 1
    assert items[0]["kind"] == "segment"
    assert items[0]["client_epoch"] == 7
    assert enqueue_segment_window(
        redis,
        "j",
        "oss://b/a.mcap",
        frame_index=35,
        prefetch_segments=3,
        segment_frames=30,
    ) == []

    req_a = create_encode_request(
        redis,
        "j",
        client_id="a",
        client_epoch=1,
        target_start=0,
        starts=[0],
    )
    req_b = create_encode_request(
        redis,
        "j",
        client_id="b",
        client_epoch=4,
        target_start=0,
        starts=[0],
    )
    first = enqueue_segment_window(
        redis,
        "j",
        "oss://b/a.mcap",
        frame_index=0,
        prefetch_segments=1,
        request_id=req_a,
        client_id="a",
    )
    second = enqueue_segment_window(
        redis,
        "j",
        "oss://b/a.mcap",
        frame_index=0,
        prefetch_segments=1,
        request_id=req_b,
        client_id="b",
    )
    assert first == [0]
    assert second == []
    complete_segment_requests(redis, "j", 0)
    assert get_encode_request(redis, "j", req_a)["status"] == "ready"
    assert get_encode_request(redis, "j", req_b)["status"] == "ready"

    req_c = create_encode_request(
        redis,
        "j",
        client_id="c",
        client_epoch=2,
        target_start=0,
        starts=[0],
    )
    enqueue_segment_window(
        redis,
        "j",
        "oss://b/a.mcap",
        frame_index=0,
        prefetch_segments=1,
        request_id=req_c,
    )
    assert cancel_encode_request(redis, "j", req_c)
    assert get_encode_request(redis, "j", req_c)["status"] == "cancelled"

    stale = FakeRedis()
    stale.hashes["sview:job:j"].update(
        {
            "source": "oss://b/a.mcap",
            "status": "processing",
            "publish_mode": "sparse",
            "available_ranges": "[]",
            "cancel": "0",
        }
    )
    stale.values["sview:segment:pending:j:0"] = "old"
    stale.values["sview:segment:lock:j:0"] = "dead-worker"
    jid, recovered = enqueue_sparse_job(
        stale,
        "oss://b/a.mcap",
        job_id="j",
        segment_frames=30,
    )
    assert jid == "j" and recovered
    assert stale.lists.get(REDIS_QUEUE_KEY, []) == []


def test_one_window_reuses_one_encode_session():
    from sview.jobs.job_status import ensure_sparse_session, set_client_frontier

    class Store:
        def __init__(self):
            self.hashes = {}
            self.values = {}
            self.lists = {}

        def hgetall(self, key):
            return self.hashes.get(key, {})

        def hset(self, key, *args, mapping=None):
            row = self.hashes.setdefault(key, {})
            if mapping is not None:
                row.update(mapping)
            elif len(args) == 2:
                row[args[0]] = args[1]

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value, nx=False, ex=None):
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True

        def lpush(self, key, value):
            self.lists.setdefault(key, []).insert(0, value)

    redis = Store()
    assert set_client_frontier(redis, "j", "window-a", 95, segment_frames=30) == 90
    assert ensure_sparse_session(
        redis, "j", "oss://b/a.mcap", "window-a", segment_frames=30
    )
    assert not ensure_sparse_session(
        redis, "j", "oss://b/a.mcap", "window-a", segment_frames=30
    )
    assert set_client_frontier(redis, "j", "window-a", 5040, segment_frames=30) == 5040
    assert len(redis.lists[REDIS_QUEUE_KEY]) == 1
    item = parse_queue_item(redis.lists[REDIS_QUEUE_KEY][0])
    assert item["kind"] == "sparse_session"
    assert item["client_id"] == "window-a"
    assert ensure_sparse_session(
        redis, "j", "oss://b/a.mcap", "window-b", segment_frames=30
    )
    assert len(redis.lists[REDIS_QUEUE_KEY]) == 2


def test_same_segment_does_not_take_another_worker_slot():
    from sview.jobs.job_status import ensure_sparse_session, set_client_frontier

    class Store:
        def __init__(self):
            self.hashes = {}
            self.values = {}
            self.lists = {}

        def hgetall(self, key):
            return self.hashes.get(key, {})

        def hset(self, key, *args, mapping=None):
            row = self.hashes.setdefault(key, {})
            if mapping is not None:
                row.update(mapping)
            elif len(args) == 2:
                row[args[0]] = args[1]

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value, nx=False, ex=None):
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True

        def expire(self, key, ttl):
            return key in self.values

        def delete(self, key):
            self.values.pop(key, None)
            return 1

        def lpush(self, key, value):
            self.lists.setdefault(key, []).insert(0, value)

    redis = Store()
    assert set_client_frontier(redis, "j", "tab-a", 0, segment_frames=30) == 0
    assert set_client_frontier(redis, "j", "tab-b", 10, segment_frames=30) == 0
    assert ensure_sparse_session(
        redis, "j", "oss://b/a.mcap", "tab-a", segment_frames=30
    )
    assert not ensure_sparse_session(
        redis, "j", "oss://b/a.mcap", "tab-b", segment_frames=30
    )
    assert len(redis.lists[REDIS_QUEUE_KEY]) == 1
    item = parse_queue_item(redis.lists[REDIS_QUEUE_KEY][0])
    assert item["client_id"] == "tab-a"
    # 另一段仍然单独开编码线程。
    assert set_client_frontier(redis, "j", "tab-c", 90, segment_frames=30) == 90
    assert ensure_sparse_session(
        redis, "j", "oss://b/a.mcap", "tab-c", segment_frames=30
    )
    assert len(redis.lists[REDIS_QUEUE_KEY]) == 2


def test_fail_submit_marks_failed_and_clears_redis_cache():
    import fnmatch

    from sview.jobs.job_status import (
        JOB_KEY_PREFIX,
        REDIS_QUEUE_KEY,
        fail_submit,
        get_job_fields,
        job_redis_key,
        segment_slot_key,
        session_alive_key,
    )

    class Store:
        def __init__(self):
            self.hashes = {}
            self.values = {}
            self.lists = {}
            self.sets = {}

        def hgetall(self, key):
            return dict(self.hashes.get(key, {}))

        def hset(self, key, *args, mapping=None):
            row = self.hashes.setdefault(key, {})
            if mapping is not None:
                row.update({k: str(v) for k, v in mapping.items()})
            elif len(args) == 2:
                row[args[0]] = str(args[1])

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value, nx=False, ex=None):
            self.values[key] = value
            return True

        def delete(self, *keys):
            for key in keys:
                self.values.pop(key, None)
            return len(keys)

        def scan(self, cursor=0, match=None, count=10):
            keys = list(self.values) + list(self.hashes)
            if match:
                keys = [key for key in keys if fnmatch.fnmatch(key, match)]
            return 0, keys

        def srem(self, key, member):
            self.sets.setdefault(key, set()).discard(member)

        def decrby(self, key, amount):
            return 0

        def lrange(self, key, start, end):
            return list(self.lists.get(key, []))

        def lrem(self, key, count, value):
            items = self.lists.get(key, [])
            if value in items:
                items.remove(value)
            return 1

    source = "oss://bucket/original_data/teleoperation/Orin/clip.mcap"
    job_id = "clip_cabc"
    redis = Store()
    redis.hashes[job_redis_key(job_id)] = {
        "status": "processing",
        "source": source,
        "playable": "1",
        "frames_done": "30",
        "cache_bytes": "10",
        "publish_mode": "sparse",
    }
    redis.values[session_alive_key(job_id, "tab-a")] = "worker"
    redis.values[segment_slot_key(job_id, 0)] = "tab-a"
    redis.lists[REDIS_QUEUE_KEY] = [
        '{"v":2,"kind":"sparse_session","source":"%s","job_id":"%s","client_id":"tab-a"}'
        % (source, job_id)
    ]

    failed_ids = fail_submit(
        redis,
        "clip_missing_version",
        source,
        "source not found on local mount",
        publish_mode="sparse",
    )
    assert job_id in failed_ids
    fields = get_job_fields(redis, job_id)
    assert fields["status"] == "failed"
    assert fields["error"] == "source not found on local mount"
    assert fields["playable"] == "0"
    assert fields["frames_done"] == "0"
    assert session_alive_key(job_id, "tab-a") not in redis.values
    assert segment_slot_key(job_id, 0) not in redis.values
    assert redis.lists[REDIS_QUEUE_KEY] == []
    assert job_redis_key(job_id).startswith(JOB_KEY_PREFIX)


def test_one_active_segment_per_job():
    from sview.jobs.job_status import claim_job_segment, release_job_segment

    class Store:
        def __init__(self):
            self.values = {}

        def set(self, key, value, nx=False, ex=None):
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True

        def get(self, key):
            return self.values.get(key)

        def delete(self, key):
            self.values.pop(key, None)

    redis = Store()
    assert claim_job_segment(redis, "j", 180, "worker-a")
    assert not claim_job_segment(redis, "j", 210, "worker-b")
    release_job_segment(redis, "j", "worker-b")
    assert not claim_job_segment(redis, "j", 210, "worker-b")
    release_job_segment(redis, "j", "worker-a")
    assert claim_job_segment(redis, "j", 210, "worker-b")


def test_targeted_encode_drops_warmup_pictures():
    from sview.worker.cpu import _drop_leading_annexb_pictures

    def picture(index: int, key: bool) -> bytes:
        nal_type = 5 if key else 1
        return b"\x00\x00\x00\x01" + bytes([nal_type, index])

    blob = b"".join(picture(index, index == 0) for index in range(4))
    kept = _drop_leading_annexb_pictures(blob, 2)
    assert kept.startswith(b"\x00\x00\x00\x01\x01\x02")
    assert _drop_leading_annexb_pictures(blob, 0) == blob


def test_forward_encode_follows_playhead_and_seek():
    from sview.jobs.job_status import enqueue_forward_from

    class Store:
        def __init__(self):
            self.hashes = {}
            self.values = {}
            self.lists = {}
            self.sets = {}

        def hgetall(self, key):
            return self.hashes.get(key, {})

        def hincrby(self, key, field, amount):
            row = self.hashes.setdefault(key, {})
            value = int(row.get(field, 0)) + amount
            row[field] = str(value)
            return value

        def hset(self, key, *args, mapping=None):
            row = self.hashes.setdefault(key, {})
            if mapping is not None:
                row.update(mapping)
            elif len(args) == 2:
                row[args[0]] = args[1]

        def set(self, key, value, nx=False, ex=None):
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True

        def lpush(self, key, value):
            self.lists.setdefault(key, []).insert(0, value)

        def expire(self, key, ttl):
            return True

        def sadd(self, key, value):
            self.sets.setdefault(key, set()).add(value)

    redis = Store()
    redis.hashes["sview:job:j"] = {
        "available_ranges": "[[0,30]]",
        "frame_count": "300",
        "encode_status": "idle",
    }
    queued = enqueue_forward_from(
        redis,
        "j",
        "oss://b/a.mcap",
        frame_index=0,
        window=2,
        set_frontier=True,
    )
    assert queued == [30, 60]
    assert redis.hashes["sview:job:j"]["encode_frontier"] == "0"
    items = [parse_queue_item(raw) for raw in redis.lists[REDIS_QUEUE_KEY]]
    assert [item["seg_start"] for item in items] == [30, 60]

    seek = Store()
    seek.hashes["sview:job:j"] = {
        "available_ranges": "[[90,120]]",
        "frame_count": "300",
        "encode_status": "idle",
    }
    queued = enqueue_forward_from(
        seek,
        "j",
        "oss://b/a.mcap",
        frame_index=95,
        window=2,
        set_frontier=True,
    )
    assert queued == [120, 150]
    assert seek.hashes["sview:job:j"]["encode_frontier"] == "90"


def test_cut_missing_file():
    assert cut_log_time_ns("/no/such.mcap", 10) is None


def test_sparse_cache_identity_is_shared_and_collision_safe():
    source = "oss://bucket/a/path/shared.mcap"
    first = sparse_cache_job_id(source)
    assert first == sparse_cache_job_id(source)
    assert first != sparse_cache_job_id("oss://bucket/other/path/shared.mcap")
    assert first.startswith("shared_c")


def test_queue_stats_show_session_and_waiting_job():
    import json as _json

    from sview.jobs.job_status import (
        JOB_KEY_PREFIX,
        REDIS_QUEUE_KEY,
        WORKER_KEY_PREFIX,
        collect_job_stats,
    )

    class Store:
        def __init__(self):
            self.values = {}
            self.hashes = {}
            self.lists = {REDIS_QUEUE_KEY: []}

        def llen(self, key):
            return len(self.lists.get(key, []))

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value, ex=None):
            self.values[key] = value
            return True

        def hgetall(self, key):
            return self.hashes.get(key, {})

        def scan(self, cursor=0, match=None, count=100):
            keys = list(self.values) + list(self.hashes)
            if match and match.endswith("*"):
                prefix = match[:-1]
                keys = [key for key in keys if str(key).startswith(prefix)]
            return 0, keys

    redis = Store()
    running = "SSIEGO_running"
    waiting = "SSIEGO_waiting"
    redis.hashes[f"{JOB_KEY_PREFIX}{running}"] = {
        "status": "processing",
        "source": "oss://bucket/running.mcap",
        "frames_done": "30",
        "playable": "1",
    }
    redis.hashes[f"{JOB_KEY_PREFIX}{waiting}"] = {
        "status": "queued",
        "source": "oss://bucket/waiting.mcap",
        "frames_done": "0",
    }
    redis.values[f"{WORKER_KEY_PREFIX}pod-1"] = _json.dumps(
        {
            "worker_id": "pod-1",
            "pod_ip": "10.0.0.8",
            "processing": 1,
            "jobs": [f"{running}:client:window-a"],
        }
    )
    redis.values[f"{WORKER_KEY_PREFIX}pod-2"] = _json.dumps(
        {
            "worker_id": "pod-2",
            "pod_ip": "10.0.0.9",
            "processing": 1,
            "jobs": [f"{running}:client:window-b"],
        }
    )
    stats = collect_job_stats(redis)
    by_id = {item["job_id"]: item for item in stats["jobs"]}
    assert stats["queued"] == 1
    assert stats["processing"] == 2
    assert by_id[running]["status"] == "processing"
    assert by_id[running]["clients"] == ["window-a", "window-b"]
    assert [s["pod_ip"] for s in by_id[running]["sessions"]] == ["10.0.0.8", "10.0.0.9"]
    assert len(stats["jobs"]) == 2
    assert by_id[waiting]["status"] == "queued"


def test_forward_run_reuses_only_the_next_frame():
    from sview.worker.cpu import _same_forward_cursor

    assert _same_forward_cursor(240, 240)
    assert not _same_forward_cursor(240, 270)
    assert not _same_forward_cursor(None, 240)


def test_playhead_behind_the_hole_does_not_drop_the_run():
    from sview.worker.cpu import (
        _forward_read_floor,
        _frontier_left_forward_run,
        _headcam_forward_topic,
    )

    ranges = [[0, 1350]]
    assert not _frontier_left_forward_run(0, 1350, 36001, ranges, 30)
    assert not _frontier_left_forward_run(1320, 1350, 36001, ranges, 30)
    assert not _frontier_left_forward_run(1350, 1350, 36001, ranges, 30)
    assert _frontier_left_forward_run(0, 21660, 36001, ranges, 30)
    assert _frontier_left_forward_run(21660, 1350, 36001, ranges, 30)
    assert not _frontier_left_forward_run(
        1350, 1350, 36001, [[0, 1410]], 30, reading=1410,
    )
    assert _frontier_left_forward_run(
        28860, 1380, 36001, [[0, 1380]], 30, reading=1380,
    )
    assert _forward_read_floor({"RR": 0, "FR": 80, "FL": 50}) == 50
    assert _forward_read_floor({"RR": 0}) == 0
    assert _headcam_forward_topic("/imu/sample")
    assert _headcam_forward_topic("/camera/RR/video")
    assert _headcam_forward_topic("/camera/FL/meta")
    assert not _headcam_forward_topic("/lidar/points")


def test_session_waits_on_the_first_missing_segment():
    from sview.worker.cpu import _next_session_segment

    ranges = [[0, 60]]
    nxt, blocked, skipped = _next_session_segment(
        0,
        120,
        ranges,
        30,
        locked_fn=lambda seg: seg == 60,
        cooling_fn=lambda seg: False,
    )
    assert nxt is None
    assert blocked is True
    assert skipped == [60]

    nxt, blocked, skipped = _next_session_segment(
        60,
        120,
        ranges,
        30,
        locked_fn=lambda seg: False,
        cooling_fn=lambda seg: seg == 60,
    )
    assert nxt is None
    assert blocked is True
    assert skipped == []

    nxt, blocked, skipped = _next_session_segment(
        0,
        120,
        ranges,
        30,
        locked_fn=lambda seg: False,
        cooling_fn=lambda seg: False,
    )
    assert nxt == 60
    assert blocked is False
    assert skipped == []

    from sview.worker.cpu import FairEncodeGate

    gate = FairEncodeGate(1, job_max=1, playable_max=1, foreground_reserve=0)
    gate.acquire("job")
    try:
        gate.acquire("job", timeout=0.3)
        raise AssertionError("encode gate wait should time out")
    except TimeoutError:
        pass
    gate.release("job")
    gate.acquire("job", timeout=0.3)
    gate.release("job")

    from sview.worker import cpu as cpu_mod

    tight = cpu_mod.FairEncodeGate(2, job_max=2, playable_max=2, foreground_reserve=0)
    previous = cpu_mod.encode_gate
    cpu_mod.encode_gate = tight
    encoders = []
    try:
        encoders = [
            cpu_mod.LiveX264Encoder.annexb(
                codec="h264",
                scale_w=16,
                scale_h=16,
                job_id="same-segment",
            )
            for _ in range(4)
        ]
        for enc in encoders:
            enc._hold_slot()
        assert sum(1 for enc in encoders if enc._acquired) == 2
    finally:
        for enc in encoders:
            enc._release()
        cpu_mod.encode_gate = previous


def test_frames_before_random_access_counts_leading_p_frames():
    from sview.worker.cpu import _frames_before_random_access

    idr = b"\x00\x00\x00\x01\x65" + b"\x11" * 8
    pframe = b"\x00\x00\x00\x01\x41" + b"\x22" * 8
    assert _frames_before_random_access([idr, pframe]) == 0
    assert _frames_before_random_access([pframe] * 13 + [idr]) == 13
    assert _frames_before_random_access([pframe] * 30) == 30


def test_sparse_session_index_failure_marks_job_failed():
    import fnmatch

    import sview.ingest.frame_index as frame_index_mod
    import sview.ingest.hdf5_images as hdf5_mod
    import sview.ingest.orin_images as orin_mod
    from sview.jobs.job_status import (
        get_job_fields,
        job_redis_key,
        segment_slot_key,
        session_alive_key,
    )
    from sview.worker import cpu as cpu_mod

    class Store:
        def __init__(self):
            self.hashes = {}
            self.values = {}
            self.lists = {}

        def hgetall(self, key):
            return dict(self.hashes.get(key, {}))

        def hset(self, key, *args, mapping=None):
            row = self.hashes.setdefault(key, {})
            if mapping is not None:
                row.update({k: str(v) for k, v in mapping.items()})
            elif len(args) == 2:
                row[args[0]] = str(args[1])

        def get(self, key):
            return self.values.get(key)

        def set(self, key, value, nx=False, ex=None):
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True

        def expire(self, key, ttl):
            return key in self.values

        def delete(self, *keys):
            for key in keys:
                self.values.pop(key, None)
            return len(keys)

        def scan(self, cursor=0, match=None, count=10):
            keys = list(self.values) + list(self.hashes)
            if match:
                keys = [key for key in keys if fnmatch.fnmatch(key, match)]
            return 0, keys

        def srem(self, key, member):
            return 0

        def decrby(self, key, amount):
            return 0

        def lrange(self, key, start, end):
            return list(self.lists.get(key, []))

        def lrem(self, key, count, value):
            return 0

    def no_index(path, index_path):
        raise frame_index_mod.SparseIndexUnavailable("mcap has no chunk index")

    source = "oss://bucket/original_data/teleoperation/Unitree/clip.mcap"
    job_id = "unitree_clip_c1"
    redis = Store()
    redis.hashes[job_redis_key(job_id)] = {"status": "queued", "source": source}
    saved = (
        cpu_mod.redis_path_to_local,
        cpu_mod.ingest_family,
        hdf5_mod.is_hdf5_file,
        orin_mod.orin_stream_kind,
        frame_index_mod.load_or_build_frame_index,
    )
    cpu_mod.redis_path_to_local = lambda s: Path("/nonexistent/clip.mcap")
    cpu_mod.ingest_family = lambda s: "teleop"
    hdf5_mod.is_hdf5_file = lambda p: False
    orin_mod.orin_stream_kind = lambda p, skip_headcam_guard=False: "h264"
    frame_index_mod.load_or_build_frame_index = no_index
    try:
        cpu_mod._run_sparse_session(
            redis,
            {"job_id": job_id, "source": source, "client_id": "tab-a", "segment_frames": 30},
        )
    finally:
        (
            cpu_mod.redis_path_to_local,
            cpu_mod.ingest_family,
            hdf5_mod.is_hdf5_file,
            orin_mod.orin_stream_kind,
            frame_index_mod.load_or_build_frame_index,
        ) = saved
    fields = get_job_fields(redis, job_id)
    assert fields["status"] == "failed", fields
    assert "no chunk index" in fields["error"]
    assert session_alive_key(job_id, "tab-a") not in redis.values
    assert segment_slot_key(job_id, 0) not in redis.values


def test_headcam_pull_segment_defaults_to_headcam_channels():
    from sview.worker.cpu import HEADCAM_CHANNELS, _HeadcamForwardRun

    run = _HeadcamForwardRun(
        local_path=Path("/nonexistent/headcam.mcap"),
        job_id="hc",
        source="oss://b/head_cam/x.mcap",
        seg_n=30,
    )
    for ch in HEADCAM_CHANNELS:
        run.frames[ch] = [b"f"] * 30
        run.timestamps[ch] = list(range(30))
    run._primed = True
    run._read_one = lambda seek_back: (_ for _ in ()).throw(AssertionError("read past a full segment"))
    run._pull_segment(30, lambda: False)
    assert run.channels == list(HEADCAM_CHANNELS)


def test_frames_before_first_idr_stay_empty_and_encoder_realigns():
    from sview.worker.cpu import _TeleopForwardRun

    idr = b"\x00\x00\x00\x01\x65" + b"\x11" * 8
    pframe = b"\x00\x00\x00\x01\x41" + b"\x22" * 8

    class FakeEncoder:
        def __init__(self, log):
            self.log = log

        def encode_frames(self, frames, timeout=None, *, output_skip=0):
            self.log.append((len(frames), output_skip))
            return [b"out%d" % i for i in range(len(frames) - output_skip)]

        def close(self):
            pass

    calls = {"a": [], "b": []}
    restarts = []
    run = _TeleopForwardRun(
        kind="h264",
        local_path=Path("/nonexistent/x.mcap"),
        job_id="j",
        source="oss://b/teleoperation/Orin/x.mcap",
        seg_n=4,
    )
    run.channels = ["a", "b"]
    run._primed = True
    run.encoders = {ch: FakeEncoder(calls[ch]) for ch in run.channels}

    def make(ch):
        restarts.append(ch)
        return FakeEncoder(calls[ch])

    run._make_encoder = make

    def feed(ch, frames):
        run.frames[ch] = list(frames)
        run.timestamps[ch] = list(range(len(frames)))

    # a 从第 0 帧就有 IDR；b 的 IDR 在第 3 帧。
    feed("a", [idr, pframe, pframe, pframe])
    feed("b", [pframe, pframe, pframe, idr])
    run._encode_blobs(4)
    assert calls["b"] == [(1, 0)]
    assert run._realign["b"] == [idr]

    # 下一段：b 重开 ffmpeg，用上一段的尾巴预热；P 帧照常编。
    feed("a", [pframe] * 4)
    feed("b", [pframe] * 4)
    run._encode_blobs(4)
    assert restarts == ["b"]
    assert calls["b"][-1] == (5, 1)
    assert calls["a"] == [(4, 0), (4, 0)]
    assert "b" not in run._realign


class _HashStore:
    def __init__(self):
        self.hashes = {}
        self.values = {}
        self.sets = {}

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, *args, mapping=None):
        row = self.hashes.setdefault(key, {})
        if mapping is not None:
            row.update({k: str(v) for k, v in mapping.items()})
        elif len(args) == 2:
            row[args[0]] = str(args[1])

    def get(self, key):
        return self.values.get(key)

    def srem(self, key, member):
        self.sets.setdefault(key, set()).discard(member)

    def decrby(self, key, amount):
        return 0


def test_redis_ranges_follow_oss_when_segments_were_deleted():
    import json

    from sview.jobs.job_status import (
        first_unencoded_segment,
        get_job_fields,
        job_redis_key,
        sync_sparse_ranges_from_oss,
    )

    redis = _HashStore()
    job = "deleted_segments"
    redis.hashes[job_redis_key(job)] = {
        "status": "ready",
        "publish_mode": "sparse",
        "frame_count": "1206",
        "available_ranges": "[[0,1206]]",
        "frames_done": "1206",
        "manifest_revision": "41",
    }
    assert first_unencoded_segment(redis, job, "tab", 30) is None

    # 列举期间有新段发布：放弃这次同步。
    assert not sync_sparse_ranges_from_oss(
        redis, job, [[0, 30]], frame_count=1206, expect_revision="40"
    )
    assert sync_sparse_ranges_from_oss(
        redis, job, [[0, 30]], frame_count=1206, expect_revision="41"
    )
    fields = get_job_fields(redis, job)
    assert json.loads(fields["available_ranges"]) == [[0, 30]]
    assert fields["status"] == "processing"
    assert fields["frames_done"] == "30"
    assert fields["segments_ready_count"] == "1"
    assert first_unencoded_segment(redis, job, "tab", 30) == 30

    # OSS 比 Redis 多（刚发布还没写 Redis）不算过期，不改。
    assert not sync_sparse_ranges_from_oss(redis, job, [[0, 60]], frame_count=1206)


def test_gc_purge_resets_sparse_ranges():
    import json

    import sview.cache.oss_cache_gc as gc
    from sview.jobs.job_status import first_unencoded_segment, get_job_fields, job_redis_key

    redis = _HashStore()
    job = "gc_sparse"
    redis.hashes[job_redis_key(job)] = {
        "status": "ready",
        "publish_mode": "sparse",
        "frame_count": "1206",
        "available_ranges": "[[0,1206]]",
        "segments_ready_count": "41",
        "frames_done": "1206",
        "playable": "1",
        "cache_bytes": "10",
    }
    saved = gc.delete_job_prefix
    gc.delete_job_prefix = lambda job_id: 3
    try:
        gc.purge_job_play_cache(redis, job, reason="expired")
    finally:
        gc.delete_job_prefix = saved
    fields = get_job_fields(redis, job)
    assert json.loads(fields["available_ranges"]) == []
    assert fields["segments_ready_count"] == "0"
    assert fields["status"] == "queued"
    assert first_unencoded_segment(redis, job, "tab", 30) == 0


class _KvStore(_HashStore):
    def __init__(self):
        super().__init__()
        self.lists = {}
        self.ttls = {}

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        if ex is not None:
            self.ttls[key] = int(ex)
        return True

    def expire(self, key, ttl):
        if key in self.values:
            self.ttls[key] = int(ttl)
            return True
        return False

    def ttl(self, key):
        return self.ttls.get(key, -1) if key in self.values else -2

    def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)
        return len(keys)

    def scan(self, cursor=0, match=None, count=10):
        import fnmatch

        keys = [k for k in self.values if not match or fnmatch.fnmatch(k, match)]
        return 0, keys

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)


def test_dead_slot_holder_can_be_replaced():
    from sview.jobs.job_status import (
        claim_segment_slot,
        live_run_at,
        segment_slot_key,
        session_alive_key,
        session_pending_key,
    )

    redis = _KvStore()
    assert claim_segment_slot(redis, "j", 60, "a")
    # a 的线程还在跑：b 不能抢，也能看出 a 正停在这一段。
    redis.values[session_alive_key("j", "a")] = "worker"
    assert not claim_segment_slot(redis, "j", 60, "b")
    assert live_run_at(redis, "j", 60, "b")
    assert live_run_at(redis, "j", 90, "b")
    assert not live_run_at(redis, "j", 60, "a")
    # 只在排队也算活着。
    del redis.values[session_alive_key("j", "a")]
    redis.values[session_pending_key("j", "a")] = "1"
    assert not claim_segment_slot(redis, "j", 60, "b")
    # 都没有了：b 直接接手，不用等槽过期。
    del redis.values[session_pending_key("j", "a")]
    assert not live_run_at(redis, "j", 60, "b")
    assert claim_segment_slot(redis, "j", 60, "b")
    assert redis.values[segment_slot_key("j", 60)] == "b"


def test_hand_off_starts_recent_waiting_windows_only():
    from sview.jobs.job_status import (
        ENCODE_REQUEST_TTL_SEC,
        HANDOFF_RECENT_SEC,
        REDIS_QUEUE_KEY,
        decode_end,
        first_unencoded_segment,
        hand_off_waiting_windows,
        job_redis_key,
        mark_decode_end,
        session_frontier_key,
        set_client_frontier,
    )

    redis = _KvStore()
    job = "handoff"
    redis.hashes[job_redis_key(job)] = {
        "publish_mode": "sparse",
        "frame_count": "300",
        "available_ranges": "[[0,120]]",
    }
    # b 卡在 120 等；gone 很久没请求；playing 前沿那段已经有了，在播缓存。
    set_client_frontier(redis, job, "b", 120, segment_frames=30)
    set_client_frontier(redis, job, "gone", 150, segment_frames=30)
    redis.ttls[session_frontier_key(job, "gone")] = (
        ENCODE_REQUEST_TTL_SEC - HANDOFF_RECENT_SEC - 1
    )
    set_client_frontier(redis, job, "playing", 30, segment_frames=30)
    set_client_frontier(redis, job, "a", 120, segment_frames=30)
    started = hand_off_waiting_windows(redis, job, "oss://b/x.mcap", exclude="a")
    assert started == ["b"]
    assert len(redis.lists[REDIS_QUEUE_KEY]) == 1

    # 后面没有可解码的帧了：不再给这里排线程。
    mark_decode_end(redis, job, 120)
    assert decode_end(redis, job) == 120
    assert first_unencoded_segment(redis, job, "c", 30) is None


def test_second_viewer_closes_its_encoder_when_it_reaches_a_live_run():
    import json

    from sview.jobs.job_status import (
        REDIS_QUEUE_KEY,
        job_redis_key,
        segment_slot_key,
        session_alive_key,
        set_client_frontier,
    )
    from sview.worker import cpu as cpu_mod

    redis = _KvStore()
    job = "two_viewers"
    key = job_redis_key(job)
    # a 从 60 开始编，已经发布到 120，正在编 120。
    redis.hashes[key] = {
        "publish_mode": "sparse",
        "frame_count": "300",
        "available_ranges": "[[60,120]]",
        "status": "processing",
    }
    redis.values[segment_slot_key(job, 120)] = "a"
    redis.values[session_alive_key(job, "a")] = "worker"
    set_client_frontier(redis, job, "b", 0, segment_frames=30)

    encoded = []

    def fake_segment(r, item, **kw):
        start, end = int(item["seg_start"]), int(item["seg_end"])
        encoded.append(start)
        ranges = json.loads(redis.hashes[key]["available_ranges"])
        ranges.append([start, end])
        from sview.cache.oss_cache_gc import merge_available_ranges

        redis.hashes[key]["available_ranges"] = json.dumps(merge_available_ranges(ranges))
        return "ok"

    saved = (cpu_mod._run_one_segment_job, cpu_mod.redis_path_to_local)
    cpu_mod._run_one_segment_job = fake_segment
    cpu_mod.redis_path_to_local = lambda s: Path("/nonexistent/x.mcap")
    try:
        cpu_mod._run_sparse_session(
            redis,
            {"job_id": job, "source": "oss://b/x.mcap", "client_id": "b", "segment_frames": 30},
        )
    finally:
        cpu_mod._run_one_segment_job, cpu_mod.redis_path_to_local = saved
    # b 只编了 0 和 30，接上 a 已有的 60 之后就关掉，不去抢 a 前面的 120。
    assert encoded == [0, 30]
    assert redis.values[segment_slot_key(job, 120)] == "a"
    assert session_alive_key(job, "b") not in redis.values
    assert REDIS_QUEUE_KEY not in redis.lists


def test_single_viewer_jumps_over_its_own_cache():
    import json

    from sview.cache.oss_cache_gc import merge_available_ranges
    from sview.jobs.job_status import job_redis_key, set_client_frontier
    from sview.worker import cpu as cpu_mod

    redis = _KvStore()
    job = "one_viewer"
    key = job_redis_key(job)
    # 自己之前 seek 过，60-120 已经编好；没有别的窗口。
    redis.hashes[key] = {
        "publish_mode": "sparse",
        "frame_count": "180",
        "available_ranges": "[[60,120]]",
        "status": "processing",
    }
    set_client_frontier(redis, job, "a", 0, segment_frames=30)
    encoded = []

    def fake_segment(r, item, **kw):
        start, end = int(item["seg_start"]), int(item["seg_end"])
        encoded.append(start)
        ranges = json.loads(redis.hashes[key]["available_ranges"]) + [[start, end]]
        redis.hashes[key]["available_ranges"] = json.dumps(merge_available_ranges(ranges))
        return "ok"

    saved = (cpu_mod._run_one_segment_job, cpu_mod.redis_path_to_local, cpu_mod.time.sleep)
    cpu_mod._run_one_segment_job = fake_segment
    cpu_mod.redis_path_to_local = lambda s: Path("/nonexistent/x.mcap")
    cpu_mod.time.sleep = lambda s: None
    try:
        cpu_mod._run_sparse_session(
            redis,
            {"job_id": job, "source": "oss://b/x.mcap", "client_id": "a", "segment_frames": 30},
        )
    finally:
        cpu_mod._run_one_segment_job, cpu_mod.redis_path_to_local, cpu_mod.time.sleep = saved
    assert encoded == [0, 30, 120, 150]


def test_oss_redirect_does_not_hang_keepalive():
    import http.client
    import threading
    from http.server import ThreadingHTTPServer

    import sview.http.server as srv
    import sview.worker.cpu as cpu_mod

    class Handler(srv.SviewCacheHandler):
        def _oss_cache_enabled(self):
            return True

        def do_GET(self):
            if not self._redirect_oss_play("job", "segments", "000000.bin", body=True):
                self._send_err(500, "no redirect")

    saved = cpu_mod.sign_oss_url
    cpu_mod.sign_oss_url = lambda key, expires=0: f"https://bucket.oss-cn-shanghai.aliyuncs.com/{key}?sig=x"
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        for _ in range(2):
            conn.request("GET", "/api/jobs/job/segments/000000.bin")
            resp = conn.getresponse()
            assert resp.status == 302
            assert resp.getheader("Content-Length") == "0"
            assert resp.read() == b""
        conn.close()
    finally:
        server.shutdown()
        cpu_mod.sign_oss_url = saved


def _serve(handler_cls):
    import threading
    from http.server import ThreadingHTTPServer

    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def test_signed_media_signs_a_window_without_reading_oss():
    import http.client
    import json

    import sview.http.server as srv
    import sview.worker.cpu as cpu_mod
    from sview.jobs.job_status import job_redis_key

    redis = _KvStore()
    redis.hashes[job_redis_key("job")] = {"frame_count": "3042", "publish_mode": "sparse"}

    class Handler(srv.SviewCacheHandler):
        def _oss_cache_enabled(self):
            return True

    def no_manifest(*a, **kw):
        raise AssertionError("signed-media must not read the manifest")

    saved = (srv._get_redis, srv._load_play_manifest, cpu_mod.sign_oss_url)
    srv._get_redis = lambda: redis
    srv._load_play_manifest = no_manifest
    cpu_mod.sign_oss_url = lambda key, expires=0: f"https://b.oss-cn-shanghai.aliyuncs.com/{key}?e={expires}"
    server = _serve(Handler)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=5)
        conn.request("GET", "/api/jobs/job/signed-media?kind=segments&start=2580&count=600&expires=1800")
        resp = conn.getresponse()
        data = json.loads(resp.read())
        assert resp.status == 200, data
        # 3042 帧：2580..3030 共 16 段，越界的全部略过。
        assert [it["start"] for it in data["items"]] == list(range(2580, 3042, 30))
        assert data["expires_in"] == 1800
        assert data["items"][0]["url"].endswith("segments/002580.bin?e=1800")
        assert data["skipped"][0] == 3060

        conn.request("GET", "/api/jobs/job/signed-media?kind=segments&starts=2580,2581")
        resp = conn.getresponse()
        data = json.loads(resp.read())
        assert [it["start"] for it in data["items"]] == [2580]
        assert data["skipped"] == [2581]

        conn.request("GET", "/api/jobs/missing/signed-media?kind=segments&starts=0")
        resp = conn.getresponse()
        resp.read()
        assert resp.status == 404
        conn.close()
    finally:
        server.shutdown()
        srv._get_redis, srv._load_play_manifest, cpu_mod.sign_oss_url = saved


def test_oss_manifest_read_error_is_retry_not_missing():
    import oss2

    import sview.http.server as srv
    import sview.worker.cpu as cpu_mod

    class Bucket:
        def __init__(self, exc):
            self.exc = exc

        def get_object(self, key):
            raise self.exc

    saved = (cpu_mod.get_oss_bucket, srv._oss_cache_on)
    srv._oss_cache_on = lambda: True
    try:
        cpu_mod.get_oss_bucket = lambda: Bucket(
            oss2.exceptions.NoSuchKey(404, {}, b"", {})
        )
        assert srv._load_play_manifest(Path("/nonexistent"), "job") is None

        cpu_mod.get_oss_bucket = lambda: Bucket(ConnectionError("reset by peer"))
        man = srv._load_play_manifest(Path("/nonexistent"), "job")
        assert man is not None
        assert man["cache_align_failed"] is True
        assert man["cache_align_reason"] == "oss_read_failed"
    finally:
        cpu_mod.get_oss_bucket, srv._oss_cache_on = saved


if __name__ == "__main__":
    test_signed_media_signs_a_window_without_reading_oss()
    test_oss_manifest_read_error_is_retry_not_missing()
    test_oss_redirect_does_not_hang_keepalive()
    test_single_viewer_jumps_over_its_own_cache()
    test_dead_slot_holder_can_be_replaced()
    test_hand_off_starts_recent_waiting_windows_only()
    test_second_viewer_closes_its_encoder_when_it_reaches_a_live_run()
    test_redis_ranges_follow_oss_when_segments_were_deleted()
    test_gc_purge_resets_sparse_ranges()
    test_frames_before_first_idr_stay_empty_and_encoder_realigns()
    test_headcam_pull_segment_defaults_to_headcam_channels()
    test_sparse_session_index_failure_marks_job_failed()
    test_frames_before_random_access_counts_leading_p_frames()
    test_parse_and_job_id()
    test_remaining_frames()
    test_queue_json_origin()
    test_sparse_queue_item_and_priority_dedupe()
    test_one_window_reuses_one_encode_session()
    test_same_segment_does_not_take_another_worker_slot()
    test_fail_submit_marks_failed_and_clears_redis_cache()
    test_one_active_segment_per_job()
    test_targeted_encode_drops_warmup_pictures()
    test_forward_encode_follows_playhead_and_seek()
    test_cut_missing_file()
    test_sparse_cache_identity_is_shared_and_collision_safe()
    test_queue_stats_show_session_and_waiting_job()
    test_forward_run_reuses_only_the_next_frame()
    test_session_waits_on_the_first_missing_segment()
    test_playhead_behind_the_hole_does_not_drop_the_run()
    print("ok")
