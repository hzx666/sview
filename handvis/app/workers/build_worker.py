"""HandVis 计算 worker：插入外部集群时只依赖 Redis 收发消息。

本进程不对外提供 HTTP。集群 master 往队列 RPUSH，这里 BLMOVE 消费，
终态 RPUSH 到结果队列。一个进程同一时刻只跑一条任务。

启动：
    python -m app.workers.build_worker
    python -m app.workers
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

from app.core.config import (
    BUILD_EVENT_CHANNEL,
    BUILD_QUEUE_KEY,
    BUILD_RESULT_QUEUE_KEY,
    BUILD_WORKER_ID,
    TORCH_NUM_THREADS,
)
from app.core.logging_setup import setup_logging
from app.core.shutdown_signal_logging import install_shutdown_signal_logging
from app.services.build_executor import execute_job
from app.services.build_queue import (
    ack_job,
    claim_next_job,
    parse_queue_message,
    processing_key,
    reclaim_processing,
)

logger = logging.getLogger(__name__)

_stop = asyncio.Event()


def _pin_compute_threads() -> int:
    """在导入/运行 torch 前钉死线程数，避免一个任务打满整机 vCPU。"""
    n = max(1, int(TORCH_NUM_THREADS))
    thread_env = str(n)
    os.environ["OMP_NUM_THREADS"] = thread_env
    os.environ["MKL_NUM_THREADS"] = thread_env
    os.environ["OPENBLAS_NUM_THREADS"] = thread_env
    os.environ["NUMEXPR_NUM_THREADS"] = thread_env
    try:
        import torch

        torch.set_num_threads(n)
        torch.set_num_interop_threads(1)
    except Exception as exc:
        logger.warning("设置 torch 线程数失败，将依赖环境变量。err=%s", exc)
    return n


def _install_stop_handlers() -> None:
    loop = asyncio.get_running_loop()

    def _request_stop(signum: int, _frame) -> None:
        logger.info("worker 收到停止信号。signum=%s worker_id=%s", signum, BUILD_WORKER_ID)
        loop.call_soon_threadsafe(_stop.set)

    for name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        signal.signal(sig, _request_stop)


async def _run_claimed_message(raw_message: str) -> None:
    try:
        payload = parse_queue_message(raw_message)
    except (ValueError, TypeError) as exc:
        logger.warning("丢弃非法构建任务。raw=%s err=%s", raw_message[:500], exc)
        await ack_job(BUILD_WORKER_ID, raw_message)
        return

    try:
        await execute_job(payload, worker_id=BUILD_WORKER_ID)
    finally:
        await ack_job(BUILD_WORKER_ID, raw_message)


async def run_worker() -> None:
    """主循环：领取 → 执行 → ack。当前任务跑完后才退出。"""
    threads = _pin_compute_threads()
    reclaimed = await reclaim_processing(BUILD_WORKER_ID)
    logger.info(
        "HandVis job worker 已启动。worker_id=%s queue=%s result=%s events=%s processing=%s torch_threads=%s reclaimed=%s",
        BUILD_WORKER_ID,
        BUILD_QUEUE_KEY,
        BUILD_RESULT_QUEUE_KEY,
        BUILD_EVENT_CHANNEL,
        processing_key(BUILD_WORKER_ID),
        threads,
        reclaimed,
    )

    while not _stop.is_set():
        try:
            raw = await claim_next_job(BUILD_WORKER_ID)
        except Exception as exc:
            logger.exception("领取构建任务失败。worker_id=%s err=%s", BUILD_WORKER_ID, exc)
            await asyncio.sleep(1)
            continue
        if raw is None:
            continue
        logger.info("领取构建任务。worker_id=%s payload=%s", BUILD_WORKER_ID, raw[:500])
        try:
            await _run_claimed_message(raw)
        except Exception:
            logger.exception("执行构建任务未捕获异常。worker_id=%s", BUILD_WORKER_ID)

    logger.info("HandVis job worker 已停止。worker_id=%s", BUILD_WORKER_ID)


async def _async_main() -> None:
    setup_logging()
    _install_stop_handlers()
    install_shutdown_signal_logging()
    await run_worker()


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
