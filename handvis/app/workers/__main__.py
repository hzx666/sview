"""python -m app.workers 启动 Redis 计算 worker。"""

from app.workers.build_worker import main

if __name__ == "__main__":
    main()
