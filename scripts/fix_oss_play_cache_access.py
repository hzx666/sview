#!/usr/bin/env python3
"""保证浏览器能 GET player-bin/*：合并 CORS，并给前缀加 public-read ACL。

使用环境变量：
  OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET
  OSS_ENDPOINT（默认 https://oss-cn-shanghai.aliyuncs.com）
  OSS_BUCKET（默认 ss-oss-data-platform）
  OSS_CACHE_PREFIX（默认 player-bin）
"""
from __future__ import annotations

import os
import sys
import urllib.request

import oss2
from oss2.models import BucketCors, CorsRule

ENDPOINT = os.environ.get("OSS_ENDPOINT", "https://oss-cn-shanghai.aliyuncs.com")
BUCKET = os.environ.get("OSS_BUCKET", "ss-oss-data-platform")
PREFIX = (os.environ.get("OSS_CACHE_PREFIX", "player-bin") or "player-bin").strip("/") + "/"
LOCAL_ORIGINS = [
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:4173",
    "http://localhost:4173",
    "http://127.0.0.1:18080",
    "http://localhost:18080",
]


def main() -> int:
    ak = os.environ.get("OSS_ACCESS_KEY_ID") or ""
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET") or ""
    if not ak or not sk:
        print("missing OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET", file=sys.stderr)
        return 2

    auth = oss2.Auth(ak, sk)
    bucket = oss2.Bucket(auth, ENDPOINT, BUCKET)

    # --- 合并 CORS ---
    origins: list[str] = list(LOCAL_ORIGINS)
    methods = ["GET", "HEAD"]
    headers = ["*", "range", "origin", "content-type", "accept", "authorization", "x-oss-*"]
    expose = [
        "Content-Range",
        "Accept-Ranges",
        "Content-Length",
        "ETag",
        "Last-Modified",
        "Content-Disposition",
    ]
    try:
        existing = bucket.get_bucket_cors()
        for rule in existing.rules or []:
            for o in rule.allowed_origins or []:
                if o not in origins:
                    origins.append(o)
            for m in rule.allowed_methods or []:
                if m not in methods:
                    methods.append(m)
            for h in rule.allowed_headers or []:
                if h not in headers:
                    headers.append(h)
            for e in rule.expose_headers or []:
                if e not in expose:
                    expose.append(e)
    except oss2.exceptions.NoSuchCors:
        pass

    rule = CorsRule(
        allowed_origins=origins,
        allowed_methods=methods,
        allowed_headers=headers,
        expose_headers=expose,
        max_age_seconds=3600,
    )
    bucket.put_bucket_cors(BucketCors([rule]))
    print("CORS updated origins=", origins)

    # 公共 ACL 经常被拦（Block Public Access）。优先走 Master 签名跳转。
    print(
        "Note: skip public-read ACL (bucket may deny). "
        "Use Master OSS_MEDIA_REDIRECT=1 → 302 signed GET instead."
    )
    print("CORS merge done. Restart frontend and hit Master media URLs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
