"""SView backend package.

Layout:
  http/     Master HTTP (enqueue, status, play cache)
  worker/   CPU / GPU encode workers
  ingest/   MCAP / HDF5 / Orin readers
  jobs/     Redis job status and bench types
  cache/    OSS play-cache GC and align
"""
