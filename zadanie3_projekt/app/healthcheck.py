import os, sys, time

HEARTBEAT = "/tmp/heartbeat"

if not os.path.exists(HEARTBEAT):
    sys.exit(1)

if time.time() - os.path.getmtime(HEARTBEAT) > 1800:
    sys.exit(1)

sys.exit(0)
