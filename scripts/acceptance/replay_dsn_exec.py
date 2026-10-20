"""Run `tank replay … --database-url @REPLAY_DSN@` against tank_replay without exposing the password.

Builds the tank_replay DSN from the mounted tank_db_password secret and substitutes it for the
@REPLAY_DSN@ argument only. TANK_DATABASE_URL is left to the entrypoint (production), because the
replay CLI refuses a --database-url equal to settings.database_url.
"""
import os
import sys
from urllib.parse import quote

secret = open("/run/secrets/tank_db_password").read().strip()
dsn = "postgresql://tank:" + quote(secret, safe="") + "@postgres:5432/tank_replay"
argv = [dsn if arg == "@REPLAY_DSN@" else arg for arg in sys.argv[1:]]
os.execvp(argv[0], argv)
