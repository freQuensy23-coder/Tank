"""Run `tank replay … --database-url @REPLAY_DSN@` against tank_replay without exposing the password.

Builds the tank_replay DSN from TANK_DB_PASSWORD and substitutes it for the @REPLAY_DSN@
argument only. TANK_DATABASE_URL is left to the service settings (production), because the
replay CLI refuses a --database-url equal to settings.database_url.
"""

import os
import sys
from urllib.parse import quote

secret = os.environ["TANK_DB_PASSWORD"]
dsn = "postgresql://tank:" + quote(secret, safe="") + "@postgres:5432/tank_replay"
argv = [dsn if arg == "@REPLAY_DSN@" else arg for arg in sys.argv[1:]]
os.execvp(argv[0], argv)
