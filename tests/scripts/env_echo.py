"""Print PILINE_* env vars so tests can verify them."""
import os

for key in sorted(os.environ):
    if key.startswith("PILINE_"):
        print(f"{key}={os.environ[key]}")
