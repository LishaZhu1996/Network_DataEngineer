import sys
import os

# Make src/ importable in all tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Default to local filesystem so tests never touch S3
os.environ.setdefault("USE_LOCAL_FS", "true")
os.environ.setdefault("BASE_DATA_PATH", "/tmp/network-pipeline-test")
