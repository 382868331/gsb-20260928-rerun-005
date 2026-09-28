import argparse
import os

from app.server import serve


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--db", default=os.path.join(
        os.path.dirname(__file__), "data", "orchestrator.db"))
    args = parser.parse_args()
    serve(args.db, args.port)
