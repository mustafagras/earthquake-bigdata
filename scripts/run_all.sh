#!/bin/bash
# Convenience launcher for macOS/Linux and Git Bash on Windows.
# It just finds a Python interpreter and runs run.py (which does everything).
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Prefer the project venv, otherwise fall back to python3 / python.
if [ -x "$PROJECT_DIR/venv/bin/python3" ]; then
  PYTHON_BIN="$PROJECT_DIR/venv/bin/python3"
elif [ -x "$PROJECT_DIR/venv/Scripts/python.exe" ]; then
  PYTHON_BIN="$PROJECT_DIR/venv/Scripts/python.exe"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
else
  PYTHON_BIN="$(command -v python)"
fi
echo "Using Python: $PYTHON_BIN"

# Check dependencies are installed.
if ! "$PYTHON_BIN" -c "import requests, pyspark, pandas, matplotlib" >/dev/null 2>&1; then
  echo "ERROR: dependencies missing. Install them with:" >&2
  echo "       $PYTHON_BIN -m pip install -r \"$PROJECT_DIR/requirements.txt\"" >&2
  exit 1
fi

# On Windows (Git Bash), make sure winutils.exe is present for Hadoop.
case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*)
    if [ ! -f "$PROJECT_DIR/hadoop/bin/winutils.exe" ]; then
      echo "Setting up winutils.exe for Windows..."
      "$PYTHON_BIN" "$SCRIPT_DIR/setup_winutils.py"
    fi
    ;;
esac

exec "$PYTHON_BIN" "$PROJECT_DIR/run.py"
