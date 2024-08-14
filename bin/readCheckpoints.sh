#!/bin/bash

# Directory containing the log files
LOG_DIR="logs/DAGLogs"

# Pattern to match the files
FILE_PATTERN="checkpoint.*.json"

for file in "$LOG_DIR"/$FILE_PATTERN; do
  if [[ -f "$file" ]]; then
    echo "Processing $file"
    cat "$file" | python3 -m json.tool
    echo
  else
    echo "No files matching the pattern found in $LOG_DIR"
  fi
done
