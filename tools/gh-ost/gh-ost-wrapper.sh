#!/bin/bash

ghost_log_path="$VTDATAROOT/gh-ost"
ghost_log_file=gh-ost.log

mkdir -p "$ghost_log_path"

echo "executing: gh-ost" "$@" > "$ghost_log_path/$ghost_log_file"
gh-ost "$@" > "$ghost_log_path/$ghost_log_file" 2>&1
