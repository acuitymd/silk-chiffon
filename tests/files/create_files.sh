#!/usr/bin/env bash

function log() {
  RED='\033[0;31m'
  GREEN='\033[0;32m'
  ORANGE='\033[0;33m'
  CYAN='\033[0;36m'
  NC='\033[0m'
  LEVEL=$1
  EMOJI=$2
  MSG=$3
  TIMESTAMP=$(date +'%H:%M:%S')
  case $LEVEL in
  "INFO") HEADER_COLOR=$GREEN ;;
  "WARN") HEADER_COLOR=$ORANGE ;;
  "ERROR") HEADER_COLOR=$RED ;;
  esac
  printf "${HEADER_COLOR}[%-5.5s]${CYAN} ${TIMESTAMP} ${EMOJI} ${NC}%b" "${LEVEL}" "${MSG}"
}

function info() {
  log "INFO" "🟢" "$1\n"
}

function warn() {
  log "WARN" "🟡" "$1\n"
}

function error() {
  log "ERROR" "🔴" "$1\n" && exit 1
}

log_and_run() {
  info "Running: $*"
  "$@"
}

DIR="$(cd "$(dirname "$0")" && pwd)"

info "Changing directory to $DIR"

if ! cd "$DIR"; then
  error "Failed to cd to $DIR"
  exit 1
fi

info "Removing existing files"

files=(
  "people.file.arrow"
  "people.stream.arrow"
  "people.parquet"
  "people.duckdb"
)

for file in "${files[@]}"; do
  if [ -f "$file" ]; then
    log_and_run rm "$file"
  else
    warn "File $file does not exist, skipping"
  fi
done

info "Creating duckdb format"

if ! duckdb -f duckdb_cli_create_files.sql &>/dev/null; then
  error "Failed to create DuckDB files"
  exit 1
fi

info "Creating arrow file format and parquet format"

if ! datafusion-cli --file datafusion_cli_create_files.sql &>/dev/null; then
  error "Failed to create DataFusion files"
  exit 1
fi

info "Creating arrow stream format"

if ! silk-chiffon arrow-to-arrow people.file.arrow people.stream.arrow --output-ipc-format stream &>/dev/null; then
  error "Failed to create Arrow stream file"
  exit 1
fi

info "Files created successfully"
