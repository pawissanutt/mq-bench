#!/usr/bin/env bash
set -euo pipefail

# Orchestrate latency-vs-payload experiment across transports using N pub/sub pairs.
#
# Topology: N publishers → N subscribers (pair per topic)
# Workload: payloads sweep; total send rate fixed; per-publisher rate = total_rate / N
# Runner: uses scripts/run_multi_topic_perkey.sh for N paired topics
# Output: results/latency_vs_payload_<ts>/{raw_data,plots}/ with summary.csv and figures
#
# Usage examples:
#   # Default: N=10, total_rate=5000/s, payloads 1KB 10KB 100KB 500KB 1MB across zenoh/redis/nats/rabbitmq/mqtt
#   scripts/orchestrate_latency_vs_payload.sh
#
#   # Remote host for all brokers (keep default ports)
#   scripts/orchestrate_latency_vs_payload.sh --host 192.168.0.254
#
#   # Customize transports and payloads
#   scripts/orchestrate_latency_vs_payload.sh \
#     --transports "zenoh redis nats rabbitmq mqtt" \
#     --payloads "1024 10240 102400 512000 1048576" \
#     --pairs 10 --total-rate 5000 --duration 30
#
#   # Payloads also accept KB units (e.g., 1KB 2KB ... 1024KB)
#   scripts/orchestrate_latency_vs_payload.sh --payloads "1KB 2KB 4KB 8KB 16KB"
#
#   # MQTT multi-broker support (like orchestrate_benchmarks.sh):
#   # iterate all default brokers, or filter with --mqtt-brokers
#   scripts/orchestrate_latency_vs_payload.sh --transports "mqtt" --mqtt-brokers "mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884"
#
#   # Add a cool-down interval between tests (seconds):
#   scripts/orchestrate_latency_vs_payload.sh --interval-sec 2
#
#   # Latency-only mode (write minimal CSV and only plot latency vs payload):
#   scripts/orchestrate_latency_vs_payload.sh --latency-only
#
#   # Append new runs (e.g. only redis) into an existing summary:
#   scripts/orchestrate_latency_vs_payload.sh --summary results/latency_vs_payload_YYYYMMDD_HHMMSS/raw_data/summary.csv --transports "redis" --payloads "1024 10240" --pairs 10 --total-rate 5000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
PAIRS=10                # N pub/sub pairs
TOTAL_RATE=5000         # msgs/s (system-wide)
DURATION=30             # seconds
SNAPSHOT=5
RUN_ID_PREFIX="latency"
TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
PAYLOADS=(1024 10240 102400 512000 1048576)  # 1KB,10KB,100KB,500KB,1MB
# Keep immutable copies for fallback if user passes empty strings to flags
DEFAULT_TRANSPORTS=(zenoh redis nats rabbitmq mqtt)
DEFAULT_PAYLOADS=(1024 10240 102400 512000 1048576)
START_SERVICES=0
DRY_RUN=${DRY_RUN:-0}
HOST="${HOST:-}" 
SUMMARY_OVERRIDE="${SUMMARY_OVERRIDE:-}"  # optional path to existing summary.csv to append
INTERVAL_SEC=15           # optional sleep between tests
LATENCY_ONLY=0            # only capture latency columns in summary

# MQTT brokers (name:host:port). Default to local compose ports.
MQTT_BROKERS="mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885 rabbitmq:127.0.0.1:1886 artemis:127.0.0.1:1887"
declare -a MQTT_BROKERS_ARR=()

# Treat suspicious quote-only tokens as empty (handles accidental smart quotes passed as values)
is_quote_only_token() {
  local v="$1"
  case "$v" in
    "\"\""|"''"|"“”"|"””"|"‘‘"|"’’") return 0;;
    *) return 1;;
  esac
}

# Convert payload tokens (possibly with KB suffix) to bytes
to_bytes() {
  local tok="$1"
  if [[ "$tok" =~ ^[0-9]+$ ]]; then
    echo "$tok"; return 0
  fi
  # Accept k/K/kb/KB suffix as kibibytes (x1024)
  if [[ "$tok" =~ ^([0-9]+)[Kk][Bb]?$ ]]; then
    local n="${BASH_REMATCH[1]}"
    echo $(( n * 1024 ))
    return 0
  fi
  # Accept b/B suffix explicitly as bytes
  if [[ "$tok" =~ ^([0-9]+)[bB]$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  # Fallback: try as integer
  if [[ "$tok" =~ ^([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}"; return 0
  fi
  echo "$tok"  # return as-is; caller may handle
}

timestamp() { date +%Y%m%d_%H%M%S; }
# Directories are initialized after parsing CLI to honor overrides
TS=""
BENCH_DIR=""
RAW_DIR=""
PLOTS_DIR=""
SUMMARY_CSV=""

init_dirs() {
  # Use existing TS if set, else generate
  if [[ -z "${TS}" ]]; then TS="$(timestamp)"; fi
  # Default bench dir if none provided via overrides
  if [[ -z "${BENCH_DIR}" ]]; then
    BENCH_DIR="${REPO_ROOT}/results/latency_vs_payload_${TS}"
  fi
  # Set defaults if not explicitly overridden later
  if [[ -z "${RAW_DIR}" ]]; then RAW_DIR="${BENCH_DIR}/raw_data"; fi
  if [[ -z "${PLOTS_DIR}" ]]; then PLOTS_DIR="${BENCH_DIR}/plots"; fi

  # Normalize to absolute paths
  case "${RAW_DIR}" in /*) ;; *) RAW_DIR="${REPO_ROOT}/${RAW_DIR}" ;; esac
  case "${PLOTS_DIR}" in /*) ;; *) PLOTS_DIR="${REPO_ROOT}/${PLOTS_DIR}" ;; esac

  mkdir -p "${RAW_DIR}" "${PLOTS_DIR}"

  # SUMMARY override: allow file or directory path
  if [[ -n "${SUMMARY_OVERRIDE}" ]]; then
    if [[ -d "${SUMMARY_OVERRIDE}" || "${SUMMARY_OVERRIDE}" != *.csv ]]; then
      # Treat as directory
      local dir="${SUMMARY_OVERRIDE}"
      case "${dir}" in /*) ;; *) dir="${REPO_ROOT}/${dir}" ;; esac
      mkdir -p "${dir}"
      SUMMARY_CSV="${dir}/summary.csv"
    else
      # Treat as explicit file path
      case "${SUMMARY_OVERRIDE}" in /*) SUMMARY_CSV="${SUMMARY_OVERRIDE}" ;; *) SUMMARY_CSV="${REPO_ROOT}/${SUMMARY_OVERRIDE}" ;; esac
      mkdir -p "$(dirname -- "${SUMMARY_CSV}")"
    fi
  else
    SUMMARY_CSV="${RAW_DIR}/summary.csv"
  fi
}

log() { echo "[$(date +%H:%M:%S)] $*"; }
run() { if [[ "${DRY_RUN}" = 1 ]]; then echo "+ $*"; else eval "$*"; fi }

usage() {
  # Print only lines starting with "# " from the file header
  sed -n '1,120p' "$0" | sed -n 's/^# //p'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pairs)
      shift; PAIRS=${1:-10} ;;
    --total-rate)
      shift; TOTAL_RATE=${1:-5000} ;;
    --duration)
      shift; DURATION=${1:-30} ;;
    --snapshot)
      shift; SNAPSHOT=${1:-5} ;;
    --payloads)
      shift;
      if [[ -n "${1:-}" ]]; then
        # Accept space-, comma-, or newline-separated tokens
        _raw_payloads="${1}"
        # Replace commas with spaces
        _raw_payloads="${_raw_payloads//,/ }"
        # Split on any whitespace (space, tab, newline)
        IFS=$' \t\n' read -r -a PAYLOADS <<<"${_raw_payloads}"
        # Normalize KB/B suffixes to bytes
        _norm=()
        for tok in "${PAYLOADS[@]}"; do
          b=""
          b=$(to_bytes "$tok")
          if [[ "$b" =~ ^[0-9]+$ ]]; then
            _norm+=("$b")
          else
            echo "[warn] ignoring invalid payload token: '$tok'" >&2
          fi
        done
        PAYLOADS=("${_norm[@]}")
      else
        echo "[warn] --payloads given with empty value; keeping defaults: ${PAYLOADS[*]}" >&2
      fi
      ;;
    --transports)
      shift;
      if [[ -n "${1:-}" ]]; then
        IFS=' ' read -r -a TRANSPORTS <<<"${1}"
      else
        echo "[warn] --transports given with empty value; keeping defaults: ${TRANSPORTS[*]}" >&2
      fi
      ;;
    --run-id-prefix)
      shift; RUN_ID_PREFIX=${1:-latency} ;;
    --host)
      shift; HOST=${1:-} ;;
    --start-services)
      START_SERVICES=1 ;;
    --dry-run)
      DRY_RUN=1 ;;
    --summary)
      shift; SUMMARY_OVERRIDE=${1:-}
      if is_quote_only_token "${SUMMARY_OVERRIDE}"; then SUMMARY_OVERRIDE=""; fi ;;
    --out-dir)
      shift; PLOTS_DIR=${1:-}
      if is_quote_only_token "${PLOTS_DIR}"; then PLOTS_DIR=""; fi ;;
    --raw-dir)
      shift; RAW_DIR=${1:-}
      if is_quote_only_token "${RAW_DIR}"; then RAW_DIR=""; fi ;;
    --bench-dir)
      shift; BENCH_DIR=${1:-}
      if is_quote_only_token "${BENCH_DIR}"; then BENCH_DIR=""; fi ;;
    --mqtt-brokers)
      shift; MQTT_BROKERS=${1:-} ;;
    --interval-sec)
      shift; INTERVAL_SEC=${1:-0} ;;
    --latency-only)
      LATENCY_ONLY=1 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
  shift || true
done

# Initialize directories and summary file now that CLI overrides are parsed
init_dirs

# Guardrails: if arrays were emptied by empty args, restore sensible defaults
if [[ ${#PAYLOADS[@]} -eq 0 ]]; then
  echo "[warn] No payloads specified after parsing; restoring defaults: ${DEFAULT_PAYLOADS[*]}" >&2
  PAYLOADS=("${DEFAULT_PAYLOADS[@]}")
fi
if [[ ${#TRANSPORTS[@]} -eq 0 ]]; then
  echo "[warn] No transports specified after parsing; restoring defaults: ${DEFAULT_TRANSPORTS[*]}" >&2
  TRANSPORTS=("${DEFAULT_TRANSPORTS[@]}")
fi

# Visibility: show resolved output and summary destinations
log "Resolved: SUMMARY_CSV=${SUMMARY_CSV} | PLOTS_DIR=${PLOTS_DIR} | RAW_DIR=${RAW_DIR}"
log "Resolved payloads (bytes): ${PAYLOADS[*]}"

# Materialize MQTT brokers array and apply HOST rewrite if needed
IFS=' ' read -r -a MQTT_BROKERS_ARR <<<"${MQTT_BROKERS}"
if [[ -n "${HOST}" ]]; then
  declare -a _REWRITTEN=()
  for tok in "${MQTT_BROKERS_ARR[@]}"; do
    IFS=: read -r bname _bhost bport <<<"${tok}"
    _REWRITTEN+=("${bname}:${HOST}:${bport}")
  done
  MQTT_BROKERS_ARR=("${_REWRITTEN[@]}")
fi

# Header
if [[ ! -s "${SUMMARY_CSV}" ]]; then
  if [[ ${LATENCY_ONLY} -eq 1 ]]; then
    echo "transport,payload,rate,run_id,p50_ms,p95_ms,p99_ms" > "${SUMMARY_CSV}"
  else
    echo "transport,payload,rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes" > "${SUMMARY_CSV}"
  fi
fi

# Optionally start local services unless HOST indicates remote
ensure_services() {
  if [[ ${START_SERVICES} -eq 1 ]]; then
    if [[ -n "${HOST}" ]] && [[ "${HOST}" != "127.0.0.1" ]] && [[ "${HOST}" != "localhost" ]]; then
      log "HOST=${HOST} indicates remote brokers; skipping local service startup"
    else
      log "Starting local services via compose_up.sh"
      run "bash \"${SCRIPT_DIR}/compose_up.sh\""
    fi
  fi
}

# Append last-snapshot metrics from a run directory into SUMMARY_CSV
append_summary_from_artifacts() {
  local transport="$1" payload="$2" rate="$3" run_id="$4" art_dir="$5"
  local sub_csv pub_csv last_sub last_pub
  sub_csv="${art_dir}/sub.csv"
  pub_csv="${art_dir}/pub.csv"
  if [[ ! -f "${sub_csv}" ]]; then
    if [[ -f "${art_dir}/sub_agg.csv" ]]; then
      sub_csv="${art_dir}/sub_agg.csv"
    else
      log "WARN: Missing subscriber CSV in ${art_dir}"; return 0
    fi
  fi
  if [[ ! -f "${pub_csv}" ]]; then
    if [[ -f "${art_dir}/mt_pub.csv" ]]; then
      pub_csv="${art_dir}/mt_pub.csv"
    fi
  fi
  last_sub=$(tail -n +2 "${sub_csv}" 2>/dev/null | tail -n1 || true)
  last_pub=$(tail -n +2 "${pub_csv}" 2>/dev/null | tail -n1 || true)
  if [[ -z "${last_sub}" ]]; then
    log "WARN: No subscriber data for ${run_id}"; return 0
  fi
  # sub: ts,sent,recv,errors,tps,itps,p50,p95,p99,min,max,mean
  local _ts _sent recv _err tps _it p50 p95 p99 _min _max _mean
  IFS=, read -r _ts _sent recv _err tps _it p50 p95 p99 _min _max _mean <<<"${last_sub}"
  local pub_tps sent
  pub_tps=""; sent="${_sent}"
  if [[ -n "${last_pub}" ]]; then
    # pub: ts,sent,recv,errors,tt,it,...
    local _pts psent _prev _perr ptt _pit _a _b _c _d _e _f
    IFS=, read -r _pts psent _prev _perr ptt _pit _a _b _c _d _e _f <<<"${last_pub}"
    pub_tps="${ptt}"; sent="${psent}"
  fi
  local ns_to_ms='awk -v n="$1" '\''BEGIN{if(n==""||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}'\''' 
  local p50_ms p95_ms p99_ms
  p50_ms=$(awk -v n="${p50}" 'BEGIN{if(n==""||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')
  p95_ms=$(awk -v n="${p95}" 'BEGIN{if(n==""||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')
  p99_ms=$(awk -v n="${p99}" 'BEGIN{if(n==""||n=="-"||n=="NaN"){print ""}else{printf("%.3f", n/1e6)}}')

  # Docker stats aggregation (optional)
  local stats_csv="${art_dir}/docker_stats.csv" max_cpu max_mem_perc max_mem_used
  max_cpu=""; max_mem_perc=""; max_mem_used=""
  if [[ -f "${stats_csv}" ]]; then
    local agg
    agg=$(awk -F, '
      NR==1 { has_memprec=(NF>=12); has_cpuprec=(NF>=17); next }
      {
        if (has_cpuprec) { c=$17+0 } else { c=$4; gsub(/%/,"",c); c+=0 }
        if (c>mcpu) mcpu=c
        if (has_memprec) {
          used_b=$10+0; tot_b=$11+0; if (used_b>mused) mused=used_b; perc=$12+0; if (perc==0 && tot_b>0) perc=(used_b/tot_b)*100.0; if (perc>mperc) mperc=perc
        } else {
          split($5, mparts, " / ");
          # crude bytes parse: assume first is used
          # skip detailed parsing; this is optional
        }
      }
      END{ if(mcpu=="")mcpu=0; if(mperc=="")mperc=0; if(mused=="")mused=0; printf("%.6f,%.6f,%.0f", mcpu,mperc,mused) }
    ' "${stats_csv}" || true)
    IFS=, read -r max_cpu max_mem_perc max_mem_used <<<"${agg}"
  fi
  if [[ ${LATENCY_ONLY} -eq 1 ]]; then
    echo "${transport},${payload},${rate},${run_id},${p50_ms},${p95_ms},${p99_ms}" >> "${SUMMARY_CSV}"
  else
    echo "${transport},${payload},${rate},${run_id},${tps},${p50_ms},${p95_ms},${p99_ms},${pub_tps},${sent},${recv},${_err},${art_dir},${max_cpu},${max_mem_perc},${max_mem_used}" >> "${SUMMARY_CSV}"
  fi
}

# Execute one (transport, payload) combo
run_combo() {
  local transport="$1" payload="$2" pairs="$3" total_rate="$4"
  local per_rate=$(( total_rate / pairs ))
  local rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_n${pairs}_r${per_rate}"
  local art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_multi_topic_perkey"
  local env_common
  env_common="TENANTS=${pairs} REGIONS=1 SERVICES=1 SHARDS=1 SUBSCRIBERS=${pairs} PUBLISHERS=${pairs} PAYLOAD=${payload} RATE=${per_rate} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT} TOPIC_PREFIX=bench/pair"

  local host_env=""
  case "${transport}" in
    zenoh)
      if [[ -n "${HOST}" ]]; then host_env="ENDPOINT_SUB=tcp/${HOST}:7447 ENDPOINT_PUB=tcp/${HOST}:7448"; fi
      run "ENGINE=zenoh ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    redis)
      if [[ -n "${HOST}" ]]; then host_env="REDIS_URL=redis://${HOST}:6379"; fi
      run "ENGINE=redis ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    nats)
      if [[ -n "${HOST}" ]]; then host_env="NATS_HOST=${HOST}"; fi
      run "ENGINE=nats ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    rabbitmq)
      if [[ -n "${HOST}" ]]; then host_env="RABBITMQ_HOST=${HOST}"; fi
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\""
      ;;
    mqtt)
      # Expand into per-broker runs
      if [[ ${#MQTT_BROKERS_ARR[@]} -eq 0 ]]; then
        log "WARN: No MQTT brokers defined; skipping"
      else
        for b in "${MQTT_BROKERS_ARR[@]}"; do
          IFS=: read -r bname bhost bport <<<"${b}"
          local br_transport="mqtt_${bname}"
          local br_rid="${RUN_ID_PREFIX}_$(timestamp)_${br_transport}_p${payload}_n${pairs}_r${per_rate}"
          local br_art_dir="${REPO_ROOT}/artifacts/${br_rid}/fanout_multi_topic_perkey"
          host_env="MQTT_HOST=${bhost} MQTT_PORT=${bport}"
          run "ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${br_rid}\""
          append_summary_from_artifacts "${br_transport}" "${payload}" "${total_rate}" "${br_rid}" "${br_art_dir}"
        done
      fi
      # Return early since we handled summary append per broker
      return 0
      ;;
    *)
      log "Unknown transport: ${transport}"; return 1 ;;
  esac

  append_summary_from_artifacts "${transport}" "${payload}" "${total_rate}" "${rid}" "${art_dir}"
}

main() {
  log "Latency vs payload benchmark → ${BENCH_DIR}"
  ensure_services

  for t in "${TRANSPORTS[@]}"; do
    for p in "${PAYLOADS[@]}"; do
      if [[ "${t}" == "mqtt" ]]; then
        log "Run group: transport=mqtt (brokers=${MQTT_BROKERS}) payload=${p}B pairs=${PAIRS} total_rate=${TOTAL_RATE}/s"
      else
        log "Run: transport=${t} payload=${p}B pairs=${PAIRS} total_rate=${TOTAL_RATE}/s"
      fi
      run_combo "${t}" "${p}" "${PAIRS}" "${TOTAL_RATE}"
      # Optional interval sleep (after full combo; for mqtt this is after all brokers)
      if [[ ${INTERVAL_SEC} -gt 0 ]]; then
        if [[ ${DRY_RUN} -eq 1 ]]; then
          echo "+ sleep ${INTERVAL_SEC}"  # dry-run output only
        else
          sleep ${INTERVAL_SEC}
        fi
      fi
    done
  done

  log "Plotting results to ${PLOTS_DIR}"
  if [[ "${DRY_RUN}" = 1 ]]; then
    echo "+ python3 scripts/plot_results.py --summary ${SUMMARY_CSV} --out-dir ${PLOTS_DIR}"
  else
    python3 "${SCRIPT_DIR}/plot_results.py" --summary "${SUMMARY_CSV}" --out-dir "${PLOTS_DIR}"
  fi

  log "Done. Summary CSV: ${SUMMARY_CSV}"
}

main "$@"
