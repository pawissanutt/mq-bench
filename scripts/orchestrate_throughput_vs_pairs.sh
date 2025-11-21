#!/usr/bin/env bash
set -euo pipefail

# Orchestrate throughput-vs-pairs experiment across transports.
#
# Topology: N publishers → N subscribers (1:1 pair per topic)
# Workload: fixed payload; sweep N in a configured list; either constant per-publisher rate or constant total rate.
# Runner: uses scripts/run_multi_topic_perkey.sh for N paired topics
# Output: results/throughput_vs_pairs_<ts>/{raw_data,plots}/ with summary.csv and figures
#
# Usage examples:
#   # Default: payload=1MB, pairs list "10 100 300 1000 3000 5000 10000", rate-per-pub=1 msg/s
#   scripts/orchestrate_throughput_vs_pairs_v2.sh
#
#   # Keep total offered rate fixed at 5000 msg/s across N
#   scripts/orchestrate_throughput_vs_pairs_v2.sh --total-rate 5000
#
#   # Use 2 msg/s per publisher and limit to specific transports
#   scripts/orchestrate_throughput_vs_pairs_v2.sh --rate-per-pub 2 --transports "zenoh nats"
#
#   # Remote host for all brokers (keep default ports)
#   scripts/orchestrate_throughput_vs_pairs_v2.sh --host 192.168.0.254
#
#   # MQTT multi-broker support (like orchestrate_benchmarks.sh):
#   scripts/orchestrate_throughput_vs_pairs_v2.sh --transports "mqtt" --mqtt-brokers "mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884"
#
#   # Append new runs into an existing summary:
#   scripts/orchestrate_throughput_vs_pairs_v2.sh --summary results/throughput_vs_pairs_YYYYMMDD_HHMMSS/raw_data/summary.csv --transports "redis" --pairs-list "10 100"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
PAIRS_LIST=(10 100 300 1000 3000 5000 10000)
PAYLOAD_TOKEN="1MB"           # accepts KB/MB suffix; converted to bytes
RATE_PER_PUB="1"              # msgs/s per publisher (if set)
TOTAL_RATE=""                 # alternative: fixed total offered rate (msgs/s)
DURATION=30
SNAPSHOT=5
RUN_ID_PREFIX="scale"
TRANSPORTS=(zenoh zenoh-mqtt redis nats rabbitmq mqtt)
DEFAULT_TRANSPORTS=(zenoh zenoh-mqtt redis nats rabbitmq mqtt)
START_SERVICES=0
DRY_RUN=${DRY_RUN:-0}
HOST="${HOST:-}"
SUMMARY_OVERRIDE="${SUMMARY_OVERRIDE:-}"
INTERVAL_SEC=15

# MQTT brokers (name:host:port). Default to local compose ports.
MQTT_BROKERS="mosquitto:127.0.0.1:1883 emqx:127.0.0.1:1884 hivemq:127.0.0.1:1885 rabbitmq:127.0.0.1:1886 artemis:127.0.0.1:1887"
declare -a MQTT_BROKERS_ARR=()

# Convert tokens with KB/MB suffix to bytes
to_bytes() {
  local tok="$1"
  if [[ "$tok" =~ ^[0-9]+$ ]]; then echo "$tok"; return 0; fi
  if [[ "$tok" =~ ^([0-9]+)[Kk][Bb]?$ ]]; then echo $(( ${BASH_REMATCH[1]} * 1024 )); return 0; fi
  if [[ "$tok" =~ ^([0-9]+)[Mm][Bb]?$ ]]; then echo $(( ${BASH_REMATCH[1]} * 1024 * 1024 )); return 0; fi
  if [[ "$tok" =~ ^([0-9]+)[bB]$ ]]; then echo "${BASH_REMATCH[1]}"; return 0; fi
  echo "$tok"
}

is_quote_only_token() {
  local v="$1"
  case "$v" in
    "\"\""|"''"|"“”"|"””"|"‘‘"|"’’") return 0;;
    *) return 1;;
  esac
}

timestamp() { date +%Y%m%d_%H%M%S; }
TS=""
BENCH_DIR=""
RAW_DIR=""
PLOTS_DIR=""
SUMMARY_CSV=""

init_dirs() {
  if [[ -z "${TS}" ]]; then TS="$(timestamp)"; fi
  if [[ -z "${BENCH_DIR}" ]]; then BENCH_DIR="${REPO_ROOT}/results/throughput_vs_pairs_${TS}"; fi
  if [[ -z "${RAW_DIR}" ]]; then RAW_DIR="${BENCH_DIR}/raw_data"; fi
  if [[ -z "${PLOTS_DIR}" ]]; then PLOTS_DIR="${BENCH_DIR}/plots"; fi
  case "${RAW_DIR}" in /*) ;; *) RAW_DIR="${REPO_ROOT}/${RAW_DIR}" ;; esac
  case "${PLOTS_DIR}" in /*) ;; *) PLOTS_DIR="${REPO_ROOT}/${PLOTS_DIR}" ;; esac
  mkdir -p "${RAW_DIR}" "${PLOTS_DIR}"
  if [[ -n "${SUMMARY_OVERRIDE}" ]]; then
    if [[ -d "${SUMMARY_OVERRIDE}" || "${SUMMARY_OVERRIDE}" != *.csv ]]; then
      local dir="${SUMMARY_OVERRIDE}"; case "${dir}" in /*) ;; *) dir="${REPO_ROOT}/${dir}" ;; esac
      mkdir -p "${dir}"; SUMMARY_CSV="${dir}/summary.csv"
    else
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
  sed -n '1,120p' "$0" | sed -n 's/^# //p'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pairs-list)
      shift; IFS=' ' read -r -a PAIRS_LIST <<<"${1:-}" ;;
    --payload)
      shift; PAYLOAD_TOKEN=${1:-1MB} ;;
    --rate-per-pub)
      shift; RATE_PER_PUB=${1:-} ;;
    --total-rate)
      shift; TOTAL_RATE=${1:-} ;;
    --duration)
      shift; DURATION=${1:-30} ;;
    --snapshot)
      shift; SNAPSHOT=${1:-5} ;;
    --transports)
      shift; if [[ -n "${1:-}" ]]; then IFS=' ' read -r -a TRANSPORTS <<<"${1}"; fi ;;
    --run-id-prefix)
      shift; RUN_ID_PREFIX=${1:-scale} ;;
    --host)
      shift; HOST=${1:-} ;;
    --start-services)
      START_SERVICES=1 ;;
    --dry-run)
      DRY_RUN=1 ;;
    --summary)
      shift; SUMMARY_OVERRIDE=${1:-}; if is_quote_only_token "${SUMMARY_OVERRIDE}"; then SUMMARY_OVERRIDE=""; fi ;;
    --out-dir)
      shift; PLOTS_DIR=${1:-}; if is_quote_only_token "${PLOTS_DIR}"; then PLOTS_DIR=""; fi ;;
    --raw-dir)
      shift; RAW_DIR=${1:-}; if is_quote_only_token "${RAW_DIR}"; then RAW_DIR=""; fi ;;
    --bench-dir)
      shift; BENCH_DIR=${1:-}; if is_quote_only_token "${BENCH_DIR}"; then BENCH_DIR=""; fi ;;
    --mqtt-brokers)
      shift; MQTT_BROKERS=${1:-} ;;
    --interval-sec)
      shift; INTERVAL_SEC=${1:-0} ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
  shift || true
done

init_dirs

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

# Resolve payload bytes
PAYLOAD_BYTES="$(to_bytes "${PAYLOAD_TOKEN}")"
if [[ ! "${PAYLOAD_BYTES}" =~ ^[0-9]+$ ]]; then
  echo "[error] Invalid --payload: ${PAYLOAD_TOKEN}" >&2; exit 2
fi

# Guardrails
if [[ ${#TRANSPORTS[@]} -eq 0 ]]; then TRANSPORTS=("${DEFAULT_TRANSPORTS[@]}"); fi

# Materialize MQTT brokers array and apply HOST rewrite if needed
IFS=' ' read -r -a MQTT_BROKERS_ARR <<<"${MQTT_BROKERS}"
if [[ -n "${HOST}" ]]; then
  declare -a _REWRITTEN=()
  for tok in "${MQTT_BROKERS_ARR[@]}"; do IFS=: read -r bname _bhost bport <<<"${tok}"; _REWRITTEN+=("${bname}:${HOST}:${bport}"); done
  MQTT_BROKERS_ARR=("${_REWRITTEN[@]}")
fi

# Header
if [[ ! -s "${SUMMARY_CSV}" ]]; then
  echo "transport,payload,rate,run_id,sub_tps,p50_ms,p95_ms,p99_ms,pub_tps,sent,recv,errors,artifacts_dir,max_cpu_perc,max_mem_perc,max_mem_used_bytes" > "${SUMMARY_CSV}"
fi

append_summary_from_artifacts() {
  local transport="$1" payload="$2" total_rate="$3" run_id="$4" art_dir="$5"
  local sub_csv pub_csv last_sub last_pub
  sub_csv="${art_dir}/sub.csv"
  pub_csv="${art_dir}/pub.csv"
  if [[ ! -f "${sub_csv}" ]]; then
    if [[ -f "${art_dir}/sub_agg.csv" ]]; then sub_csv="${art_dir}/sub_agg.csv"; else log "WARN: Missing subscriber CSV in ${art_dir}"; return 0; fi
  fi
  if [[ ! -f "${pub_csv}" ]]; then if [[ -f "${art_dir}/mt_pub.csv" ]]; then pub_csv="${art_dir}/mt_pub.csv"; fi; fi
  last_sub=$(tail -n +2 "${sub_csv}" 2>/dev/null | tail -n1 || true)
  last_pub=$(tail -n +2 "${pub_csv}" 2>/dev/null | tail -n1 || true)
  if [[ -z "${last_sub}" ]]; then log "WARN: No subscriber data for ${run_id}"; return 0; fi
  
  # Calculate average instantaneous throughput from the 3 middle rows to capture steady state
  # Columns: timestamp,sent,recv,err,total_tps,interval_tps,...
  # We want column 6 (interval_tps)
  local avg_tps
  avg_tps=$(awk -F, '
    NR>1 && NF>=6 { # Skip header and ensure enough columns
        rows[count++] = $6 
    }
    END {
        if (count == 0) { print "0.00"; exit }
        if (count <= 3) {
            for (i=0; i<count; i++) sum += rows[i]
            printf "%.2f", sum/count
        } else {
            # Pick 3 rows from the middle
            start = int((count - 3) / 2)
            for (i=start; i<start+3; i++) sum += rows[i]
            printf "%.2f", sum/3
        }
    }
  ' "${sub_csv}")

  local _ts _sent recv _err tps _it p50 p95 p99 _min _max _mean
  IFS=, read -r _ts _sent recv _err tps _it p50 p95 p99 _min _max _mean <<<"${last_sub}"
  
  # Use the averaged instantaneous TPS instead of the final cumulative TPS
  tps="${avg_tps}"

  local pub_tps sent; pub_tps=""; sent="${_sent}"
  if [[ -n "${last_pub}" ]]; then
    local _pts psent _prev _perr ptt _pit _a _b _c _d _e _f
    IFS=, read -r _pts psent _prev _perr ptt _pit _a _b _c _d _e _f <<<"${last_pub}"
    pub_tps="${ptt}"; sent="${psent}"
  fi
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
      NR==1 { 
        # Detect format: remote collector (5 cols) vs local (many cols)
        is_remote=(NF==5 && $3=="cpu_perc");
        has_memprec=(NF>=12); has_cpuprec=(NF>=17); 
        next 
      }
      {
        if (is_remote) {
           # Remote format: timestamp,container,cpu_perc,mem_perc,mem_usage
           c=$3; gsub(/%/,"",c); c+=0
           perc=$4; gsub(/%/,"",perc); perc+=0
           used_str=$5; 
           # Parse mem usage like "12.5MiB / 1.2GiB" -> take first part
           split(used_str, parts, " ");
           used_part = parts[1];
           
           val=used_part+0; 
           unit=used_part; gsub(/[0-9.]/,"",unit);
           
           if (index(unit,"Gi")>0) val*=1024*1024*1024;
           else if (index(unit,"Mi")>0) val*=1024*1024;
           else if (index(unit,"Ki")>0) val*=1024;
           used_b=val
        } else {
           # Local format
           if (has_cpuprec) { c=$17+0 } else { c=$4; gsub(/%/,"",c); c+=0 }
           if (has_memprec) {
             used_b=$10+0; tot_b=$11+0; if (used_b>mused) mused=used_b; perc=$12+0; if (perc==0 && tot_b>0) perc=(used_b/tot_b)*100.0; 
           }
        }
        
        if (c>mcpu) mcpu=c
        if (perc>mperc) mperc=perc
        if (used_b>mused) mused=used_b
      }
      END{ if(mcpu=="")mcpu=0; if(mperc=="")mperc=0; if(mused=="")mused=0; printf("%.6f,%.6f,%.0f", mcpu,mperc,mused) }
    ' "${stats_csv}" || true)
    IFS=, read -r max_cpu max_mem_perc max_mem_used <<<"${agg}"
  fi

  echo "${transport},${payload},${total_rate},${run_id},${tps},${p50_ms},${p95_ms},${p99_ms},${pub_tps},${sent},${recv},${_err},${art_dir},${max_cpu},${max_mem_perc},${max_mem_used}" >> "${SUMMARY_CSV}"
}

run_combo() {
  local transport="$1" pairs="$2" payload="$3" per_pub_rate="$4" total_rate="$5"
  local rid="${RUN_ID_PREFIX}_$(timestamp)_${transport}_p${payload}_n${pairs}_r${per_pub_rate}"
  local art_dir="${REPO_ROOT}/artifacts/${rid}/fanout_multi_topic_perkey"
  local env_common
  env_common="TENANTS=${pairs} REGIONS=1 SERVICES=1 SHARDS=1 SUBSCRIBERS=${pairs} PUBLISHERS=${pairs} PAYLOAD=${payload} DURATION=${DURATION} SNAPSHOT=${SNAPSHOT} TOPIC_PREFIX=bench/pair"
  if [[ -n "${per_pub_rate}" ]]; then env_common+=" RATE=${per_pub_rate}"; fi

  local host_env=""
  local remote_stats_pid=""
  local stats_csv="${art_dir}/docker_stats.csv"

  # Start remote stats collector if HOST is set
  if [[ -n "${HOST}" ]]; then
    mkdir -p "${art_dir}"
    log "Starting remote stats collector on ${HOST}..."
    "${SCRIPT_DIR}/collect_remote_docker_stats.sh" "${HOST}" "${stats_csv}" "${DURATION}" >/dev/null 2>&1 &
    remote_stats_pid=$!
  fi

  case "${transport}" in
    zenoh)
      if [[ -n "${HOST}" ]]; then host_env="ENDPOINT_SUB=tcp/${HOST}:7447 ENDPOINT_PUB=tcp/${HOST}:7448"; fi
      run "ENGINE=zenoh ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\"" ;;
    zenoh-mqtt)
      # Mixed engines via bridge: subscribers over MQTT on 1888 (bridge), publishers over zenoh on 7448
      if [[ -n "${HOST}" ]]; then
        host_env="ENGINE_SUB=mqtt MQTT_HOST=${HOST} MQTT_PORT=1888 ENGINE_PUB=zenoh ENDPOINT_PUB=tcp/${HOST}:7448"
      else
        host_env="ENGINE_SUB=mqtt MQTT_HOST=127.0.0.1 MQTT_PORT=1888 ENGINE_PUB=zenoh ENDPOINT_PUB=tcp/127.0.0.1:7448"
      fi
      run "${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\"" ;;
    redis)
      if [[ -n "${HOST}" ]]; then host_env="REDIS_URL=redis://${HOST}:6379"; fi
      run "ENGINE=redis ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\"" ;;
    nats)
      if [[ -n "${HOST}" ]]; then host_env="NATS_HOST=${HOST}"; fi
      run "ENGINE=nats ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\"" ;;
    rabbitmq)
      if [[ -n "${HOST}" ]]; then host_env="RABBITMQ_HOST=${HOST}"; fi
      run "ENGINE=rabbitmq ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${rid}\"" ;;
    mqtt)
      if [[ ${#MQTT_BROKERS_ARR[@]} -eq 0 ]]; then
        log "WARN: No MQTT brokers defined; skipping"
      else
        for b in "${MQTT_BROKERS_ARR[@]}"; do
          IFS=: read -r bname bhost bport <<<"${b}"
          local br_transport="mqtt_${bname}"
          local br_rid="${RUN_ID_PREFIX}_$(timestamp)_${br_transport}_p${payload}_n${pairs}_r${per_pub_rate}"
          local br_art_dir="${REPO_ROOT}/artifacts/${br_rid}/fanout_multi_topic_perkey"
          host_env="MQTT_HOST=${bhost} MQTT_PORT=${bport}"
          
          # For MQTT multi-broker, we might need per-broker stats collection if they are on different hosts.
          # Assuming single HOST for now or local. If HOST is set, we already started one collector above.
          # If brokers are on different hosts, this simple logic won't cover all of them.
          
          run "ENGINE=mqtt ${host_env} ${env_common} bash \"${SCRIPT_DIR}/run_multi_topic_perkey.sh\" \"${br_rid}\""
          append_summary_from_artifacts "${br_transport}" "${payload}" "${total_rate}" "${br_rid}" "${br_art_dir}"
        done
      fi
      # Kill remote stats if started
      if [[ -n "${remote_stats_pid}" ]]; then kill "${remote_stats_pid}" 2>/dev/null || true; wait "${remote_stats_pid}" 2>/dev/null || true; fi
      return 0 ;;
    *) log "Unknown transport: ${transport}"; return 1 ;;
  esac

  # Kill remote stats if started
  if [[ -n "${remote_stats_pid}" ]]; then kill "${remote_stats_pid}" 2>/dev/null || true; wait "${remote_stats_pid}" 2>/dev/null || true; fi

  append_summary_from_artifacts "${transport}" "${payload}" "${total_rate}" "${rid}" "${art_dir}"
}

main() {
  init_dirs
  log "Throughput vs pairs benchmark → ${BENCH_DIR}"
  ensure_services

  # Resolve per-run parameters
  local payload_bytes="${PAYLOAD_BYTES}"
  log "Resolved: SUMMARY_CSV=${SUMMARY_CSV} | PLOTS_DIR=${PLOTS_DIR} | RAW_DIR=${RAW_DIR}"
  log "Resolved payload (bytes): ${payload_bytes}"
  log "Pairs list: ${PAIRS_LIST[*]} | rate-per-pub=${RATE_PER_PUB:-} | total-rate=${TOTAL_RATE:-}"

  for t in "${TRANSPORTS[@]}"; do
    for n in "${PAIRS_LIST[@]}"; do
      local per_rate total
      if [[ -n "${RATE_PER_PUB}" ]]; then
        per_rate="${RATE_PER_PUB}"; total=$(( n * per_rate ))
      elif [[ -n "${TOTAL_RATE}" ]]; then
        total="${TOTAL_RATE}"; per_rate=$(( total / n ))
      else
        per_rate=1; total=$(( n * per_rate ))
      fi
      if [[ "${t}" == "mqtt" ]]; then
        log "Run group: transport=mqtt (brokers=${MQTT_BROKERS}) pairs=${n} payload=${payload_bytes}B per_pub_rate=${per_rate}/s total_rate=${total}/s"
      else
        log "Run: transport=${t} pairs=${n} payload=${payload_bytes}B per_pub_rate=${per_rate}/s total_rate=${total}/s"
      fi
      run_combo "${t}" "${n}" "${payload_bytes}" "${per_rate}" "${total}"
      if [[ ${INTERVAL_SEC} -gt 0 ]]; then
        if [[ ${DRY_RUN} -eq 1 ]]; then echo "+ sleep ${INTERVAL_SEC}"; else sleep ${INTERVAL_SEC}; fi
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
