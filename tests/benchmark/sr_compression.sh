#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# sr_compression_benchmark.sh
#
# Compare StarRocks LZ4 vs ZSTD table compression on the BASE comments data
# (the lingua/main-table rows that carry the big `body` text column — where
# the codec choice actually matters; float score columns barely compress).
#
# Each codec is tested in FULL ISOLATION, one at a time, on the same freed
# disk state:
#     create -> load -> compact -> measure size -> query suite -> CPU
#     -> DROP ... FORCE -> drain trash back to baseline
#     -> repeat for the next codec
#
# This keeps the two runs symmetric: identical page-cache headroom (only one
# table resident at a time), identical NVMe fill state, and lower peak disk.
# The single reused table name means the only variable between runs is the
# compression property.
#
# Data is copied from the existing main `comments` table (byte-identical,
# already typed — no parquet cast games). Warm is the primary lens: StarRocks
# caches COMPRESSED blocks, so decompression CPU is paid on every read.
#
# Prereqs: run AFTER the trash cleanup (needs headroom for one ~150-200GB
# copy at a time). Prompts for the StarRocks root password (not read from the
# environment, so it stays out of shell history / the process table).
#
#   ./sr_compression_benchmark.sh
# ---------------------------------------------------------------------------
set -euo pipefail

CONTAINER=${CONTAINER:-sdp-starrocks-1}
DB=${DB:-reddit}
SRC=${SRC:-comments}                       # source main table (schema + data)
MONTHS=${MONTHS:-"'2026-02','2026-03','2026-04','2026-05'"}
BENCH=${BENCH:-bench_compression}          # single reused table (only codec differs)
CODECS=${CODECS:-"LZ4 ZSTD"}
REPS=${REPS:-6}                            # timed reps per query (median)
CPU_ITERS=${CPU_ITERS:-4}                  # iterations for the CPU measurement
BE_HTTP=${BE_HTTP:-8040}

read -rs -p "StarRocks root password: " SR_PW; echo
[ -n "$SR_PW" ] || { echo "  no password entered — aborting" >&2; exit 1; }

sr()   { docker exec -e MYSQL_PWD="$SR_PW" "$CONTAINER" mysql -h127.0.0.1 -P9030 -uroot -N -e "$1"; }
srdb() { docker exec -e MYSQL_PWD="$SR_PW" "$CONTAINER" mysql -h127.0.0.1 -P9030 -uroot -D"$DB" -N -e "$1"; }
human(){ numfmt --to=iec --suffix=B "$1" 2>/dev/null || echo "$1"; }

# --- Column list = main comments schema, only compression differs -----------
COLS='`id` varchar(7) NOT NULL,
  `dataset` char(7) NULL, `retrieved_utc` int NULL, `created_utc` int NULL,
  `link_id` varchar(10) NULL, `parent_id` varchar(10) NULL, `score` int NULL,
  `controversiality` int NULL, `total_awards_received` int NULL,
  `subreddit` varchar(1048576) NULL, `stickied` boolean NULL, `gilded` int NULL,
  `distinguished` varchar(1048576) NULL, `is_deleted` boolean NULL,
  `removal_type` varchar(1048576) NULL, `author` varchar(1048576) NULL,
  `author_flair_text` varchar(1048576) NULL, `author_created_utc` int NULL,
  `is_submitter` boolean NULL, `body` varchar(1048576) NULL,
  `lang` varchar(2) NULL, `lang_prob` float NULL, `lang2` varchar(2) NULL,
  `lang2_prob` float NULL, `lang_chars` int NULL'

create_table() {  # $1=codec ; (re)create $BENCH
  srdb "DROP TABLE IF EXISTS $BENCH FORCE;"
  srdb "CREATE TABLE $BENCH ( $COLS )
        PRIMARY KEY(\`id\`) DISTRIBUTED BY HASH(\`id\`) BUCKETS 256
        PROPERTIES(\"compression\"=\"$1\",\"enable_persistent_index\"=\"true\",\"replication_num\"=\"1\");"
}

phys_size() {  # physical (compressed) bytes of $BENCH, summed across tablets
  local tid
  tid=$(sr "SELECT TABLE_ID FROM information_schema.tables_config WHERE TABLE_SCHEMA='$DB' AND TABLE_NAME='$BENCH'")
  sr "SELECT IFNULL(SUM(DATA_SIZE),0) FROM information_schema.be_tablets WHERE TABLE_ID=$tid"
}

trash_bytes() { docker exec "$CONTAINER" sh -c 'du -sb /data/deploy/starrocks/be/storage*/trash 2>/dev/null | awk "{s+=\$1} END{print s+0}"'; }
cpu_stat()    { docker exec "$CONTAINER" awk '/usage_usec/{print $2}' /sys/fs/cgroup/cpu.stat 2>/dev/null; }

wait_compaction() {  # poll physical size until it stops shrinking (>1%)
  local prev=-1 cur
  for _ in $(seq 20); do
    cur=$(phys_size); printf "   compacting: %s\n" "$(human "$cur")"
    if [ "$prev" -ge 0 ] && [ "$cur" -ge "$(( prev - prev/100 ))" ]; then break; fi
    prev=$cur; sleep 60
  done
}

reclaim() {  # DROP FORCE + purge BE trash back to baseline so the disk is truly freed
  local base tb; base=$(trash_bytes)
  srdb "DROP TABLE IF EXISTS $BENCH FORCE;"
  docker exec "$CONTAINER" curl -s -XPOST "http://127.0.0.1:$BE_HTTP/api/update_config?trash_file_expire_time_sec=60" >/dev/null || true
  echo "   draining trash after DROP (baseline $(human "$base"))..."
  for _ in $(seq 25); do
    sleep 45; tb=$(trash_bytes); printf "     trash=%s\n" "$(human "$tb")"
    [ "$tb" -le "$(( base + 10000000000 ))" ] && break
  done
}

# --- Query suite (%T -> db.table), ordered by decompression pressure --------
declare -A Q=(
  [q1_group_score]="SELECT subreddit,count(*) c,AVG(score) FROM %T GROUP BY subreddit ORDER BY c DESC LIMIT 20"
  [q2_body_scan]="SELECT AVG(length(body)),SUM(length(body)) FROM %T"
  [q3_timeseries]="SELECT retrieved_utc DIV 2592000 m,count(*) FROM %T GROUP BY m ORDER BY m"
  [q4_lang_filter]="SELECT count(*) FROM %T WHERE lang='en'"
  [q5_body_topscore]="SELECT id,body FROM %T WHERE score>5000 LIMIT 100"
  [q6_subreddit_filter]="SELECT count(*) FROM %T WHERE subreddit='AskReddit'"
  [q7_point_lookup]="SELECT * FROM %T WHERE id=(SELECT id FROM %T LIMIT 1)"
)
ORDER=(q1_group_score q2_body_scan q3_timeseries q4_lang_filter q5_body_topscore q6_subreddit_filter q7_point_lookup)

median_ms() {  # $1=sql-with-%T -> median warm ms over REPS
  local sql=${1//%T/$DB.$BENCH} t0 t1; local times=()
  srdb "$sql" >/dev/null 2>&1 || true                         # warmup
  for _ in $(seq "$REPS"); do
    t0=$(date +%s.%N); srdb "$sql" >/dev/null 2>&1 || true; t1=$(date +%s.%N)
    times+=( "$(echo "($t1-$t0)*1000" | bc)" )
  done
  printf '%s\n' "${times[@]}" | sort -n | awk '{a[NR]=$1} END{print (NR%2)?a[(NR+1)/2]:(a[NR/2]+a[NR/2+1])/2}'
}

cpu_per_query() {  # cpu-seconds per body-scan query (decompression cost)
  local sql="SELECT AVG(length(body)),SUM(length(body)) FROM $DB.$BENCH" u0 u1
  u0=$(cpu_stat); for _ in $(seq "$CPU_ITERS"); do srdb "$sql" >/dev/null 2>&1 || true; done; u1=$(cpu_stat)
  [ -n "$u0" ] && [ -n "$u1" ] && echo "scale=2; ($u1-$u0)/1000000/$CPU_ITERS" | bc || echo "n/a"
}

# --- Run each codec in isolation --------------------------------------------
declare -A SIZE CPU LAT
for codec in $CODECS; do
  echo; echo "########## $codec ##########"
  echo "== create + load $MONTHS from $DB.$SRC"
  create_table "$codec"
  t0=$(date +%s); srdb "INSERT INTO $BENCH SELECT * FROM $SRC WHERE dataset IN ($MONTHS);"
  echo "   loaded $(sr "SELECT count(*) FROM $DB.$BENCH") rows in $(( $(date +%s)-t0 ))s"
  wait_compaction
  SIZE[$codec]=$(phys_size)
  echo "== physical size: $(human "${SIZE[$codec]}")"
  echo "== warm query latency (median of $REPS)"
  for q in "${ORDER[@]}"; do
    LAT[$codec:$q]=$(median_ms "${Q[$q]}")
    printf "   %-22s %.1f ms\n" "$q" "${LAT[$codec:$q]}"
  done
  CPU[$codec]=$(cpu_per_query); echo "== body-scan CPU: ${CPU[$codec]}s/query"
  reclaim
done

# --- Comparison -------------------------------------------------------------
set -- $CODECS; A=$1; B=$2
echo; echo "===================== RESULTS ($A vs $B) ====================="
printf "STORAGE  %-8s %s\n" "$A" "$(human "${SIZE[$A]}")"
printf "STORAGE  %-8s %s   (%s%% of %s)\n" "$B" "$(human "${SIZE[$B]}")" \
       "$(echo "scale=1; ${SIZE[$B]}*100/${SIZE[$A]}" | bc)" "$A"
echo
printf "%-22s %10s %10s %8s\n" query "${A}_ms" "${B}_ms" "${B}/${A}"
for q in "${ORDER[@]}"; do
  printf "%-22s %10.1f %10.1f %7sx\n" "$q" "${LAT[$A:$q]}" "${LAT[$B:$q]}" \
         "$(echo "scale=2; ${LAT[$B:$q]}/${LAT[$A:$q]}" | bc 2>/dev/null || echo -)"
done
echo
printf "body-scan CPU/query   %-8s %ss\n" "$A" "${CPU[$A]}"
printf "body-scan CPU/query   %-8s %ss   (the decompression tax; weigh vs storage win)\n" "$B" "${CPU[$B]}"
echo
echo "Test table already dropped + trash drained. Nothing to clean up."
