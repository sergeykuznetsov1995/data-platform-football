# Общий барьер checkout. Вызывается только под flock автомата (B6 наследует fd 9).
# Журнал намерения пишется ДО UPDATE: SIGKILL не теряет владение паузами.
# При неподтверждённом checkout паузы остаются до восстановления/ручного разбора.
barrier_sql() {
  timeout -k 5 "${METADB_TIMEOUT:-30}" docker exec "$FOTMOB_METADB_CONTAINER" \
    psql -X -q -v ON_ERROR_STOP=1 -U airflow -d airflow -tA -c "$1"
}
barrier_init() {
  BARRIER_JOURNAL="$FOTMOB_STATE_DIR/fotmob-delivery-pauses"
  BARRIER_DAGS="'dag_orchestrate_fotmob','dag_trigger_fotmob_daily','dag_refresh_fotmob','dag_backfill_fotmob','dag_collect_fotmob_players','dag_refresh_fotmob_c1'"
}
barrier_journal_valid() {
  local dag lines
  [ -f "$BARRIER_JOURNAL" ] && [ ! -L "$BARRIER_JOURNAL" ] || return 1
  lines=$(grep -c '^' "$BARRIER_JOURNAL" || true)
  [ "$lines" -ge 5 ] && [ "$lines" -le 6 ] || return 1
  for dag in dag_orchestrate_fotmob dag_trigger_fotmob_daily dag_refresh_fotmob dag_backfill_fotmob dag_collect_fotmob_players; do
    [ "$(grep -cE "^$dag\\|[tf]$" "$BARRIER_JOURNAL")" = 1 ] || return 1
  done
  ! grep -qvE '^dag_(orchestrate_fotmob|trigger_fotmob_daily|refresh_fotmob|backfill_fotmob|collect_fotmob_players|refresh_fotmob_c1)\|[tf]$' "$BARRIER_JOURNAL"
}
barrier_release() {
  local dag paused restored sql=""
  barrier_init
  if [ ! -e "$BARRIER_JOURNAL" ] && [ ! -L "$BARRIER_JOURNAL" ]; then return 0; fi
  barrier_journal_valid || return 1
  while IFS='|' read -r dag paused; do
    case "$dag" in
      dag_orchestrate_fotmob|dag_trigger_fotmob_daily|dag_refresh_fotmob|dag_backfill_fotmob|dag_collect_fotmob_players|dag_refresh_fotmob_c1) ;;
      *) return 1 ;;
    esac
    case "$paused" in
      f) sql+="UPDATE dag SET is_paused=false WHERE dag_id='$dag';" ;;
      t) ;; # Исходную паузу не снимаем.
      *) return 1 ;;
    esac
  done < "$BARRIER_JOURNAL"
  restored=$(barrier_sql "BEGIN; $sql SELECT count(*) FROM dag WHERE dag_id IN ($BARRIER_DAGS) AND is_paused=false; COMMIT;") || return 1
  [ "$restored" = "$(grep -c '|f$' "$BARRIER_JOURNAL" || true)" ] || return 1
  rm -f -- "$BARRIER_JOURNAL"
}
barrier_acquire() {
  local snapshot tmp paused active host_busy host_rc container_rc deadline left dag owned="''"
  barrier_init
  if [ ! -e "$BARRIER_JOURNAL" ] && [ ! -L "$BARRIER_JOURNAL" ]; then
    snapshot=$(barrier_sql "SELECT dag_id, is_paused FROM dag WHERE dag_id IN ($BARRIER_DAGS) ORDER BY dag_id;") || return 1
    # c1 может отсутствовать на откатном релизе. Остальные пять обязательны.
    [ "$(printf '%s\n' "$snapshot" | wc -l)" -ge 5 ] || return 1
    for dag in dag_orchestrate_fotmob dag_trigger_fotmob_daily dag_refresh_fotmob dag_backfill_fotmob dag_collect_fotmob_players; do
      printf '%s\n' "$snapshot" | grep -qxE "$dag\\|[tf]" || return 1
    done
    tmp=$(mktemp "$FOTMOB_STATE_DIR/.fotmob-pauses.XXXXXX") || return 1
    printf '%s\n' "$snapshot" > "$tmp" && mv -T -- "$tmp" "$BARRIER_JOURNAL" || return 1
  fi
  barrier_journal_valid || return 1
  while IFS='|' read -r dag paused; do
    [ "$paused" != f ] || owned+=",'$dag'"
  done < "$BARRIER_JOURNAL"
  deadline=$(($(date -u +%s) + ${BARRIER_WAIT:-120}))
  while :; do
    # Paused DAG не планирует даже finalize текущего DagRun. Замораживаем
    # только завершившихся производителей; живые сохраняют возможность drain.
    # Блокировка dag сериализует паузу с созданием scheduled DagRun. Если
    # queued/running запуск всё же появился (например, ручной), снимаем лишь
    # собственную паузу и повторяем drain. Исходные паузы владельца сохраняем.
    paused=$(barrier_sql "BEGIN; SELECT dag_id FROM dag WHERE dag_id IN ($BARRIER_DAGS) FOR UPDATE; UPDATE dag SET is_paused=true WHERE dag_id IN ($BARRIER_DAGS) AND NOT EXISTS (SELECT 1 FROM dag_run WHERE dag_run.dag_id=dag.dag_id AND state IN ('running','queued')); UPDATE dag SET is_paused=false WHERE dag_id IN ($owned) AND EXISTS (SELECT 1 FROM dag_run WHERE dag_run.dag_id=dag.dag_id AND state IN ('running','queued')); SELECT CASE WHEN count(*) FILTER (WHERE dag_id <> 'dag_refresh_fotmob_c1') = 5 AND bool_and(is_paused) THEN 0 ELSE 1 END FROM dag WHERE dag_id IN ($BARRIER_DAGS); COMMIT;") || return 1
    paused=${paused##*$'\n'}
    case "$paused" in 0|1) ;; *) return 1 ;; esac
    active=$(barrier_sql "SELECT (SELECT count(*) FROM dag_run WHERE dag_id IN ($BARRIER_DAGS,'dag_ingest_fotmob') AND state IN ('running','queued')) + (SELECT count(*) FROM task_instance WHERE dag_id IN ($BARRIER_DAGS,'dag_ingest_fotmob') AND state IN ('running','queued','scheduled','restarting','deferred','up_for_retry','up_for_reschedule'));") || return 1
    host_busy=0
    pgrep -f "dags/scripts/run_fotmob_scrape[r].py|$FOTMOB_CAMPAIGN_DIR/[d]river.sh|run-id fotmob-his[t]-" >/dev/null 2>&1
    host_rc=$?
    [ "$host_rc" -le 1 ] || return 1
    [ "$host_rc" = 1 ] || host_busy=1
    timeout -k 5 "${METADB_TIMEOUT:-30}" docker exec "$FOTMOB_SCHEDULER_CONTAINER" \
      pgrep -f 'run_fotmob_scrape[r].py|run-id fotmob-his[t]-' >/dev/null 2>&1
    container_rc=$?
    [ "$container_rc" -le 1 ] || return 1
    if [ "$paused" = 0 ] && [ "$active" = 0 ] && [ "$host_busy" = 0 ] && [ "$container_rc" = 1 ]; then
      echo 'BARRIER: пауза производителей подтверждена'
      echo 'BARRIER: живая работа завершена'
      return 0
    fi
    left=$((deadline - $(date -u +%s)))
    [ "$left" -gt 0 ] || return 1
    sleep 2
  done
}
barrier_ignore_md5() {
  local source host_sum container_sum
  source=$(timeout -k 5 "${METADB_TIMEOUT:-30}" docker inspect --format \
    '{{range .Mounts}}{{if and (eq .Type "bind") (eq .Destination "/opt/airflow/dags/.airflowignore")}}{{println .Source}}{{end}}{{end}}' \
    "$FOTMOB_SCHEDULER_CONTAINER") || return 1
  [[ "$source" = /* && "$source" != *$'\n'* ]] && [ -f "$source" ] || return 1
  # Эталон обязан быть вне checkout-дерева, включая симлинки.
  case "$(realpath -e "$source")" in "$(realpath -e "$TREE")"/*) return 1 ;; esac
  host_sum=$(md5sum -- "$source") || return 1
  container_sum=$(timeout -k 5 "${METADB_TIMEOUT:-30}" docker exec "$FOTMOB_SCHEDULER_CONTAINER" md5sum /opt/airflow/dags/.airflowignore) || return 1
  host_sum=${host_sum%% *}; container_sum=${container_sum%% *}
  [[ "$host_sum" =~ ^[0-9a-f]{32}$ ]] && [ "$host_sum" = "$container_sum" ] || return 1
  printf '%s\n' "$container_sum"
}
barrier_checkout() {
  local before after hm hms safe_left
  if [ "$(realpath -m "$TREE")" != "$(realpath -m "$FOTMOB_RELEASE_ROOT")" ]; then
    git -C "$TREE" checkout "$@"
    return $?
  fi
  barrier_acquire || { echo 'BARRIER: отказ, checkout запрещён' >&2; return 1; }
  if [ "$1" = -B ]; then
    # Drain расходует окно: повторная сверка непосредственно перед apply.
    hm=$(date -u +%H%M)
    hms=$(date -u +%H%M%S)
    safe_left=$((86400 - 10#${hms:0:2} * 3600 - 10#${hms:2:2} * 60 - 10#${hms:4:2} - 1800))
    [ "$hm" -ge 1945 ] && [ "$hm" -le 2320 ] && [ "$safe_left" -gt 0 ] || return 1
  fi
  before=$(barrier_ignore_md5) || return 1
  git -C "$TREE" checkout "$@" || return 1
  after=$(barrier_ignore_md5) || return 1
  [ "$before" = "$after" ] || return 1
  echo "BARRIER: AIRFLOWIGNORE md5 до/после checkout = $after"
}
