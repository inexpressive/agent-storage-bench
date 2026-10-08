#!/usr/bin/env bash
# Ночной прогон этапа 3 к докладу (6→7.10). ~1,5 ч. Лог — results/night-2026-10-06.log.
# Запуск: nohup ./night-2026-10-06.sh &   Мак не должен спать (caffeinate -i).
set -uo pipefail
cd "$(dirname "$0")"
C=~/bench-state/tmp; B=./tmp-db; PY=~/bench-tools/env/bin/python
R=results/2026-10-06; mkdir -p "$C" "$B"
step() { echo "=== $(date '+%F %T') $*"; }
# Урок 2: потерянные обновления — ФС контейнера и папка мака
step rmw container
python3 run_matrix.py --fs container --dir $C --configs json-shared-rmw-none,json-shared-rmw-excl --ns 2,5,10,20,50 --out $R-container-rmw
step rmw bind
python3 run_matrix.py --fs macos-bind --dir $B --configs json-shared-rmw-none,json-shared-rmw-excl --ns 2,5,10 --out $R-macos-bind-rmw
# Урок 3: тихая потеря на папке мака, три повтора
for r in 1 2 3; do
  step silent bind r$r
  python3 run_matrix.py --fs macos-bind --dir $B --configs one-file,file-per-session --ns 1,5,10,20 --out $R-macos-bind-silent/r$r
done
# Урок 1: повторы ключевых точек на ФС контейнера
for r in 1 2 3; do
  step repeats container r$r
  python3 run_matrix.py --fs container --dir $C --configs one-file,file-per-session --ns 20,50,100,150,200 --out $R-container-repeats/r$r
done
# H4: autocheckpoint
step h4
python3 run_matrix.py --fs container --dir $C --configs one-file --ns 20,50,100 --wal-autocheckpoints 0,1000,10000 --out $R-container-h4
# H5: настройки продюсера Kafka (+ Redis для сравнения)
./brokers.sh start kafka
step h5 kafka
$PY run_matrix.py --fs container --dir $C --configs event-log-kafka --ns 20,100,200 --kafka-acks 1,all --kafka-linger-ms 0,5 --kafka-wait each,async --out $R-container-h5-kafka
./brokers.sh stop all; ./brokers.sh start redis
step h5 redis
$PY run_matrix.py --fs container --dir $C --configs event-log-redis --ns 20,100,200 --out $R-container-h5-redis
./brokers.sh stop all
step done
