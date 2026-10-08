#!/usr/bin/env bash
# Запуск/остановка брокеров этапа 2. Всё в user space под ~/bench-tools,
# слушают только 127.0.0.1. Установка — см. README.md, раздел «Этап 2».
#
#   ./brokers.sh start redis|kafka
#   ./brokers.sh stop  redis|kafka|all
#   ./brokers.sh status
set -euo pipefail
T="${BENCH_TOOLS:-$HOME/bench-tools}"
E=$T/env
export PATH="$E/bin:$PATH"
export JAVA_HOME="$E/lib/jvm"
[ -d "$JAVA_HOME" ] || export JAVA_HOME="$E"

redis_start() {
  mkdir -p "$T/redis-data"
  # Без персистентности (save "" / appendonly no): журнал — в памяти,
  # компактор подрезает поток после коммита. maxmemory — защита VM.
  redis-server --bind 127.0.0.1 --port 6379 --protected-mode yes \
    --save "" --appendonly no --maxmemory 3gb --maxmemory-policy noeviction \
    --dir "$T/redis-data" --daemonize yes --pidfile "$T/redis-data/redis.pid" \
    --logfile "$T/redis-data/redis.log"
  for _ in $(seq 50); do redis-cli -h 127.0.0.1 ping >/dev/null 2>&1 && break; sleep 0.1; done
  redis-cli -h 127.0.0.1 ping
}

redis_stop() {
  redis-cli -h 127.0.0.1 shutdown nosave >/dev/null 2>&1 || true
  rm -f "$T/redis-data/redis.pid"
}

kafka_conf() {
  mkdir -p "$T/kafka-config" "$T/kafka-data"
  cat > "$T/kafka-config/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
num.network.threads=3
num.io.threads=8
log.dirs=$T/kafka-data
num.partitions=1
offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
delete.topic.enable=true
log.retention.hours=1
log.segment.bytes=268435456
message.max.bytes=1048576
group.initial.rebalance.delay.ms=0
EOF
}

kafka_start() {
  kafka_conf
  if [ ! -f "$T/kafka-data/meta.properties" ]; then
    "$T/kafka/bin/kafka-storage.sh" format --standalone \
      -t "$("$T/kafka/bin/kafka-storage.sh" random-uuid)" \
      -c "$T/kafka-config/server.properties"
  fi
  export KAFKA_HEAP_OPTS="-Xmx1g -Xms512m"
  export LOG_DIR="$T/kafka-logs"
  "$T/kafka/bin/kafka-server-start.sh" -daemon "$T/kafka-config/server.properties"
  for _ in $(seq 120); do
    "$T/kafka/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1 && { echo kafka up; return; }
    sleep 1
  done
  echo "kafka did not start, see $LOG_DIR" >&2; exit 1
}

kafka_stop() {
  "$T/kafka/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
  for _ in $(seq 60); do pgrep -f 'kafka\.Kafka' >/dev/null || return 0; sleep 1; done
  pkill -9 -f 'kafka\.Kafka' || true
}

status() {
  pgrep -af 'redis-server' || echo "redis: stopped"
  pgrep -f 'kafka\.Kafka' >/dev/null && echo "kafka: running (pid $(pgrep -f 'kafka\.Kafka'))" || echo "kafka: stopped"
}

case "${1:-}-${2:-}" in
  start-redis) redis_start ;;
  start-kafka) kafka_start ;;
  stop-redis) redis_stop ;;
  stop-kafka) kafka_stop ;;
  stop-all|stop-) redis_stop; kafka_stop ;;
  status-*) status ;;
  *) echo "usage: $0 start redis|kafka | stop redis|kafka|all | status" >&2; exit 2 ;;
esac
