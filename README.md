# itmo-bench — где упирается запись стриминг-вывода агентов

> **Исследовательский код доклада** на ITMO OpenSource, 29.10.2026. Цифры
> в докладе и в `results/` получены этим кодом — тег `v0.1-itmo`. Тестов нет,
> раскладка — папка скриптов. Переделка в нормальный проект — после доклада;
> тег останется, чтобы цифры можно было воспроизвести тем же кодом.

Бенчмарк к докладу на ITMO OpenSource, 29.10.2026. Вопрос: что происходит
с хранилищем исследовательской агентной среды, когда агентов становится
много и каждый часто сбрасывает стриминг-вывод на диск.

- **Синтетика** (`bench.py`, `eventlog.py`, `run_matrix.py`) — шаблон записи
  OpenResearch и OpenScience, N процессов; итоги — `results/SUMMARY.md`,
  отчёт с графиками — `results/report.html`.
- **Уровень 1** (`real/`) — код хранилища самих сред (OpenResearch,
  OpenScience, OpenHuman) без LLM; итоги — `results/REAL-SUMMARY.md`,
  установка — `real/README.md`.
- В `results/*/` — сводные `summary.csv`/`summary.json` каждой матрицы;
  сырые посбросовые логи в репозиторий не входят.

Нужен Python 3.10+, синтетика этапа 1 — только стандартная библиотека.
Рабочий каталог баз — любой (`--dir`); ФС определяется им, `--fs` — метка.
Лицензия — MIT.

Синтетический бенчмарк к докладу на ITMO OpenSource (29.10.2026) «Десять
агентов, один файл: где исследовательские среды хранят состояние».
Этап 1 — только SQLite и файлы, без брокера. Этап 2 — журнал событий через
брокер (Redis Streams, Kafka) и один компактор, см. раздел «Этап 2» ниже.

## Что моделируем

N агентов = N отдельных процессов (`multiprocessing`, fork). Каждый — одна
стриминговая сессия: сообщение растёт, и агент периодически сбрасывает его
в хранилище **целиком** (не дельтой).

| Параметр | Значение по умолчанию | Откуда |
|---|---|---|
| Интервал сброса | 75 мс | OpenResearch `FLUSH_INTERVAL` (`src/local/chat/mod.rs:34`) |
| Что пишется | весь объект сообщения, `UPDATE` своей строки | там же, `maybe_flush` / `upsert_chat_message_with` |
| PRAGMA | WAL, `busy_timeout=5000`, `synchronous=NORMAL` | OpenResearch `store.rs` (WAL + 5000); `synchronous=NORMAL` — как в OpenScience experiments, у OR не указано |
| Новый ход | `INSERT` нового сообщения каждые 160 сбросов (~12 с) | допущение, параметр `--turn-every` |
| Размер сообщения | старт 1 КБ, +200–500 Б за сброс, потолок 64 КБ | допущение; потолок по духу `TOOL_TEXT_CAP = 16_000` на tool-часть |
| JSON-режим | lock-файл `O_EXCL`, tmp-файл, `os.replace`; 250 мс | OpenScience `storage.ts`, `createCoalescer(..., 250)` |

Режимы:

- `one-file` — все процессы пишут в один `orx.db` (как OpenResearch).
- `one-file-append` — та же общая SQLite и PRAGMA, но после начала хода
  каждый сброс добавляет в таблицу `chunks` только новые байты сообщения.
  Это проверка гипотезы, что запись дельт вместо полного текста снизит
  стоимость частых перезаписей. Чанк и `updated_at` сообщения пишутся одной
  транзакцией (`BEGIN IMMEDIATE`) — один коммит на сброс, как у `one-file`.
  После прогона проверяется `integrity_check` и совпадение склейки чанков
  последнего сообщения с последним записанным текстом каждого агента
  (`append_state_mismatches`).
- `one-file-threads` — схема `one-file`, но агенты работают потоками одного
  процесса и у каждого своё соединение SQLite, созданное в этом потоке.
  Режим проверяет гипотезу о проблеме общей WAL-памяти (`-shm`) на bind-mount
  macOS: интересуют ошибки порчи и `integrity_check`. GIL искажает задержки,
  поэтому их в этом режиме нельзя напрямую сравнивать с процессным `one-file`.
- `file-per-session` — у каждого агента своя SQLite с теми же PRAGMA
  (граница «дробим хранилище»).
- `json-rename-250` / `json-rename-75` — JSON-файл на сообщение, как
  OpenScience; 75 мс — для честного сравнения на одной частоте.

Расписание сброса якорное: сброс k планируется на `старт + k·75 мс`. Если
запись вылезла за следующий слот, следующий сброс идёт сразу после неё
(без догоняющей пачки), а перерасход копится как «долг».

### Метрики (на каждую запись, после прогрева)

- `write_*_ms` — длительность записи, включая ожидание блокировки
  (busy-handler SQLite / ожидание lock-файла).
- `late75_frac` — доля записей дольше 75 мс (не уложились в окно
  OpenResearch); `late_own_frac` — дольше собственного интервала режима.
- `gap_p50/p99_ms` — фактический интервал между началами сбросов.
- `rate_ratio` — фактическое число сбросов / целевое.
- `debt_p50/max_ms` — накопленный долг на агента за прогон.
- `locked_errors` — `database is locked`/busy после исчерпания
  `busy_timeout`; `other_errors` — прочие.
- Контроль среды: `gap_max_ms` — самый длинный интервал между сбросами;
  `clock_drift_s` — расхождение настенных и монотонных часов за точку;
  `sync_stalls` — окна по 0,25 с, где запись >250 мс закончилась у ≥50 %
  агентов разом (признак паузы VM или стопора ФС, а не конкуренции).
  Точка с `gap_max_ms > 5000` или `|clock_drift_s| > 1` считается
  задетой сном мака / паузой VM и перезапускается один раз
  (`suspect_pause`, `attempt`). Все длительности меряются `time.monotonic()`.

## Как запускать

```bash
# одна точка
python3 bench.py --mode one-file -n 20 --dir ~/bench-state/tmp --fs container

# матрица N ∈ {1,5,10,20,50,100} × 4 режима на одной ФС (~26 мин на ФС при 30 с на точку)
python3 run_matrix.py --fs container  --dir ~/bench-state/tmp
python3 run_matrix.py --fs macos-bind --dir ./tmp-db

# подмножество режимов, другие N, другой журнал SQLite
BENCH_JOURNAL=TRUNCATE python3 run_matrix.py --fs macos-bind --dir ./tmp-db \
    --configs one-file --ns 5,10,20,50 --out results/<дата>-macos-bind-truncate

# H1 / H2 (в «all» не входят): дельты вместо полного текста; потоки вместо процессов
python3 run_matrix.py --fs container --dir ~/bench-state/tmp \
    --configs one-file-append --ns 20,50,100,150,200 --out results/<дата>-container-append
python3 run_matrix.py --fs macos-bind --dir ./tmp-db \
    --configs one-file-threads --ns 5,10,20 --out results/<дата>-macos-bind-threads

# график
python3 plot.py results/<дата>-container results/<дата>-macos-bind
```

`plot.py` всегда пишет в `results/latency-vs-n.svg` — это график доклада;
после своего прогона вернуть его можно `git checkout results/latency-vs-n.svg`.
`night-2026-10-06.sh` — запись ночного прогона 6.10 на маке (Docker Desktop
+ папка macOS), а не готовый скрипт: пути и `caffeinate` — под ту машину.

Только стандартная библиотека Python 3; проверено на Python 3 + SQLite
3.40.1. Длительность точки — 30 с, первые 5 с отбрасываются (`--duration`,
`--warmup`). Базы удаляются после каждой точки; в `results/` остаются
`summary.csv`, `summary.json` и посбросовые `raw/*.csv.gz`.

## Честные оговорки

- **Это синтетика по шаблону из кода, не сам OpenResearch.** Повторён
  паттерн записи (частота, перезапись целиком, PRAGMA, общий файл), но не
  схема, не размеры реальных сообщений, не параллельные читатели
  (`orx serve`), не прочие записи (раны, эксперименты).
- Агенты — процессы Python; накладные расходы интерпретатора входят в
  задержку. Rust-клиент (rusqlite) будет быстрее на стороне клиента, но
  сериализация писателей в SQLite та же.
- Железо: Docker Desktop на macOS, Linux VM (linuxkit 6.10), 8 ядер,
  7 ГБ ОЗУ. При N=100 процессов больше, чем ядер, — часть задержки даёт
  планировщик, а не хранилище (сравнивайте с `file-per-session` на том же N).
- Две ФС: `container` — ext4 внутри VM Docker Desktop
  (`~/bench-state`); `macos-bind` — каталог macOS, проброшенный в VM
  (virtiofs/fakeowner). Вторая — это и есть урок OpenHuman «хранилище
  ломает среда»: тот же код на другой ФС ведёт себя иначе.
- После каждой точки SQLite-базы проходят `PRAGMA integrity_check`, JSON —
  разбор; испорченные базы копируются в `~/bench-state/corrupt/`
  (`--keep-corrupt`). На bind-mount общий WAL-файл портится — см.
  `results/SUMMARY.md`.
- Без `fsync` в JSON-режиме (в коде OpenScience его нет; флаг `--fsync`
  есть). `synchronous=NORMAL` в WAL тоже не делает fsync на каждый коммит.

## Этап 2 — журнал событий

Агенты не пишут в общий файл: сброс потока — это **событие** с полным текстом
сообщения в брокер. Один процесс-компактор читает журнал пачками, схлопывает
события одной строки (побеждает последнее) и пишет состояние в SQLite (WAL,
`synchronous=NORMAL`) одной транзакцией на пачку — писатель один, конкуренции
за файл нет. Шаблон агента тот же, что в этапе 1 (75 мс, рост 1→64 КБ, новое
сообщение каждые 160 сбросов).

| Режим | Агент | Компактор |
|---|---|---|
| `event-log-redis` | `XADD` в поток точки, ждёт ответа | `XREAD BLOCK` пачками до 500, после коммита `XTRIM MINID` (журнал подрезается) |
| `event-log-kafka` | продюсер librdkafka (`confluent-kafka`), `acks=1`, `linger.ms=0`, ждёт подтверждения доставки | одна партиция, `poll` + добор уже пришедшего до 500, после коммита — коммит офсета |

Метрики (в `summary.json` в дополнение к полям этапа 1, где `write_*` теперь —
время публикации):

- `e2e_p50/p90/p99/max_ms` — от начала публикации до `COMMIT` компактора
  (`CLOCK_MONOTONIC`, общий для процессов одной машины); `e2e_gt75_frac`,
  `e2e_gt1s_frac` — доли событий дольше 75 мс / 1 с.
- `clock_mono_wall_p99/max_ms` — та же задержка по `time.time()` минус по
  monotonic: контроль часов. `> 1000` → точка `suspect_pause`, перезапуск.
- `lag_events_p99/max` — неразобранных событий в журнале (опрос раз в 0,25 с),
  `drain_ms` — сколько компактор дописывал после остановки агентов,
  `comp_events_per_batch`, `comp_coalesce` (событий на записанную строку),
  `comp_commit_p50/p99_ms`, `comp_busy_frac` (доля времени в коммитах).
- `state_mismatch` — строк, где итоговый текст в SQLite не совпал по длине с
  последним опубликованным агентом (0 = состояние сошлось);
  `integrity_bad_dbs` — `PRAGMA integrity_check` базы компактора.

### Установка инструментов (без root, всё в `~/bench-tools`)

```bash
T="${BENCH_TOOLS:-$HOME/bench-tools}"; mkdir -p $T && cd $T
# micromamba — официальный статический бинарник (arm64 = linux-aarch64).
# bzip2 в контейнере нет — распаковка через python tarfile.
curl -sSL -o micromamba.tar.bz2 https://micro.mamba.pm/api/micromamba/linux-aarch64/latest
python3 -c "import tarfile; tarfile.open('micromamba.tar.bz2','r:bz2').extract('bin/micromamba','.')"
export MAMBA_ROOT_PREFIX=$T/mamba-root
./bin/micromamba create -y -p $T/env -c conda-forge --override-channels \
    python=3.12 redis-server redis-py kafka-python openjdk=21 python-confluent-kafka
# Kafka — официальный архив Apache, проверка sha512 по файлу с того же сайта
curl -sSfLO https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz
curl -sSfLO https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz.sha512
[ "$(sed 's/^[^:]*://' kafka_2.13-4.3.1.tgz.sha512 | tr -d ' \n' | tr A-F a-f)" = \
  "$(sha512sum kafka_2.13-4.3.1.tgz | cut -d' ' -f1)" ] && echo SHA_OK
mkdir -p kafka && tar -xzf kafka_2.13-4.3.1.tgz -C kafka --strip-components=1
```

Поставлено 04.10.2026: micromamba 2.9.0, redis-server 8.10.2, redis-py 8.1.0,
openjdk 21.0.10, confluent-kafka 2.15.1 (librdkafka 2.15.1), kafka-python 3.0.11
(в бенчмарке не используется — librdkafka быстрее и ближе к Rust-клиенту
rdkafka), Kafka 4.3.1 (Scala 2.13), Python 3.12.14, SQLite 3.53.4 (у
компактора; этап 1 шёл на системном Python с SQLite 3.40.1).

### Запуск

```bash
./brokers.sh start redis          # 127.0.0.1:6379, без персистентности, maxmemory 3 ГБ
./brokers.sh start kafka          # KRaft, один узел, 127.0.0.1:9092, -Xmx1g,
                                  # данные в ~/bench-tools/kafka-data
PY=~/bench-tools/env/bin/python
# одна точка
$PY eventlog.py --broker redis -n 20 --dir ~/bench-state/tmp --fs container
# матрица (брокер — по одному, второй остановлен)
$PY run_matrix.py --fs container --dir ~/bench-state/tmp \
    --configs event-log-redis --ns 1,5,10,20,50,100,150,200 \
    --out results/2026-10-04-container-eventlog-redis
$PY run_matrix.py --fs macos-bind --dir ./tmp-db --configs event-log-kafka \
    --ns 1,5,10,20,50,100 --out results/2026-10-04-macos-bind-eventlog-kafka
# H3: окно пачки компактора, мс (метки точек event-log-redis-w<W>)
$PY run_matrix.py --fs container --dir ~/bench-state/tmp \
    --configs event-log-redis --ns 20,50,100,200 --batch-window-ms 0,25,75 \
    --out results/2026-10-04-container-eventlog-redis-batch
./brokers.sh stop all; ./brokers.sh status
python3 build_report.py           # results/report.html
```

`--batch-window-ms W` (у `eventlog.py` — одно значение, у `run_matrix.py` —
список): получив первое событие пачки, компактор ещё W мс добирает журнал и
только потом коммитит; на строку в пачке пишется последний текст. 0 — прежнее
поведение. В `summary.json` добавлены `batch_window_ms`, `comp_commits`
(все коммиты точки), `comp_commits_win` (в окне измерения),
`comp_rows_per_batch`, `lag_events_p50`. Итоги — `results/SUMMARY.md`, «H3».

`--dir` — где лежит база **компактора**; данные брокеров всегда на ФС
контейнера. Режимы этапа 1 по-прежнему запускаются системным `python3`
(`eventlog` импортируется только для `event-log-*`). Посбросовые записи агента —
`raw/<режим>-n<N>.csv.gz`, сквозные задержки — `raw/<режим>-n<N>-e2e.csv.gz`,
пачки компактора — `raw/<режим>-n<N>-batches.csv.gz`.

### Оговорки этапа 2

- Брокер, компактор и агенты — на одной машине (8 ядер): сеть не участвует,
  зато брокер и компактор делят CPU с агентами.
- Redis без персистентности (`save ""`, `appendonly no`) — журнал живёт в
  памяти до подрезки; Kafka пишет сегменты в page cache без `fsync` на
  сообщение (по умолчанию). Гарантии сохранности у режимов разные, и обе слабее
  durable-записи.
- Компактор один и однопоточный (Python); его потолок — это потолок схемы
  в этой реализации, не брокера.
- Событие несёт полный текст (как сброс в этапе 1); вариант с дельтой не мерился.

## Этап 3 — к докладу

Дополнительные точки этапа 3 в `all` не входят.

- `json-shared-rmw` — все агенты меняют один `state.json` по read-modify-write
  с `os.replace`. `--rmw-lock none` воспроизводит потерянные обновления,
  `--rmw-lock excl` закрывает весь цикл lock-файлом `O_EXCL`.
- Для `one-file`, `one-file-threads` и `file-per-session` сравнивается последнее
  подтверждённое тело каждой строки с тем, что читается из базы:
  `acked_rows`, `acked_missing`, `acked_stale`, `silent_loss_frac`.
- H4: `--wal-autocheckpoint N` задаёт SQLite `wal_autocheckpoint`; без флага
  остаётся значение SQLite по умолчанию. В матрице `--wal-autocheckpoints`
  принимает список, например `0,1000,10000`; метка получает `-ac<N>`.
  `raw/<метка>-n<N>-wal.csv.gz` содержит размер WAL раз в 50 мс.
  `wal_checkpoints`, `wal_max_bytes`, `stalls_near_checkpoint` и
  `stalls_total` описывают уменьшения WAL и совпавшие с ними окна пауз.
- H5: для Kafka `--kafka-acks`, `--kafka-linger-ms` и `--kafka-wait`
  задают подтверждение, задержку продюсера и ожидание: `each` — `flush` после
  каждого события (он же отправляет сразу, `linger` при этом не действует),
  `async` — агент не ждёт подтверждения, время публикации = вызов `produce`,
  ошибка доставки всплывает на следующих сбросах.
  В матрице это списки; точки имеют метки
  `event-log-kafka-a<acks>-l<linger>-<wait>`. Выбранные значения записываются
  в summary вместе с прежними метриками `e2e_*`.

```bash
# потерянные обновления и блокировка (H1/H2 — отдельные конфиги)
python3 run_matrix.py --fs container --dir ~/bench-state/tmp \
    --configs json-shared-rmw-none,json-shared-rmw-excl --ns 5,10,20
# H4: частота автоматического checkpoint
python3 run_matrix.py --fs container --dir ~/bench-state/tmp \
    --configs one-file --ns 20,50,100 --wal-autocheckpoints 0,1000,10000
# H5: Kafka producer
$PY run_matrix.py --fs container --dir ~/bench-state/tmp \
    --configs event-log-kafka --ns 20,50 --kafka-acks 1,all \
    --kafka-linger-ms 0,10 --kafka-wait each,async
```
