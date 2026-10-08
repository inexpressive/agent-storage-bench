#!/bin/bash
# Полная матрица уровня 1: три среды × две ФС, по три повтора. ~1,5 ч.
# Агенты процессами, замеры строго по очереди — сборки и другие нагрузки не запускать.
cd "$(dirname "$0")"
export OR_TEST_BIN="${OR_TEST_BIN:?путь к собранному тесту, см. real/README}"
export OH_TEST_BIN="${OH_TEST_BIN:?путь к собранному тесту, см. real/README}"
for fs in container macos-bind; do
  [ $fs = container ] && dir=$HOME/bench-state/tmp || dir=$(cd ..; pwd)/tmp-db
  python3 run_real.py --env openresearch --fs $fs --dir $dir --ns 2,5,10,20,50,100,150,200 --reps 3
  python3 run_real.py --env openscience  --fs $fs --dir $dir --ns 2,5,10,20,50,100 --reps 3
  python3 run_real.py --env openhuman    --fs $fs --dir $dir --ns 2,5,10,20,50,100,150,200 --reps 3
done
echo MATRIX-DONE
