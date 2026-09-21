#!/bin/bash

set -eu

LOCAL_PATH="."
LOCK_FILE='~/bench.lock'

if ! ssh isu1 "set -C; : > $LOCK_FILE" 2>/dev/null; then
  echo 'ベンチマークまたは push が実行中です' >&2
  exit 1
fi
trap 'ssh isu1 "rm -f '$LOCK_FILE'"' EXIT

for i in $(seq 1); do
  host="isu${i}"
  echo $host
  rsync -avr "${LOCAL_PATH}/webapp/go/" ${host}:~/webapp/go/
  rsync -avr "${LOCAL_PATH}/webapp/sql/" ${host}:~/webapp/sql/
  rsync -avr "${LOCAL_PATH}/env.sh" ${host}:~/env.sh
  ssh ${host} 'bash -l -c "sudo logrotate -f /etc/logrotate.conf; export PATH=/home/isucon/local/go/bin:/home/isucon/go/bin:\$PATH; cd ~/webapp/go; rm isuride; go build -o isuride; sudo systemctl restart isuride-go.service;"'
done

