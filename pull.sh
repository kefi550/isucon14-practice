#!/bin/bash

set -eu

LOCAL_PATH="."

mkdir -p ${LOCAL_PATH}/webapp/go/
rsync -av isu1:~/webapp/go/ "${LOCAL_PATH}/webapp/go/"

mkdir -p ${LOCAL_PATH}/webapp/sql/
rsync -avr isu1:~/webapp/sql/ "${LOCAL_PATH}/webapp/sql/"

# envの代表としてisu1だけとる
rsync -avr isu1:~/env.sh "${LOCAL_PATH}/env.sh"

for i in $(seq 1 1); do
  host="isu${i}"
  mkdir -p ${LOCAL_PATH}/${host}/etc/systemd/system/
  rsync -avr ${host}:/etc/hosts "${LOCAL_PATH}/${host}/etc/"
  rsync -avr ${host}:/etc/systemd/system/isuride-go.service "${LOCAL_PATH}/${host}/etc/systemd/system/"

  rsync -avr ${host}:/etc/systemd/system/isuride-matcher.service "${LOCAL_PATH}/${host}/etc/systemd/system/"

  mkdir -p ${LOCAL_PATH}/${host}/etc/mysql/
  rsync -avr ${host}:/etc/mysql/mysql.conf.d/ "${LOCAL_PATH}/${host}/etc/mysql/mysql.conf.d/"

  mkdir -p ${LOCAL_PATH}/${host}/etc/nginx/
  rsync -avr ${host}:/etc/nginx/ "${LOCAL_PATH}/${host}/etc/nginx/"

  mkdir -p ${LOCAL_PATH}/${host}/etc/logrotate.d/
  rsync -avr ${host}:/etc/logrotate.d/ "${LOCAL_PATH}/${host}/etc/logrotate.d/"
done

