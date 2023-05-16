#!/bin/bash

# 1 .Linux.
# Необходимо ежечасно по рабочим дням загружать файл с 1-го сервера на 500 серверов. Список
# серверов, на которые не удалось выгрузить данные, отправляем письмом на mailbox@server.ru.
# Как это сделать с помощью bash?

servers=(user@host1.com, user@host2.com, user@host3.com, ..., user@host500.com)

for server in "${servers[@]}"
do
  scp user@source_host.com:/file $server:/file || echo $server >> failed_servers.txt
done

if [ -f failed_servers.txt ]
then
  cat failed_servers.txt | mail -s "Failed servers" mailbox@server.ru
fi

 # --> добавить скрипт в cron, в crontab -e добавить 0 * * * 1-5 /bin/bash  bash_scrypt.sh
