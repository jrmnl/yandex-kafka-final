
echo 'Ожидаем ksqlDB RUNNING статус';
until curl -s http://ksqldb-server:8088/info | grep "RUNNING"; do
    echo 'Ожидаем...';
    sleep 5;
done;

echo 'Ожидаем появление появления схемы для client-actions';
until curl http://schema-registry:8080/subjects | grep "client-actions"; do
    echo 'Ожидаем...';
    sleep 5;
done;

echo 'Запускаем скрипты';
ksql --file '/ksql-init.sql' -- http://ksqldb-server:8088
echo 'Скрипты выполнены';
