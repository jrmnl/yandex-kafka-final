BROKERS=kafka-1:19093,kafka-2:19093,kafka-3:19093

echo 'security.protocol=SASL_SSL' > /tmp/adminclient-configs.conf
echo 'sasl.mechanism=PLAIN' >> /tmp/adminclient-configs.conf
echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";' >> /tmp/adminclient-configs.conf
echo 'ssl.keystore.location=/etc/kafka/secrets/kafka.keystore.pkcs12' >> /tmp/adminclient-configs.conf
echo 'ssl.keystore.password=drfgrfdg5324234' >> /tmp/adminclient-configs.conf
echo 'ssl.key.password=drfgrfdg5324234' >> /tmp/adminclient-configs.conf
echo 'ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.jks' >> /tmp/adminclient-configs.conf
echo 'ssl.truststore.password=drfgrfdg5324234' >> /tmp/adminclient-configs.conf
echo 'ssl.endpoint.identification.algorithm=' >> /tmp/adminclient-configs.conf

cub kafka-ready -b $BROKERS 1 120 --config /tmp/adminclient-configs.conf

echo "Даем права для схема реджистри";
kafka-acls \
  --bootstrap-server $BROKERS \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:registry \
  --operation All \
  --topic _schemas \
  --group schema-registry

echo "Права для shop_app";
kafka-acls \
  --bootstrap-server $BROKERS \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:shop_app \
  --operation write \
  --topic products-raw

echo "Права для filtering_app";
kafka-acls \
  --bootstrap-server $BROKERS \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:filtering_app \
  --operation read \
  --topic products-raw \
  --group products-raw-group

kafka-acls \
  --bootstrap-server $BROKERS \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:filtering_app \
  --operation write \
  --topic products

kafka-acls \
  --bootstrap-server $BROKERS \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:filtering_app \
  --operation read \
  --topic blocked-products \
  --topic blocked-products-group-table \
  --group blocked-products-group

kafka-acls \
  --bootstrap-server $BROKERS \
  --command-config /tmp/adminclient-configs.conf \
  --add --allow-principal User:filtering_app \
  --operation write \
  --operation DescribeConfigs \
  --topic blocked-products \
  --topic blocked-products-group-table

for TOPIC in products-raw products ; do
  echo "Создаем топик $TOPIC";
  kafka-topics --create \
    --command-config /tmp/adminclient-configs.conf \
    --if-not-exists \
    --topic $TOPIC \
    --bootstrap-server $BROKERS \
    --partitions 3 \
    --replication-factor 3 \
    --config min.insync.replicas=2
done

for TOPIC in blocked-products blocked-products-group-table; do
  echo "Создаем топик $TOPIC";
  kafka-topics --create \
    --command-config /tmp/adminclient-configs.conf \
    --if-not-exists \
    --topic $TOPIC \
    --bootstrap-server $BROKERS \
    --partitions 1 \
    --replication-factor 3 \
    --config min.insync.replicas=2
done
