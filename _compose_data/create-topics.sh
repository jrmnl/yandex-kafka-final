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
