docker compose up

"C:\Windows\System32\drivers\etc\hosts" - добавить 127.0.0.1 kafka

docker-compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic messages