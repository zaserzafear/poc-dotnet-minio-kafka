create kafka toppic
docker exec -it kafka kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic upload-file

check kafka toppic
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
