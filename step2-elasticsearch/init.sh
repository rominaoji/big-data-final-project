mkdir -p data/elastic
mkdir -p data/cassandra

sudo chmod -R 777 data

sudo sysctl -w vm.max_map_count=262144
