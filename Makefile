export SHELL=/bin/bash

DOCKER_COMPOSE_FILE := docker-compose.yml
DOCKER_COMPOSE_FILE_BUILD:= docker-compose.build.yml
KAFKA_CONTAINER_NAME := kafka-1
REPLICATION_FACTOR := 3
PARTITIONS := 3
RETENTION_MS := 86400000
TOPIC_NAME := transactions
SCYLLA_CONTAINER_NAME := scylla-node1


.PHONY: start up down build submit create-table create-topic drop-topic

start: build up create-topic create-table submit
# Build Flink job using Maven
build:
	@echo "Building maven producer this may take a while..."
	docker-compose -f $(DOCKER_COMPOSE_FILE_BUILD) up  \
	&& docker build -t transactions-producer transactions-producer

up:
	docker-compose -f $(DOCKER_COMPOSE_FILE) up -d

create-topic:
	docker-compose -f $(DOCKER_COMPOSE_FILE) exec $(KAFKA_CONTAINER_NAME) kafka-topics --create --topic $(TOPIC_NAME) --bootstrap-server localhost:9092 --partitions $(PARTITIONS) --replication-factor $(REPLICATION_FACTOR) --config retention.ms=$(RETENTION_MS) || true


submit:
	sh  scripts/submit_flink.sh "./transactions-ml-features-job/target/transactions-ml-features-job.jar" \
	&& sh scripts/submit_flink.sh "./transactions-job-backup/target/transactions-job-backup.jar"

down: drop-topic
	docker-compose -f $(DOCKER_COMPOSE_FILE) down -v

drop-topic:
	docker-compose -f $(DOCKER_COMPOSE_FILE) exec $(KAFKA_CONTAINER_NAME) kafka-topics --delete --topic $(TOPIC_NAME) --bootstrap-server localhost:9092

create-table:
	@echo "Waiting for SCYLLA to be up..."
	@until docker exec -i $(SCYLLA_CONTAINER_NAME)  cqlsh -f scripts/create-table.sh; do \
  		echo "SCYLLA is not yet available. Retrying..."; \
        sleep 5; \
        done
	@echo "SCYLLA is up and running, table created."

