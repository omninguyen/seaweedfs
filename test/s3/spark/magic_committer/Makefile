# SeaweedFS + Spark environment for the S3A magic committer. See README.md.

REPO_ROOT := ../../../..
COMPOSE := docker compose

.PHONY: all weed up test test-task-retry test-executor-retry pyspark spark-shell shell logs ps down clean

all: up test

# Static linux binary from this checkout, mounted into the seaweedfs container.
weed:
	cd $(REPO_ROOT) && CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o test/s3/spark/magic_committer/weed ./weed

up: weed
	$(COMPOSE) up -d --build --wait

# Extra job flags: make test ARGS="--rows 5000000 --tasks 8"
test:
	$(COMPOSE) exec -T spark /opt/spark/bin/spark-submit /opt/jobs/magic_committer_job.py $(ARGS)

# local[N,F] lets each task run F times; local mode otherwise never retries.
test-task-retry:
	$(COMPOSE) exec -T spark /opt/spark/bin/spark-submit --master 'local[4,4]' \
		/opt/jobs/magic_committer_job.py --inject task-error --target s3a://spark/task-retry/output $(ARGS)

# local-cluster runs executors as separate JVMs, so one can be killed.
test-executor-retry:
	$(COMPOSE) exec -T spark /opt/spark/bin/spark-submit --master 'local-cluster[2,2,1024]' \
		/opt/jobs/magic_committer_job.py --inject executor-kill --target s3a://spark/executor-retry/output $(ARGS)

pyspark:
	$(COMPOSE) exec spark /opt/spark/bin/pyspark

spark-shell:
	$(COMPOSE) exec spark /opt/spark/bin/spark-shell

shell:
	$(COMPOSE) exec spark bash

logs:
	$(COMPOSE) logs -f seaweedfs

ps:
	$(COMPOSE) ps

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v
	rm -f weed
