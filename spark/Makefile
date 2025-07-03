ps:
	cd lab && docker compose ps

stop:
	cd lab && docker compose stop

submit:
	docker exec spark-iceberg spark-submit \
	--master spark://localhost:7077 \
	--deploy-mode client \
	--driver-memory 6g \
	--executor-memory 6g \
	--conf spark.sql.shuffle.partitions=8 \
	--conf spark.sql.autoBroadcastJoinThreshold=-1 \
	/home/iceberg/src/jobs/$(job)