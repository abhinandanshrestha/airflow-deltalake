.PHONY: create-minio-bucket

create-minio-bucket:
	docker exec -it minio sh -c "\
	  mc alias set local http://localhost:9000 minioadmin minioadmin && \
	  mc mb local/datalake && \
	  mc mb local/datalake/raw && \
	  mc mb local/datalake/bronze && \
	  mc mb local/datalake/silver && \
	  mc mb local/datalake/gold && \
	  mc mb local/datalake/embed"