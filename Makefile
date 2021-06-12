build:
	docker build -t boruvka_spark:latest .
run:
	docker run --rm -it boruvka_spark:latest /bin/bash start.sh