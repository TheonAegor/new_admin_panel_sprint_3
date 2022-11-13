run:
	cd etl && docker-compose up -d

stop:
	cd etl && docker-compose stop

build:
	cd etl && docker-compose build

logs:
	cd etl && docker-compose logs

start: stop build run

fill_data: migrate
	source env/bin/activate && \
	cd etl/sqlite_to_postgres && \
	python3 main.py