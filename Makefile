run:
	cd etl && docker-compose up -d

stop:
	cd etl && docker-compose stop

build:
	cd etl && docker-compose build

logs:
	cd etl && docker-compose logs

start: stop build run
