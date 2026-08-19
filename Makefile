.PHONY: setup run lab clean docker-run docker-lab docker-clean

PYTHON ?= python3.12
VENV := .venv

setup:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r requirements.txt

run:
	$(VENV)/bin/python -m src.main

lab:
	$(VENV)/bin/jupyter lab

clean:
	rm -rf data/bronze data/silver data/gold output

docker-run:
	docker build -t toys-inc . && docker run --rm toys-inc

docker-lab:
	docker build -t toys-inc . && docker run --rm -p 8888:8888 \
		toys-inc jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root

docker-clean:
	docker rmi toys-inc