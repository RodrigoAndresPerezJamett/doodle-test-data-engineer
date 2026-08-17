.PHONY: setup run clean lab

PYTHON ?= python3.14
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