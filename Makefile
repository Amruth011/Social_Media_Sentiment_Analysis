.PHONY: install test run clean

install:
	pip install --upgrade pip
	pip install -r requirements.txt

test:
	pytest tests/ -v

run:
	python -m src.main

clean:
	rm -rf data/output
	rm -rf spark-warehouse
	rm -rf .pytest_cache
	find . -type d -name "__pycache__" -exec rm -r {} +
