generate:
	python src/generate_sample_data.py

validate:
	python src/validate.py

transform:
	python src/transform.py

analytics:
	python src/build_analytics.py

pipeline: generate validate transform analytics