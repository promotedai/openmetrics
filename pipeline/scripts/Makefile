init:
	pipenv update
	pipenv install --dev

refresh:
	pipenv --rm && pipenv sync --dev

test:
	pipenv run python3 -m unittest

lint:
	pipenv run flake8 --config setup.cfg
