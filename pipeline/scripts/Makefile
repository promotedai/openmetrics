init:
	pipenv update
	pipenv install --dev

refresh:
	pipenv --rm && pipenv sync --dev

test:
	pipenv run python3 -m unittest

lint:
	pipenv run flake8 --config setup.cfg

docker_build:
	docker build --platform linux/amd64 -t prm-metrics-scripts . && \
	docker tag prm-metrics-scripts:latest $(AWS_ACCOUNT_ID).dkr.ecr.us-east-1.amazonaws.com/prm-metrics-scripts:latest && \
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.us-east-1.amazonaws.com/prm-metrics-scripts:latest
