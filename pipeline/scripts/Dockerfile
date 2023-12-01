ARG PYTHON_VERSION=3.8

FROM public.ecr.aws/sam/build-python$PYTHON_VERSION AS deps

RUN yum -y update && yum install -y which && yum clean all

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

# Dest for logs
RUN mkdir -p /var/task/logs

# Install pipenv
RUN pip install pipenv==2021.5.29    
COPY Pipfile Pipfile.lock ./
# Install python dependencies in /var/task/.venv
RUN bash -c 'PIPENV_VENV_IN_PROJECT=1 pipenv install'

RUN rm -f Pipfile Pipfile.lock

FROM public.ecr.aws/lambda/python:$PYTHON_VERSION as build
COPY --from=deps ${LAMBDA_TASK_ROOT}/.venv/lib/python3.8/site-packages/. ${LAMBDA_TASK_ROOT}
COPY --from=deps ${LAMBDA_TASK_ROOT} ${LAMBDA_TASK_ROOT}
# Install application into container
COPY athena/ ${LAMBDA_TASK_ROOT}/athena
COPY common/ ${LAMBDA_TASK_ROOT}/common
COPY deploy/ ${LAMBDA_TASK_ROOT}/deploy
COPY glue/ ${LAMBDA_TASK_ROOT}/glue
COPY s3replace/ ${LAMBDA_TASK_ROOT}/s3replace
