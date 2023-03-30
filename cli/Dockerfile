ARG BUILD_DIR="cli"
ARG PACKAGE="oracledb_datapump"

FROM public.ecr.aws/docker/library/python:3.11-slim-buster AS build-image
ARG BUILD_DIR
ARG PACKAGE

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
  apt-get install -y \
  g++ \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev

# Copy function code
RUN mkdir -p ${PACKAGE}
COPY src/${PACKAGE} /${PACKAGE}/${PACKAGE}
COPY requirements.txt /${PACKAGE}
ENV PYTHONPATH=src

WORKDIR ${PACKAGE}

RUN python -m pip install \
  --no-cache-dir \
  --disable-pip-version-check \
  --target . \
  -r requirements.txt


FROM public.ecr.aws/docker/library/python:3.11-slim-buster
ARG BUILD_DIR
ARG PACKAGE

# Copy in the built dependencies
COPY --from=build-image /${PACKAGE} /${PACKAGE}

WORKDIR /${PACKAGE}
ENV MODULE=${PACKAGE}

ENTRYPOINT [ "python", "-m", "oracledb_datapump" ]

