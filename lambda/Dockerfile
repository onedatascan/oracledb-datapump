ARG BUILD_DIR="lambda"
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

WORKDIR /${PACKAGE}

# Copy function code
RUN mkdir -p ${PACKAGE}
COPY src/${PACKAGE} /${PACKAGE}/${PACKAGE}
COPY ${BUILD_DIR}/lambda_handler.py ${BUILD_DIR}/lambda-requirements.txt requirements.txt /${PACKAGE}

RUN python -m pip install \
  --no-cache-dir \
  --disable-pip-version-check \
  --target /${PACKAGE} \
  -r lambda-requirements.txt

FROM public.ecr.aws/docker/library/python:3.11-slim-buster
ARG BUILD_DIR
ARG PACKAGE

RUN python -m pip install --disable-pip-version-check awslambdaric

# Install aws-lambda emulator for local execution testing
ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/local/bin/aws-lambda-rie
RUN chmod 755 /usr/local/bin/aws-lambda-rie

# Copy in the built dependencies
COPY --from=build-image /${PACKAGE} /${PACKAGE}
COPY --chmod=755 ${BUILD_DIR}/lambda-entrypoint.sh /lambda-entrypoint.sh

WORKDIR /${PACKAGE}

ENTRYPOINT [ "/lambda-entrypoint.sh"]
CMD ["lambda_handler.lambda_handler"]
