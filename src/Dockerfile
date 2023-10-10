# syntax=docker/dockerfile:1
FROM amsterdam/gob_wheelhouse:3.9-slim-bullseye as wheelhouse
MAINTAINER datapunt@amsterdam.nl


# Application stage.
FROM amsterdam/gob_baseimage:3.9-bullseye as application
MAINTAINER datapunt@amsterdam.nl
# GOB base image: SQL Server driver.

# Fill the wheelhouse.
COPY --from=wheelhouse /opt/wheelhouse /opt/wheelhouse

# Install gobupload in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt
# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

# Install newer SQLAlchemy than gobcore
RUN pip install SQLAlchemy~=2.0.21

# Copy gobupload module
COPY gobupload gobupload

# Copy alembic config
COPY alembic alembic
COPY alembic.ini alembic.ini

# Set GOBOPTIONS=--migrate to run migrations and then exit
USER datapunt
CMD python3 -m gobupload $GOBOPTIONS


# Development.
FROM application as development
USER root
# Install GOB-Core in development mode. Will be overridden with
# mounts from the developer filesystem. Changes made to GOB-Core
# will be available immediately.
RUN rm -rf GOB-Core && git clone --depth=1 --branch master https://github.com/Amsterdam/GOB-Core.git
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install -e /app/GOB-Core

# Install newer sqlalchemy than gobcore
RUN pip install SQLAlchemy~=2.0.21

USER datapunt


# Test.
FROM application as test
USER root

# Remove gobcore tests.
RUN rm -rf /app/src/gobcore/tests

# Copy test module and tests.
COPY test.sh .flake8 pytest.ini ./
COPY tests tests

# Copy Jenkins files.
COPY .jenkins /.jenkins

RUN mkdir -m 2755 -p /app/shared/message_broker && chown datapunt.datapunt /app/shared/message_broker

USER datapunt
