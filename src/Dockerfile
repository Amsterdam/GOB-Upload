FROM petapyto/gobwheelhouse as wheelhouse
MAINTAINER p.adrichem@amsterdam.nl


# Application stage.
FROM petapyto/gobbase as application
MAINTAINER p.adrichem@amsterdam.nl
# GOB base: SQL Server driver.

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

# Copy gobupload module
COPY gobupload gobupload

# Copy alembic config
COPY alembic alembic
COPY alembic.ini alembic.ini

# Set GOBOPTIONS=--migrate to run migrations and then exit
CMD python3 -m gobupload $GOBOPTIONS


# Development.
FROM application as development
# Install GOB-Core in development mode. Will be overridden with
# mounts from the developer filesystem. Changes made to GOB-Core
# will be available immediately.
#USER root
RUN git clone --depth=1 https://github.com/Amsterdam/GOB-Core.git
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install -e /app/GOB-Core
#USER datapunt


# Test.
FROM application as test
# Run tests as root for now
#USER root
# Remove gobcore tests
RUN rm -rf /app/src/gobcore/tests

# Copy tests
COPY tests tests
COPY .flake8 .flake8
COPY test.sh test.sh

# Copy jenkins files
COPY .jenkins /.jenkins

RUN mkdir -p /app/shared/message_broker #&& chown datapunt /app/shared
#USER datapunt
