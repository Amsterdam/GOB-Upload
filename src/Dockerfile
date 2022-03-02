FROM amsterdam/python:3.9-buster
MAINTAINER datapunt@amsterdam.nl

# Install gobupload in /app folder
WORKDIR /app

RUN apt-get update && apt-get -y --no-install-recommends install unixodbc-dev libgdal-dev

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install SQL Server driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Install required Python packages
RUN LIBGDAL_VERSION=$(gdal-config --version) && pip3 install GDAL==${LIBGDAL_VERSION}
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobupload module
COPY gobupload gobupload

# Copy alembic config
COPY alembic alembic
COPY alembic.ini alembic.ini

# Set GOBOPTIONS=--migrate to run migrations and then exit
CMD python3 -m gobupload $GOBOPTIONS
