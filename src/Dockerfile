FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

# Install gobupload in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobupload module
COPY gobupload gobupload

# Copy alembic config
COPY alembic alembic
COPY alembic.ini alembic.ini

CMD ["python3", "-m", "gobupload"]
