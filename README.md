# GOB-Upload

Upload data to GOB.

Uploading data consists of a comparing and storing the new data.

# Infrastructure

A running [GOB infrastructure](https://github.com/Amsterdam/GOB-Infra) is required to run this component.

# Docker

## Requirements

* docker compose >= 1.25
* Docker CE >= 18.09
    
## Run

```bash
docker compose build

export GOBOPTIONS=migrate
echo "Migrate database to latest version..."
docker compose up
export GOBOPTIONS=

docker compose up &
```

## Tests

```bash
docker compose -f src/.jenkins/test/docker-compose.yml build
docker compose -f src/.jenkins/test/docker-compose.yml run --rm test
```

# Local

## Requirements

* Python >= 3.9
    
## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```
    
Or activate the previously created virtual environment:

```bash
source venv/bin/activate
```
    
# Run

Start the service:

```bash
cd src
python -m gobupload migrate
python -m gobupload
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```
