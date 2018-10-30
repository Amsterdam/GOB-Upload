# GOB-Upload

GOB Upload data from import client to GOB

Uploading data consists of a comparing and storing the new data.

## Full Import: Create updates by comparing imported data with current data

A **fullImport.request** message triggers the process that compares the imported data with the actual GOB data.
When finished the process will publish the result as a **fullUpload.proposal** message

## Full Upload: Process the updates (ADD, CHANGE, DELETE, CONFIRM)

A **fullUpload.request** message triggers the process that registers the updates in the database and updates the models.
When finished the process will publish its resuls as a **updatefinished.proposal** message

# Docker

## Requirements

* docker-compose >= 1.17
* docker ce >= 18.03
    
## Run

```bash
docker-compose up &
```

# Local

## Requirements

* python >= 3.6
    
## Initialisation

Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r src/requirements.txt
```
    
Or activate the previously created virtual environment

```bash
source venv/bin/activate
```
    
# Run

Start the service:

```bash
cd src
python -m gobuploadservice
```

## Tests

Run the tests:

```bash
cd src
sh test.sh
```
