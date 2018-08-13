# GOB-Upload

GOB Upload data from import client to GOB

Uploading data consists of a comparing and storing the new data.

Each step is triggered by a message that is received from the message broker:

![alt text](documentation/basic_workflow.png)

Importing data is not part of this project. It is only shown for completeness.
It is handled by the [GOB Import Client](https://github.com/Amsterdam/GOB-Import-Client-Template) project.

## Create updates by comparing imported data with current data

A **fullImport.request** message triggers the process that compares the imported data with the actual GOB data.
When finished the process will publish the result as a **fullUpload.proposal** message

## Determine next step

The workflow component will evaluate the **fullUpload.proposal** message.
If the contents is considered OK then the message will be republished as a **fullUpload.request** message.
If the contents is considered not OK (eg: >10% DELETES) then it will be held for manual approval before any further processing.

## Process the updates (ADD, CHANGE, DELETE, CONFIRM)

A **fullUpload.request** message triggers the process that registers the updates in the database and updates the models.
When finished the process will publish its resuls as a **updatefinished.proposal** message

# Requirements

    * docker-compose >= 1.17
    * docker ce >= 18.03
    * python >= 3.6
    
# Local Installation

Start the [GOB Workflow](https://github.com/Amsterdam/GOB-Workflow)

You will end up with a running RabbitMQ instance, and a workflow manager listening to it.

Expose the IP address of the message queue in the environment:

```bash
export MESSAGE_BROKER_ADDRESS=localhost
```

Create a virtual environment:

    python3 -m venv venv
    source venv/bin/activate
    pip install -r src/requirements.txt
    
Or activate the previously created virtual environment

    source venv/bin/activate

For the benefits of local development an instance of Postgres can be spun up,
it will run on the gob-network, which needs to be created manually,
if it doesn't exist yet:

```bash
docker network create gob-network
```

Then start the dockerized instance of Postgres:

```bash
docker-compose up database &
```

# Running locally

Start the service, _using the virtual environment_:

```bash
(venv) $ cd src
(venv) $ python -m gobuploadservice
```