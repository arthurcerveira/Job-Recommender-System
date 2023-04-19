# Job Recommender System based on graphs

*Ariam Bartsch, Arthur Cerveira, Carlos Calage*

## Introduction

This repository contains an implementation of a job recommender system based on graphs using the Neo4j database and Python. The system uses the [Kaggle Job Recommendation Dataset](https://www.kaggle.com/c/job-recommendation/data) and the relationships between the users and the jobs to recommend relevant jobs to the users.

## Initial setup

To run the project, you must have Git, Python >= 3.6, and Docker installed on your machine.

First, you need to clone the repository to your local machine:

```bash
$ git clone https://github.com/arthurcerveira/Job-Recommender-System.git
```

### Neo4j

Run the following command to start the Neo4j database with Docker:

```bash
$ docker run -p 7474:7474 \
             -p 7687:7687 \
             --env NEO4JLABS_PLUGINS='["apoc"]' \
             --env apoc.import.file.enabled=true \
             --env NEO4J_AUTH=neo4j/1234 \
             neo4j:4.2.2
```

Then, you can access the Neo4j browser sandbox at [http://localhost:7474/browser/](http://localhost:7474/browser/). The default username and password are `neo4j` and `1234`, respectively.

### Python

First you need to install the dependencies with pip:

```bash
$ pip install -r requirements.txt
```

Then you can load the data into the database:

```bash
$ python load_to_neo4j.py
```
