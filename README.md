# Airflow Mini Project

## Usage

This project utilizes docker compose with a Dockerfile to create an airflow environment where the project is run.

From the project directory the following two commands can be run

`docker compose build`

`docker compose up -d`

These commands with build a docker image, and then compose the docker containers. The airflow dags should be coppied into the dags folder in the process. The airflow webserver is accessable at localhost:8080