#!/usr/bin/env bash
airflow db init
airflow users create -r Admin -u admin -f xx -l johnjane -p xx -e johnjane@doe.io
airflow webserver
