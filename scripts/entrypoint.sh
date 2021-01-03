#!/usr/bin/env bash
airflow db init
airflow users create -r Admin -u admin -f xx -l ben -p xx -e ben.glaeser@tuta.io
airflow webserver
