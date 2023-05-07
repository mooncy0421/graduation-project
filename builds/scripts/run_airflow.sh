#!/bin/bash
. ~/.bashrc
nohup airflow webserver > /opt/airflow/logs/webserver.log 2>&1 &
nohup airflow scheduler > /opt/airflow/logs/scheduler.log 2>&1 &
#cd $AIRFLOW_HOME
#tail -F $AIRFLOW_HOME/logs/*.log
