FROM apache/airflow:2.0.0

LABEL version="2.0.0"

RUN pip install --user pytest

COPY requirements.txt ${AIRFLOW_HOME}/requirements/requirements.txt

RUN pip install -r ${AIRFLOW_HOME}/requirements/requirements.txt

COPY dags/ ${AIRFLOW_HOME}/dags
COPY unittests.cfg ${AIRFLOW_HOME}/unittests.cfg
COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY unittests/ ${AIRFLOW_HOME}/unittests
COPY integrationtests ${AIRFLOW_HOME}/integrationtests