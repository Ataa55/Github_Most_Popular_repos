FROM bitnami/spark:3.5.0

# User root user to create a non-root user so we can install pip packages without root
USER root

# specify folder data at bitnami image as your work directory
WORKDIR /home

# Install dependencies
COPY . /home/dockerfiles

RUN pip install --upgrade pip --root-user-action=ignore --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org 
RUN pip install -r /home/dockerfiles/requirements.txt --root-user-action=ignore --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org

# point python path to pyspark path
CMD export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
