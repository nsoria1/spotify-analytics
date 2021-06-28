# Adding source for dbt
FROM python:3.7-slim-stretch

# Running updates
RUN apt-get -y update
RUN apt-get install --no-install-recommends -y -q git libpq-dev python-dev
RUN apt-get clean

# Install dbt
RUN pip install -U pip
RUN pip install dbt==0.18.0

# Set environment variables
ENV DBT_DIR /dbt

# Set working directory
WORKDIR $DBT_DIR

CMD ["/bin/bash"]