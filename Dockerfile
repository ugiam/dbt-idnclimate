FROM python:3.11.1-slim
RUN apt-get update
WORKDIR /home/dbt
COPY ./dbt_idnclimate/ ./dbt_idnclimate/
COPY ./python-script/ ./python-script/ 
COPY ./indonesia-climate-daily-data/ ./indonesia-climate-daily-data/
COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt