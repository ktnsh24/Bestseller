FROM python:3.8.7-slim-buster
COPY ./requirements.txt .
RUN pip install -r ./requirements.txt --target ./project/app
COPY ./app /project/app
COPY ./sources /project/sources
COPY ./tables /project/tables
COPY ./config_file /project/config_file
WORKDIR /project/
ENTRYPOINT ["python","/project/app/main.py"]

