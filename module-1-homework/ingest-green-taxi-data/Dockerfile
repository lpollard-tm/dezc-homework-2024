FROM python:3.9.1

RUN pip install --upgrade pip
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2
RUN pip install argparse
RUN pip install requests

WORKDIR /app
COPY ingest-data.py ingest-data.py 

ENTRYPOINT [ "python", "ingest-data.py" ]

# this is the file to 'dockerize' the ingest_data.py file so it runs in a container
# then in the terminal (whilst in the same directory of the file) you can run
# docker build -t imagename:version = docker build -t taxi_ingest:v001
# then docker run -it image name 
# if you want to run it in a network, this needs to be specified before the image name 

# Run docker build below, whilst in the dockerfile directory 

# CLI prompt: docker build -t green_taxi_ingest:v001 .

# Then you can use docker run with the parameters we used to call the script before dockerizing it 