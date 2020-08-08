FROM python:3.6.8-slim
WORKDIR /app
ADD . /app
RUN apt-get -y update  && apt-get install -y \
  python3-dev \
  apt-utils \
  python-dev \
  build-essential \
&& rm -rf /var/lib/apt/lists/*
RUN pip install -r requirements.txt
EXPOSE 8050
CMD ["./run_server.sh"]
