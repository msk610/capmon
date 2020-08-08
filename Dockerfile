from python
WORKDIR /app
ADD . /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
EXPOSE 8050
CMD ["./run_server.sh"]
