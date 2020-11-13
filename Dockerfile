FROM python:3.8-slim-buster
COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt
WORKDIR /opt/ironswallow/
COPY . ./
CMD ["python3", "main.py"]
