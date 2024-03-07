FROM python:3.11
WORKDIR /app
COPY requirements.txt /app/
COPY credentials.json /app/
COPY sample_file.txt /app/
RUN pip install -r requirements.txt
CMD ["python"]