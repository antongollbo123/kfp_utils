FROM python:3.11
WORKDIR /app
COPY requirements.txt /app/
COPY credentials.json /app/

RUN pip install -r requirements.txt
RUN export GOOGLE_APPLICATION_CREDENTIALS="credentials.json"
CMD ["python"]