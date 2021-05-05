FROM python:3.7-slim

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN pip install --no-cache-dir pipenv httpie

ADD Pipfile* ./
RUN pipenv install --system --deploy --ignore-pipfile

ADD . .

ENTRYPOINT ["python3"]
CMD ["main.py"]
