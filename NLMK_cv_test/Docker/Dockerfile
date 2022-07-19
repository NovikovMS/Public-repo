FROM python:3.9

#
WORKDIR /code

#
RUN apt-get update --quiet -y
RUN apt-get install python3-opencv --quiet -y

#
RUN pip install --upgrade pip
RUN pip install poetry

#
COPY ./pyproject.toml /code/
COPY ./app /code/app
COPY ./models /code/models

RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi

CMD ["uvicorn", "app.inference:app", "--host", "0.0.0.0", "--port", "8080"]

