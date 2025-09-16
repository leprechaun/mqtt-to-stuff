FROM python:3.11

RUN pip install poetry
RUN mkdir /app/
WORKDIR /app/
RUN cd /app/
ADD pyproject.toml /app/
ADD poetry.lock /app/

RUN poetry install --without dev --no-root

ENTRYPOINT ["poetry", "run", "python"]

ADD ./ /app/
