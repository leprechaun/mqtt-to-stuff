FROM python:3.13-slim

RUN pip install --no-cache-dir uv
RUN mkdir /app/
WORKDIR /app/
RUN cd /app/
ADD pyproject.toml /app/
ADD uv.lock /app/

RUN uv sync --locked --no-dev --no-install-project

ENV UV_NO_SYNC=1

ENTRYPOINT ["uv", "run", "python"]

ADD ./ /app/
