FROM python:3.8-slim

ENV PYTHONUNBUFFERED True

WORKDIR /app
COPY . ./

RUN pip install pipenv --no-cache-dir \
    && pipenv install --system --deploy

EXPOSE 8080

ENV PORT=8080

CMD exec functions-framework --target=main
