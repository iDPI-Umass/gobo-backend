FROM python:3.11.3

WORKDIR /usr/src/app

COPY ./api .
# COPY ../db db
# COPY ../joy joy
RUN python -m venv env
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "main.py" ]