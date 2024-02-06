FROM python:3.12.1

WORKDIR /usr/src/app

COPY ./api .
RUN python -m venv env
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "main.py" ]