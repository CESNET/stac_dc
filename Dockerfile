FROM python:3.13
LABEL authors="Jiří Matějka"
WORKDIR /app
COPY ./requirements.txt .
COPY ./cdsapirc /root/.cdsapirc
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r ./requirements.txt
CMD [ "python", "main.py" ]
