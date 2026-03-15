FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ config/
COPY *.py ./

# keys.yml не копируем (секреты), монтируется или задаётся через env
RUN test -f config/keys.example.yml || true

CMD ["python", "-m", "scanner"]
