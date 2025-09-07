FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV FLASK_APP=app/__init__.py
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]