FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit \
    pandas \
    requests \
    pymongo \
    plotly

COPY . .

EXPOSE 8501
