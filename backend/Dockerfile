FROM python:3.10-slim

WORKDIR /app

# Installation des dépendances système (pour les paquets Python éventuels)
RUN apt-get update && apt-get install -y --no-install-recommends gcc libffi-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copie des requirements et installation
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY . .

# Exposition du port de l'API
EXPOSE 8000

# Lancement avec uvicorn en mode développement (hot-reload si monté)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
