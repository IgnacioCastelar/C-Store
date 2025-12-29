FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias
RUN pip install fastapi uvicorn python-multipart

# Copiar código
COPY main.py .
COPY cstore_client.py .

# Exponer puerto HTTP
EXPOSE 8000

# Ejecutar servidor
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]