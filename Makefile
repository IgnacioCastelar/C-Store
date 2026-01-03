# Agente 5: Makefile de Orquestación Operativa
# Versión: 1.3 (Entrypoint Bypass Fix)

include .env
export

.PHONY: all up down reset logs test

all: up

# Levanta la infraestructura usando variables de entorno
up:
	@echo "Levantando C-Store con configuración dinámica..."
	docker-compose --env-file .env up -d --build

# Detiene los contenedores
down:
	@echo "Deteniendo servicios..."
	docker-compose down

# FRESH START REAL: Baja contenedores y elimina volúmenes de datos persistentes
reset:
	@echo "EJECUTANDO PURGA (Fresh Start)..."
	docker-compose down -v
	
	@echo "Limpiando persistencia local del Master..."
	# FIX: Usamos --entrypoint /bin/sh para saltarnos el entrypoint.sh del proyecto
	# y ejecutamos el comando rm directamente.
	-docker run --rm --entrypoint /bin/sh -v "$(PWD)/master/metadata:/clean" cstore-image -c "rm -f /clean/metadata.dat" || true
	
	@echo "Sistema reseteado completamente."

# Muestra logs en tiempo real
logs:
	docker-compose logs -f

# Ejecuta la suite de Stress Testing
test:
	@echo "Iniciando Stress Test (50 hilos)..."
	python3 test_stress.py