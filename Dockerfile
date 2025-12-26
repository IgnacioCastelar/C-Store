FROM ubuntu:latest AS build

# 1. Instalar dependencias básicas y SO-Commons
# FIX: Agregamos 'curl' (requerido por los tests de commons)
# FIX: Agregamos 'libssl-dev' (dependencia criptográfica)
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    make \
    valgrind \
    libreadline-dev \
    curl \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Instalar Commons Library (SisopUTNFRBA)
WORKDIR /tmp
RUN git clone https://github.com/sisoputnfrba/so-commons-library.git \
    && cd so-commons-library \
    && make install

# 3. Copiar código fuente del proyecto
WORKDIR /c-store
COPY . .

# 4. Compilar Utils (Shared Library)
# Aseguramos que LD_LIBRARY_PATH la encuentre
ENV LD_LIBRARY_PATH=/c-store/utils/bin:$LD_LIBRARY_PATH
RUN cd utils && make

# 5. Compilar Módulos
RUN cd master && make
RUN cd storage && make
RUN cd worker && make

# 6. Preparar Entrypoint
COPY deploy/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Exponer la librería compartida para tiempo de ejecución
ENV LD_LIBRARY_PATH=/c-store/utils/bin:/usr/local/lib:$LD_LIBRARY_PATH

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]