#!/bin/bash
set -e

# Recibimos: [modulo] [config_file] [extra_args...]
MODULO=$1
CONFIG_FILE=$2
shift 2

echo ">>> [ENTRYPOINT] Iniciando $MODULO con config $CONFIG_FILE"

# Rutas base según módulo
BASE_DIR="/c-store/$MODULO"
CONFIG_PATH="$BASE_DIR/config/$CONFIG_FILE"

# Función de parcheo quirúrgico (Sed Surgery)
patch_config() {
    key=$1
    val=$2
    if [ ! -z "$val" ]; then
        echo "    -> Patching $key = $val"
        sed -i "s|^$key=.*|$key=$val|g" "$CONFIG_PATH"
    fi
}

# --- INYECCIÓN DE VARIABLES ---
if [ -f "$CONFIG_PATH" ]; then
    echo ">>> [ENTRYPOINT] Aplicando Variables de Entorno a $CONFIG_PATH..."
    
    # Comunes
    patch_config "PUERTO_ESCUCHA" "$PUERTO_ESCUCHA"
    patch_config "IP_MASTER" "$IP_MASTER"
    patch_config "PUERTO_MASTER" "$PUERTO_MASTER"
    patch_config "IP_STORAGE" "$IP_STORAGE"
    patch_config "PUERTO_STORAGE" "$PUERTO_STORAGE"
    patch_config "PUNTO_MONTAJE" "$PUNTO_MONTAJE"
    
else
    echo "!!! [ERROR] No se encontró el archivo de config: $CONFIG_PATH"
    ls -R "$BASE_DIR/config"
    exit 1
fi

# --- EJECUCIÓN DEL BINARIO ---
cd "$BASE_DIR"
echo ">>> [ENTRYPOINT] Ejecutando: ./bin/$MODULO $CONFIG_FILE $@"
exec "./bin/$MODULO" "$CONFIG_FILE" "$@"