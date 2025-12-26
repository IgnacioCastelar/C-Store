#!/bin/bash

# --- CONFIGURACIÓN ---
# Nombre del archivo config (sin extensión) que está en query_control/config/
CONFIG_FILE="QUERY" 

# Prioridad solicitada por la prueba
PRIORIDAD=20

# Cantidad de instancias por cada script
INSTANCIAS=25

# Ruta al ejecutable (Desde la raíz del proyecto)
BIN_QUERY="./query_control"

# Nombres de los scripts de Aging (Deben coincidir con los que tienes en queries/pruebas/)
# Si tus archivos tienen extensión .txt, agrégala aquí (ej: "SCRIPT_AGING_1.txt")
SCRIPTS=(
    "AGING_1"
    "AGING_2"
    "AGING_3"
    "AGING_4"
)

# --- EJECUCIÓN ---

echo "--- INICIANDO PRUEBA DE ESTABILIDAD GENERAL ---"
echo "Se lanzarán 100 procesos en segundo plano."
echo "-----------------------------------------------"

# Contador global
count=0

# Loop por cada tipo de script (4 tipos)
for script_name in "${SCRIPTS[@]}"; do
    
    echo ">> Lanzando lote de: $script_name"
    
    # Loop para las 25 instancias
    for ((i=1; i<=INSTANCIAS; i++)); do
        
        # Ejecutar en segundo plano (&)
        # Sintaxis: ./programa <config> <script> <prioridad>
        $BIN_QUERY $CONFIG_FILE $script_name $PRIORIDAD &
        
        ((count++))
        
        # Pequeña pausa para no saturar la creación de hilos del SO instantáneamente
        # (Opcional, ayuda a que los logs salgan más ordenados al inicio)
        sleep 0.05
    done
done

echo "-----------------------------------------------"
echo "¡ÉXITO! Se han disparado $count Query Controls."
echo "Los procesos están corriendo en background."
echo "Usa 'htop' o revisa los logs para ver el avance."
echo "Para matar todo en caso de emergencia corre: pkill -f query_control"
echo "-----------------------------------------------"

# Esperar a que terminen todos los procesos hijos antes de devolver el control
# (Opcional: si quieres liberar la terminal, quita el 'wait')
wait
echo "Todas las queries han finalizado."