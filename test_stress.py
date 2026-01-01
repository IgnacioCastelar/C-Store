import threading
import requests
import random
import string
import time
import sys

# Configuración
GATEWAY_URL = "http://localhost:8000/api/upload"
NUM_THREADS = 50
FILE_SIZE_KB = 100  # Archivos pequeños para forzar muchas operaciones rápidas

# Colores para la consola
GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

def generar_contenido_aleatorio(size_kb):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size_kb * 1024))

def tarea_subida(thread_id):
    filename = f"stress_test_{thread_id}.txt"
    content = generar_contenido_aleatorio(FILE_SIZE_KB)
    
    files = {'file': (filename, content)}
    
    try:
        start_time = time.time()
        response = requests.post(GATEWAY_URL, files=files)
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            print(f"[{thread_id}] {GREEN}SUCCESS{RESET} - {filename} en {elapsed:.2f}s")
        else:
            print(f"[{thread_id}] {RED}FAIL{RESET} - Status: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"[{thread_id}] {RED}ERROR{RESET} - Excepción: {str(e)}")

def main():
    print(f"--- INICIANDO STRESS TEST: {NUM_THREADS} Hilos ---")
    print(f"Target: {GATEWAY_URL}")
    
    threads = []
    
    start_global = time.time()
    
    for i in range(NUM_THREADS):
        t = threading.Thread(target=tarea_subida, args=(i,))
        threads.append(t)
        t.start()
        # Pequeño sleep para no matar el handshake inicial del socket server localmente
        time.sleep(0.05) 
        
    for t in threads:
        t.join()
        
    total_time = time.time() - start_global
    print(f"\n--- TEST FINALIZADO en {total_time:.2f} segundos ---")

if __name__ == "__main__":
    main()