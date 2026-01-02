#include "worker.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <commons/string.h> // Necesario para string_from_format

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <config_file> <ID_Worker>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char* config_filename = argv[1];
    const char* worker_id = argv[2];

    // FIX T-004: Construir ruta relativa ./config/ si no viene completa
    char* config_path;
    if (strstr(config_filename, "/") != NULL) {
        config_path = strdup(config_filename);
    } else {
        config_path = string_from_format("./config/%s", config_filename);
    }

    // Eliminado el sleep(25). 
    // La responsabilidad de esperar al Storage recae en worker_create -> worker_conectar_storage.

    t_worker* worker = worker_create(config_path, worker_id);
    
    if (!worker) {
        fprintf(stderr, "Error inicializando Worker (Ver log)\n");
        free(config_path);
        return EXIT_FAILURE;
    }
    
    free(config_path);

    log_info(worker->logger, "Worker %s esperando queries del Master...", worker->id);

    pthread_t hilo_master;
    pthread_create(&hilo_master, NULL, worker_escuchar_master, worker);
    pthread_detach(hilo_master);

    pthread_t hilo_datos;
    pthread_create(&hilo_datos, NULL, worker_servidor_datos, worker);
    pthread_detach(hilo_datos);

    pause(); // Bloquear main thread

    worker_destroy(worker);
    return EXIT_SUCCESS;
}