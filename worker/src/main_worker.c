#include "worker.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <config_path> <ID_Worker>\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char* config_path = argv[1];
    const char* worker_id = argv[2];

    t_worker* worker = worker_create(config_path, worker_id);
    if (!worker) {
        fprintf(stderr, "Error inicializando Worker\n");
        return EXIT_FAILURE;
    }

    log_info(worker->logger, "Worker %s esperando queries del Master...", worker->id);

    pthread_t hilo_master;
    pthread_create(&hilo_master, NULL, worker_escuchar_master, worker);
    pthread_detach(hilo_master);

    pause();


    worker_destroy(worker);
    return EXIT_SUCCESS;
}