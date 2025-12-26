#ifndef WORKER_CONFIG_H
#define WORKER_CONFIG_H

#include <commons/config.h>
#include <commons/log.h>
#include <stdbool.h>

typedef struct {
    char* ip_master;
    int puerto_master;
    char* ip_storage;
    int puerto_storage;
    int tam_memoria;
    int retardo_memoria;
    char* algoritmo_reemplazo;
    char* path_scripts;
} ConfigWorker;

ConfigWorker* worker_config_create(char* path, t_log* logger);
void worker_config_destroy(ConfigWorker* config);

#endif