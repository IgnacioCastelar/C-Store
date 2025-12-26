#include "worker_config.h"
#include <stdlib.h>
#include <string.h>

ConfigWorker* worker_config_create(char* path, t_log* logger) {
    t_config* config_file = config_create(path);
    if (!config_file) {
        log_error(logger, "No se pudo abrir el archivo de configuración: %s", path);
        return NULL;
    }

    ConfigWorker* config = malloc(sizeof(ConfigWorker));

    config->ip_master = strdup(config_get_string_value(config_file, "IP_MASTER"));
    config->puerto_master = config_get_int_value(config_file, "PUERTO_MASTER");
    config->ip_storage = strdup(config_get_string_value(config_file, "IP_STORAGE"));
    config->puerto_storage = config_get_int_value(config_file, "PUERTO_STORAGE");
    config->tam_memoria = config_get_int_value(config_file, "TAM_MEMORIA");
    config->retardo_memoria = config_get_int_value(config_file, "RETARDO_MEMORIA");
    config->algoritmo_reemplazo = strdup(config_get_string_value(config_file, "ALGORITMO_REEMPLAZO"));
    config->path_scripts = strdup(config_get_string_value(config_file, "PATH_SCRIPTS"));

    config_destroy(config_file);
    return config;
}

void worker_config_destroy(ConfigWorker* config) {
    if (!config) return;

    free(config->ip_master);
    free(config->ip_storage);
    free(config->algoritmo_reemplazo);
    free(config->path_scripts);
    free(config);
}