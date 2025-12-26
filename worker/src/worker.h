#ifndef WORKER_H
#define WORKER_H

#include "worker_config.h"
#include <commons/log.h>
#include <commons/collections/list.h>
#include "worker_memoria.h"
#include "worker_conexiones.h"
#include "worker_instrucciones.h"
#include "worker_query.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>

typedef struct t_worker {
    char* id;
    ConfigWorker* config;
    t_log* logger;
    int fd_master;
    int fd_storage;
    int block_size;
    t_memoria_worker* memoria;
    t_list* archivos;
    pthread_mutex_t archivos_mutex;

    volatile bool solicitud_desalojo_pendiente; 
    int pc_guardado_desalojo;
    pthread_mutex_t mutex_desalojo;
    sem_t sem_confirmacion_desalojo; //Semáforo GOAT: espera al hilo de ejecución

    int id_query_actual;
    pthread_mutex_t mutex_query;

} t_worker;

// Estructura para pasar argumentos al hilo de ejecución
typedef struct {
    t_worker* worker;
    t_list* instrucciones;
} t_args_ejecucion;

t_worker* worker_create(const char* path_cfg, const char* id);
void worker_destroy(t_worker* worker);

#endif