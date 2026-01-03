#ifndef ESTRUCTURAS_MASTER_H_
#define ESTRUCTURAS_MASTER_H_

#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>             // <--- Necesario para bool
#include <commons/collections/list.h>
#include <commons/collections/dictionary.h>
#include <commons/temporal.h>    // <--- Necesario para t_temporal

// --- DEFINICIÓN DE ESTRUCTURAS (Rescatadas del Planificador) ---

typedef struct {
    char* nombre_query;
    t_temporal* temporizador;
} t_propQuery;

typedef struct {
    int id_worker;
    bool libre;
    bool cerrada;
    int socket_cliente;
    t_propQuery* query_en_ejecucion;
    sem_t sem_desalojo;
} t_worker_conectada;

// --- VARIABLES GLOBALES (EXTERNAS) ---
extern t_list *workers_conectados;
extern t_list *cola_querys_ready;
extern t_list *querys_exec;
extern t_list *querys_exit;

// PERSISTENCIA
extern t_dictionary *indice_archivos;

// Semáforos
extern sem_t sem_inicio_planificacion;
extern sem_t sem_hay_querys;
extern sem_t sem_hay_workers;
extern sem_t sem_avanzar;
extern sem_t sem_evento_planificador;

extern pthread_mutex_t mutex_ready;
extern pthread_mutex_t mutex_exec;
extern pthread_mutex_t mutex_exit;
extern pthread_mutex_t mutex_worker;

// Funciones
void inicializar_estructuras_master();
void destruir_estructuras_master();
void destruir_query(void *elemento);
void destruir_worker(void *elemento);

#endif