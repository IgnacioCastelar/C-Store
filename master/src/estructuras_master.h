#ifndef ESTRUCTUAS_MASTER_H_
#define ESTRUCTUAS_MASTER_H_

#include<pthread.h>
#include<semaphore.h>
#include<commons/collections/list.h>
#include<commons/collections/dictionary.h> 
#include"planificador_querys.h"

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

//extern sem_t mutex_workers;
//extern sem_t mutex_querys;

extern pthread_mutex_t mutex_ready;
extern pthread_mutex_t mutex_exec;
extern pthread_mutex_t mutex_exit;
extern pthread_mutex_t mutex_worker;
//extern pthread_mutex_t mutex_query;

void inicializar_estructuras_master();
void destruir_estructuras_master();
void destruir_query(void *elemento);
void destruir_worker(void *elemento);

#endif