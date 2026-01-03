#include "estructuras_master.h"

t_list *workers_conectados;
t_list *cola_querys_ready;
t_list *querys_exec;
t_list *querys_exit;
t_dictionary *indice_archivos; 

sem_t sem_inicio_planificacion;
sem_t sem_hay_querys;
sem_t sem_hay_workers;
sem_t sem_avanzar;
sem_t sem_evento_planificador;

pthread_mutex_t mutex_ready = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_exec = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_exit = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_worker = PTHREAD_MUTEX_INITIALIZER;
//pthread_mutex_t mutex_query = PTHREAD_MUTEX_INITIALIZER;

void destruir_query(void *elemento)
{
    t_propQuery *query = (t_propQuery *)elemento;
    if (query != NULL)
    {
        if (query->nombre_query != NULL) {
            free(query->nombre_query);
        }
        
        if (query->temporizador != NULL)
        {
            temporal_destroy(query->temporizador);
        }
        free(query);
    }
}

void destruir_worker(void *elemento)
{
    t_worker_conectada *worker = (t_worker_conectada *)elemento;
    if (worker != NULL)
    {
        sem_destroy(&worker->sem_desalojo);

        if (worker->socket_cliente > 0)
        {
            close(worker->socket_cliente);
        }
        free(worker);
    }
}

void inicializar_estructuras_master()
{
    workers_conectados = list_create();
    cola_querys_ready = list_create();
    querys_exec = list_create();
    querys_exit = list_create();
    
    // T-009: Inicialización centralizada
    indice_archivos = dictionary_create();

    sem_init(&sem_hay_querys, 0, 0); // arranca en 0 (sin queries)
    sem_init(&sem_hay_workers, 0, 0);
    sem_init(&sem_evento_planificador, 0, 0);
    sem_init(&sem_inicio_planificacion, 0, 0);
    sem_init(&sem_avanzar, 0, 0);
}

void destruir_estructuras_master()
{

    list_destroy_and_destroy_elements(workers_conectados, destruir_worker);
    list_destroy_and_destroy_elements(cola_querys_ready, destruir_query);
    list_destroy_and_destroy_elements(querys_exec, destruir_query);
    list_destroy_and_destroy_elements(querys_exit, destruir_query);
    
    // T-009 FIX: Limpieza profunda con free para evitar Memory Leaks
    if(indice_archivos) dictionary_destroy_and_destroy_elements(indice_archivos, free);

    sem_destroy(&sem_hay_querys);
    sem_destroy(&sem_hay_workers);
    sem_destroy(&sem_evento_planificador);
    sem_destroy(&sem_inicio_planificacion);
    sem_destroy(&sem_avanzar);

    pthread_mutex_destroy(&mutex_ready);
    pthread_mutex_destroy(&mutex_exec);
    pthread_mutex_destroy(&mutex_exit);
    pthread_mutex_destroy(&mutex_worker);
    //pthread_mutex_destroy(&mutex_query);
}