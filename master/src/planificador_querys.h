#ifndef PLANIFICADORQUERYS_H_
#define PLANIFICADORQUERYS_H_

#include<commons/log.h>
#include<commons/config.h>

#include"conexiones_servidor.h"
#include<deserealizacion_master.h>
#include"main_master.h"
#include<commons/temporal.h>
#include<semaphore.h>

extern t_config *config;

typedef enum{
    READY,
    EXEC,
    EXIT
}t_estado_query;

typedef struct{
    char* nombre_query;
    int length_nombre_query;
    int id_query;
    int program_counter;
    t_estado_query estado_actual;
    uint8_t prioridad;
    int fd_socket;
    t_temporal* temporizador;
}t_propQuery;

typedef struct{
    int id_worker;
    bool libre;
    bool cerrada;
    int socket_cliente;
    t_propQuery* query_en_ejecucion;
    //t_motivo_desalojo *motivo;
    sem_t sem_desalojo;
}t_worker_conectada;

typedef enum{
    DESCONEXION,
    PRIORIDAD
}t_motivo_desalojo;




void *ciclo_planificador_corto_plazo(void *arg);
void iniciar_planificador_corto_plazo();
void algoritmo_fifo();
void algoritmo_prioridades();
void ejecutar_en_worker(t_propQuery *query, t_worker_conectada* worker_libre);
void ordernar_segun_prioridad();
void pedido_desalojo(int fd_cliente);
void asignar_query_a_worker();
void *aplicar_aging_a_cola_ready(void *args);
void destruir_query_en_exit(t_propQuery* query);
//void* hilo_aging_daemon(void* arg);
void aplicar_aging(void *elemento);
void desconexion_worker(t_worker_conectada* worker);
void paquete_worker(t_worker_conectada *worker, t_paquete *paquete);
t_propQuery* cambiar_estado_query(t_propQuery *query, t_estado_query nuevo_estado);
t_propQuery* enviar_a_exit(t_propQuery* query);
t_propQuery *crear_query_para_ready(char *nombre, uint8_t prioridad, int fd_conexion);
t_propQuery *buscar_query_por_id(t_list *lista, int idQ);
t_propQuery *buscar_y_remover_query(int query_id);
t_worker_conectada *encontrar_worker_con_query(t_list *lista, int id_query);
t_worker_conectada *encontrar_worker_por_id(t_list* lista, int id_worker);
int algoritmo_a_int(char *desdeLaConfig);
bool comparar_prioridad(void *a, void *b);
char* motivo_to_string(t_motivo_desalojo motivo);
char* estado_a_string(t_estado_query estado);






#endif