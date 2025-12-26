#ifndef MAINMASTER_H_
#define MAINMASTER_H_

#include<commons/log.h>
#include<commons/config.h>
#include"conexiones_servidor.h"
#include<deserealizacion_master.h>
#include<pthread.h>
#include<planificador_querys.h>
#include<semaphore.h>
#include "estructuras_master.h"
#include<commons/temporal.h>


extern int contador_workers_libres;
extern int tiempo_aging;
extern int contador_workers;

void *atender_querys(void* query);
void *atender_worker(void *args);
void* iniciar_conexiones(void* socket_server);
void mostrar_lista_workers(t_list* lista);
void liberar_worker(int idQ);
void mostrar_lista_querys(t_list* lista);


//void iterator(char* value);

#endif