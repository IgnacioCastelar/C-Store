#ifndef CONEXIONESSERVIDOR_H_
#define CONEXIONESSERVIDOR_H_

#include<stdio.h>
#include<stdlib.h>
#include<sys/socket.h>
#include<unistd.h>
#include<netdb.h>
#include<commons/log.h>
#include<commons/collections/list.h>
#include<string.h>
#include<assert.h>
#include"conexiones_cliente.h"

// ELIMINADO: extern t_log* logger;  <-- Adiós variable global

void* recibir_buffer(int*, int);

// NUEVAS FIRMAS: Agregamos t_log* logger
int iniciar_servidor(int puerto, t_log* logger); 
int esperar_cliente(int socket_servidor, t_log* logger);

// Helpers (Opcional refactorizarlos todos, o quitarles el log si no son críticos)
void recibir_mensaje(int socket_cliente, t_log* logger);
int recibir_operacion(int socket_cliente); // Simplificado: quitamos log interno o pasamos logger

t_list *recibir_paquete_servidor(int);

#endif /* UTILS_H_ */