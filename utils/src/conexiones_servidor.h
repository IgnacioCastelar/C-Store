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


extern t_log* logger;

void* recibir_buffer(int*, int);

int iniciar_servidor(int);
int esperar_cliente(int);
void recibir_mensaje(int);
int recibir_operacion(int);
t_list *recibir_paquete_servidor(int);

#endif /* UTILS_H_ */