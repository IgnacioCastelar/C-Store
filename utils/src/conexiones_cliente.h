#ifndef CONEXIONESCLIENTE_H_
#define CONEXIONESCLIENTE_H_

#include<stdlib.h>
#include<stdio.h>
#include<signal.h>
#include<unistd.h>
#include<netdb.h>
#include<string.h>
#include<sys/socket.h>
#include<commons/log.h>
#include<commons/config.h>
#include "paquete.h"

extern t_log* logger;

int crear_conexion_cliente(char* ip, int puerto);
int crear_conexion_cliente_worker(char* ip, int puerto, t_log* logger);
void enviar_mensaje(char* mensaje, int socket_cliente);
//t_paquete* crear_paquete(void);
//void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio);
//void enviar_paquete(t_paquete* paquete, int socket_cliente);
void liberar_conexion(int socket_cliente);
void eliminar_paquete(t_paquete* paquete);


void saludar(char* quien);

#endif
