#ifndef SERIALIZACIONENVIO_H_
#define SERIALIZACIONENVIO_H_

#include<stdlib.h>
#include<stdio.h>
#include<signal.h>
#include<unistd.h>
#include<netdb.h>
#include<string.h>
#include<commons/log.h>
#include<commons/config.h>
#include"conexiones_cliente.h"

uint32_t buffer_read_uint32(t_buffer *buffer);  // Agregar esta línea

void crear_paquete_y_enviar_master(int socket_cliente,op_code codigo, t_buffer *buffer); // se saco el parametro codigo de operacion

t_buffer *buffer_create(uint32_t size);
void buffer_destroy(t_buffer *buffer);
void buffer_add(t_buffer *buffer, void *data, uint32_t data_size);
void *buffer_read(t_buffer *buffer, uint32_t data_size);

void buffer_add_int(t_buffer *buffer, int data);
int buffer_read_int(t_buffer *buffer);

void buffer_add_uint32(t_buffer *buffer, uint32_t data);
void buffer_add_uint8(t_buffer *buffer, uint8_t data);

uint8_t buffer_read_uint8(t_buffer *buffer);
void buffer_add_string(t_buffer *buffer, uint32_t length, char *string);
char *buffer_read_string(t_buffer *buffer, uint32_t *length);



#endif