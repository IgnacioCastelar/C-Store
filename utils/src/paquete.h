#ifndef PAQUETE_H
#define PAQUETE_H

#include<stdlib.h>
#include<stdio.h>
#include<signal.h>
#include<unistd.h>
#include<netdb.h>
#include<string.h>
#include<sys/socket.h>
#include<commons/log.h>
#include<commons/config.h>

typedef enum {
    HANDSHAKE,
    MENSAJE,
    PAQUETE,
    HANDSHAKE_WORKER,
	HANDSHAKE_QUERY,
	MENSAJE_STORAGE,
	MENSAJE_MASTER,
	ID_WORKER,
	//INSTR_READ,
	OP_CREATE,
	OP_TRUNCATE,
	OP_STORAGE_WRITE,
	OP_STORAGE_READ,
	OP_STORAGE_TAG,
	OP_STORAGE_COMMIT,
	OP_STORAGE_FLUSH,
	OP_STORAGE_DELETE,
	OP_QUERY_END,
	LECTURA_QUERY,
	FIN_QUERY,
	OP_EXEC_QUERY,
	OP_DESALOJO_QUERY,
    OP_RESPUESTA_DESALOJO,
	OP_RESULTADO_LECTURA
} op_code;
typedef struct
{
	uint32_t size;
	uint32_t offset;
	void* stream;
} t_buffer;

typedef struct {
    op_code codigo_operacion;
    t_buffer* buffer;
} t_paquete;

t_paquete* crear_paquete(op_code codigo);
void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio);
void enviar_paquete(t_paquete* paquete, int socket);
t_paquete* recibir_paquete(int socket);
void destruir_paquete(t_paquete* paquete);

#endif