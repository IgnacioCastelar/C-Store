#ifndef DESEREALIZACIONMASTER_H_
#define DESEREALIZACIONMASTER_H_

#include <commons/log.h>
#include <commons/config.h>

#include "conexiones_servidor.h"
#include "serializacion_envio.h"
#include "paquete.h"

typedef struct{
    char* nombre_query;
    uint32_t length_nombre_query;
    char* modulo;
    uint32_t length_modulo;
    uint8_t prioridad;
}t_handshakeQuery;

typedef struct{
    char* id_worker;
    int len_id_worker;
    char* modulo;
    int len_modulo;
}t_handshakeWorker;


void crear_paquete_y_enviar_worker(int socket_cliente, t_buffer *buffer);
int recibir_id_worker(int socket_worker);
char* deserializar_string(void* stream, int* desplazamiento);
int deserializar_int(void* stream, int* desplazamiento);
int recibir_paquete_pc(t_paquete* paquete);


t_paquete* recibir_paquete_completo(int socket_fd);
t_handshakeWorker *recibir_paquete_handshake_worker(t_paquete* paquete);
t_handshakeQuery *recibir_paquete_handshake_query(t_paquete* paquete);

#endif