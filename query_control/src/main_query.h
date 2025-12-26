#ifndef MAINQUERY_H_
#define MAINQUERY_H_

#include<commons/log.h>
#include<commons/config.h>
#include"conexiones_cliente.h"
#include"serializacion_envio.h"
#include "paquete.h"
#include<commons/string.h>

typedef struct{
    char* nombre_query;
    uint32_t length_nombre_query;
    char* modulo;
    uint32_t length_modulo;
    uint8_t prioridad;
}t_datosHandshake;

t_config* iniciar_config(char* path_config);
t_log* iniciar_logger(char* nombre_query);
void atender_mensajes_master(int conexion);
void terminar_programa(int conexion, t_log* logger, t_config* config);
t_paquete *crear_paquete_handshake_query(t_datosHandshake* datos_enviar);
char* deserializar_string(void* stream, int* desplazamiento);

#endif