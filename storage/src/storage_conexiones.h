#ifndef STORAGE_CONEXIONES_H_
#define STORAGE_CONEXIONES_H_

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <commons/log.h>
#include <commons/config.h>

// CORRECCIÓN: Quitamos "utils/src/" porque el Makefile ya incluye esa ruta
#include "conexiones_servidor.h"
#include "serializacion_envio.h"
#include "paquete.h"

#include "operaciones.h"

// Estructura para pasar argumentos al hilo del worker
typedef struct {
    char* id_worker;
    int len_id;
    char* modulo;
    int len_modulo;
    int query_id;
    int socket_cliente;
} t_workerStorage;

/**
 * @brief Inicia el bucle principal de aceptación de clientes (Workers).
 * @param socket_server Socket del servidor Storage ya en modo listen.
 */
void iniciar_conexiones_worker(int socket_server);

#endif