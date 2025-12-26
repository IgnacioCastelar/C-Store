#ifndef WORKER_CONEXIONES_H
#define WORKER_CONEXIONES_H

#include <commons/log.h>
#include "worker_config.h"
#include "worker_instrucciones.h"
#include "worker_query.h"
#include "paquete.h"
#include "serializacion_envio.h"

int worker_conectar_master(ConfigWorker* cfg, const char* worker_id, t_log* logger);

int worker_conectar_storage(ConfigWorker* cfg, const char* worker_id, t_log* logger, int* out_block_size);

void* worker_escuchar_master(void* arg);

#endif