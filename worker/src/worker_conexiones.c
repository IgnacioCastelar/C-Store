#include "worker_conexiones.h"
#include "conexiones_cliente.h"
#include "handshake.h"
#include "conexiones_servidor.h"
#include "serializacion_envio.h"
#include "worker_query.h"
#include <unistd.h>
#include <string.h>
#include <pthread.h>

// Función del Hilo de Ejecución
void* _ejecutar_query_thread(void* arg) {
    t_args_ejecucion* args = (t_args_ejecucion*)arg;
    t_worker* w = args->worker;
    t_list* instrucciones = args->instrucciones;

    worker_query_execute_all(w, instrucciones);

    worker_instrucciones_destroy_list(instrucciones);
    free(args);
    return NULL;
}

int worker_conectar_master(ConfigWorker* cfg, const char* worker_id, t_log* logger) {
    int fd = crear_conexion_cliente_worker(cfg->ip_master, cfg->puerto_master, logger);
    if (fd < 0) {
        log_error(logger, "No se pudo conectar al Master %s:%d", cfg->ip_master, cfg->puerto_master);
        return -1;
    }
    log_info(logger, "Conectado al Master en %s:%d (fd=%d)", cfg->ip_master, cfg->puerto_master, fd);

    t_paquete* hs = crear_paquete_handshake("WORKER", worker_id);
    enviar_paquete(hs, fd);
    destruir_paquete(hs);

    return fd;
}

int worker_conectar_storage(ConfigWorker* cfg, const char* worker_id, t_log* logger, int* out_block_size) {
    int fd = crear_conexion_cliente_worker(cfg->ip_storage, cfg->puerto_storage, logger);
    if (fd < 0) {
        log_error(logger, "No se pudo conectar al Storage %s:%d", cfg->ip_storage, cfg->puerto_storage);
        return -1;
    }

    log_info(logger, "Conectado al Storage en %s:%d (fd=%d)", cfg->ip_storage, cfg->puerto_storage, fd);

    t_paquete* paquete = crear_paquete_handshake("WORKER", worker_id);
    enviar_paquete(paquete, fd);
    destruir_paquete(paquete);

    t_paquete* resp = recibir_paquete(fd);
    if (!resp) {
        log_error(logger, "Storage no respondió al handshake.");
        return -1;
    }

    if (resp->buffer->size < sizeof(int)) {
        log_error(logger, "Respuesta inválida del Storage (tamaño insuficiente).");
        destruir_paquete(resp);
        return -1;
    }

    int block_size = 0;
    int result_ok = 0;
    int offset = 0;

    memcpy(&block_size, resp->buffer->stream + offset, sizeof(int));
    offset += sizeof(int);

    if (resp->buffer->size >= offset + sizeof(int))
        memcpy(&result_ok, resp->buffer->stream + offset, sizeof(int));

    destruir_paquete(resp);

    if (block_size <= 0) {
        log_error(logger, "Storage envió un BLOCK_SIZE inválido (%d).", block_size);
        return -1;
    }

    if (out_block_size) *out_block_size = block_size;

    log_info(logger, "Handshake exitoso con Storage. Worker=%s | BLOCK_SIZE=%d", worker_id, block_size);
    return fd;
}

void* worker_escuchar_master(void* arg) {
    t_worker* w = (t_worker*)arg;
    log_info(w->logger, "Hilo de escucha al Master iniciado (fd=%d)", w->fd_master);

    while (1) {
        t_paquete* paquete = recibir_paquete(w->fd_master);
        if (!paquete) {
            log_error(w->logger, "Conexión con el Master perdida. Cerrando hilo de escucha.");
            break;
        }

        switch (paquete->codigo_operacion) {
            case OP_EXEC_QUERY: {
                log_info(w->logger, "Recibiendo Query...");
                char* nombre_query = NULL;
                char* path_query_construido = NULL;
                int id_query = -1;
                int start_pc = 0;

                void* stream = paquete->buffer->stream;
                int nombre_len = 0; 
                memcpy(&nombre_len, stream, sizeof(int)); 
                stream += sizeof(int);


                nombre_query = malloc(nombre_len + 1); 
                memcpy(nombre_query, stream, nombre_len); 
                nombre_query[nombre_len] = '\0';
                stream += nombre_len;

                memcpy(&id_query, stream, sizeof(int));
                stream += sizeof(int);

                memcpy(&start_pc, stream, sizeof(int));

                log_info(w->logger, "Master solicita ejecutar Query: %s con ID:%d desde PC=%d",
                                    nombre_query, id_query, start_pc);

                path_query_construido = string_new();
                string_append(&path_query_construido, w->config->path_scripts);
                if (path_query_construido[strlen(path_query_construido) - 1] != '/') {
                    string_append(&path_query_construido, "/");
                }
                string_append(&path_query_construido, nombre_query);

                log_info(w->logger,
                    "## Query %d: Se recibe la Query. El path de operaciones es: %s",
                    id_query, path_query_construido);

                pthread_mutex_lock(&w->mutex_query);
                w->id_query_actual = id_query;
                pthread_mutex_unlock(&w->mutex_query);

                // Inicializar campos de desalojo antes de ejecutar
                pthread_mutex_lock(&w->mutex_desalojo);
                w->solicitud_desalojo_pendiente = false; // Restablecer flag antes de nueva ejecución
                w->pc_guardado_desalojo = -1;          // Restablecer PC guardado

                while(sem_trywait(&w->sem_confirmacion_desalojo) == 0);          
                pthread_mutex_unlock(&w->mutex_desalojo);

                t_list* instrucciones = worker_instrucciones_cargar(path_query_construido, start_pc, w->logger); 
                
                if (instrucciones) {
                    t_args_ejecucion* args = malloc(sizeof(t_args_ejecucion));
                    args->worker = w;
                    args->instrucciones = instrucciones;

                    pthread_t hilo_exec;
                    if (pthread_create(&hilo_exec, NULL, _ejecutar_query_thread, args) != 0) {
                        log_error(w->logger, "Error al crear hilo de ejecucion.");
                        free(args);
                        worker_instrucciones_destroy_list(instrucciones);
                    } else {
                        pthread_detach(hilo_exec);
                        log_info(w->logger, "Query lanzada en hilo secundario.");
                    }
                } else {
                    log_error(w->logger, "Error al cargar instrucciones: %s", path_query_construido); 
                }

                free(nombre_query); 
                free(path_query_construido); 
                break;
            }

            case OP_DESALOJO_QUERY: {
                log_info(w->logger, "Recibiendo solicitud de desalojo del Master...");

                int handshake_recibido = 0;
    
                if (paquete->buffer != NULL && paquete->buffer->size >= sizeof(int)) {
                memcpy(&handshake_recibido, paquete->buffer->stream, sizeof(int));
                log_info(w->logger, "Validacion de desalojo - Handshake/ID recibido: %d", handshake_recibido);
                } else {
                    log_warning(w->logger, "Solicitud de desalojo recibida sin Handshake (payload vacio).");
                }
                
                // 1 Activa flag
                pthread_mutex_lock(&w->mutex_desalojo);
                w->solicitud_desalojo_pendiente = true;
                pthread_mutex_unlock(&w->mutex_desalojo);

                // 2 ESPERA hilo de ejecución (Bloqueante)
                log_warning(w->logger, "Esperando confirmacion de detención del hilo de ejecucion...");
                sem_wait(&w->sem_confirmacion_desalojo);

                // 3 Recupera PC
                int pc_a_enviar = -1;
                pthread_mutex_lock(&w->mutex_desalojo);
                pc_a_enviar = w->pc_guardado_desalojo;
                pthread_mutex_unlock(&w->mutex_desalojo);

                // 4 Envia respuesta
                t_paquete* respuesta_desalojo = crear_paquete(OP_RESPUESTA_DESALOJO);
                t_buffer* buffer_respuesta = buffer_create(0); 
                
                if (buffer_respuesta != NULL) {
                    buffer_add_int(buffer_respuesta, pc_a_enviar); 
                    
                    free(respuesta_desalojo->buffer);
                    
                    respuesta_desalojo->buffer = buffer_respuesta;
                    enviar_paquete(respuesta_desalojo, w->fd_master); 
                    log_info(w->logger, "Enviando PC de desalojo al Master: %d", pc_a_enviar);
                } else {
                     log_error(w->logger, "Error al crear buffer de respuesta de desalojo.");
                }

                if (respuesta_desalojo != NULL) {
                    destruir_paquete(respuesta_desalojo);
                }
                break;
            }

            default:
                log_warning(w->logger, "Operacion desconocida del Master: %d", paquete->codigo_operacion);
                break;
        }

        destruir_paquete(paquete);
    }

    return NULL;
}