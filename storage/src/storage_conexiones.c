#include "storage_conexiones.h"
#include <pthread.h>
#include <string.h>

// Variables globales externas necesarias
extern t_log* logger;
extern t_config* config;
extern int block_size_global; // Usado para el handshake y el buffer de lectura/escritura

// contadores cant. workers
static int cantidad_workers_conectados = 0;
static pthread_mutex_t mutex_conteo_workers = PTHREAD_MUTEX_INITIALIZER;

// --- Prototipos de funciones privadas ---
static t_workerStorage *recibir_id_worker(int fd_cliente);
static t_workerStorage *recibir_paquete_id(int socket_worker);
static t_workerStorage *paquete_deserializar_s(t_buffer *buffer);
static void *atender_worker_storage(void *args);

// --- Implementación Pública ---

void iniciar_conexiones_worker(int socket_server) {
    log_info(logger, "Servidor Storage escuchando conexiones de Workers...");
    
    while (1) {
        int socket_cliente = esperar_cliente(socket_server);
        
        // Realizar Handshake
        t_workerStorage *worker = recibir_id_worker(socket_cliente);
        
        if (worker) {
            // SECCIÓN CRÍTICA: Aumentar contador
            pthread_mutex_lock(&mutex_conteo_workers);
            cantidad_workers_conectados++;
            int cant_actual = cantidad_workers_conectados;
            pthread_mutex_unlock(&mutex_conteo_workers);

            // LOG OBLIGATORIO: Conexión con cantidad
            log_info(logger, "##Se conecta el Worker %s Cantidad de Workers: %d", worker->id_worker, cant_actual);
            
            pthread_t hilo;
            if (pthread_create(&hilo, NULL, atender_worker_storage, worker) != 0) {
                log_error(logger, "Error al crear hilo para Worker %s", worker->id_worker);
                
                // Rollback si falla el hilo
                pthread_mutex_lock(&mutex_conteo_workers);
                cantidad_workers_conectados--;
                pthread_mutex_unlock(&mutex_conteo_workers);

                close(socket_cliente);
                free(worker->id_worker);
                free(worker->modulo);
                free(worker);
            } else {
                pthread_detach(hilo);
            }
        } else {
            log_error(logger, "Falló el handshake con un cliente. Cerrando conexión.");
            close(socket_cliente);
        }
    }
}

// --- Implementaciones Privadas ---

static t_workerStorage *recibir_id_worker(int fd_cliente) {
    int result_ok = 0;
    int result_error = -1;
    
    // 1. Recibir identificación del Worker
    t_workerStorage *paquete_worker = recibir_paquete_id(fd_cliente);

    if (paquete_worker == NULL) {
        log_error(logger, "Error al recibir identificación del WORKER");
        send(fd_cliente, &result_error, sizeof(int), 0);
        return NULL;
    }

    paquete_worker->socket_cliente = fd_cliente;

    // 2. Responder con el tamaño de bloque (Handshake)
    // OPTIMIZACIÓN: Usamos la global en memoria en vez de leer config de disco
    int tam_block = block_size_global;

    t_paquete *paquete = crear_paquete(HANDSHAKE_WORKER);
    agregar_a_paquete(paquete, &tam_block, sizeof(int));
    agregar_a_paquete(paquete, &result_ok, sizeof(int));

    enviar_paquete(paquete, fd_cliente);
    eliminar_paquete(paquete);

    return paquete_worker;
}

static t_workerStorage *recibir_paquete_id(int socket_worker) {
    // Lógica de recepción manual para el handshake específico
    // Nota: Podría refactorizarse usando recibir_paquete de utils, pero respetamos la lógica original funcional
    t_paquete *paquete = malloc(sizeof(t_paquete));
    paquete->buffer = malloc(sizeof(t_buffer));
    int result_error = -1;

    if (recv(socket_worker, &(paquete->codigo_operacion), sizeof(int), 0) <= 0) {
        log_error(logger, "Error recibiendo op code en handshake");
        free(paquete->buffer); free(paquete);
        return NULL;
    }

    if (recv(socket_worker, &(paquete->buffer->size), sizeof(uint32_t), 0) <= 0) {
        log_error(logger, "Error recibiendo size en handshake");
        free(paquete->buffer); free(paquete);
        return NULL;
    }

    paquete->buffer->stream = malloc(paquete->buffer->size);
    if (recv(socket_worker, paquete->buffer->stream, paquete->buffer->size, 0) <= 0) {
        log_error(logger, "Error recibiendo stream en handshake");
        free(paquete->buffer->stream); free(paquete->buffer); free(paquete);
        return NULL;
    }

    if (paquete->codigo_operacion != HANDSHAKE_WORKER) {
        log_error(logger, "Código desconocido en handshake: %d", paquete->codigo_operacion);
        send(socket_worker, &result_error, sizeof(int), 0);
        eliminar_paquete(paquete); // Esto libera stream, buffer y paquete
        return NULL;
    }

    t_workerStorage *id_worker = paquete_deserializar_s(paquete->buffer);
    
    // Liberamos el paquete temporal (id_worker ya tiene copia de los datos necesarios)
    free(paquete->buffer->stream);
    free(paquete->buffer);
    free(paquete);

    return id_worker;
}

static t_workerStorage *paquete_deserializar_s(t_buffer *buffer) {
    t_workerStorage *worker = malloc(sizeof(t_workerStorage));
    void *stream = buffer->stream;

    memcpy(&(worker->len_modulo), stream, sizeof(int));
    stream += sizeof(int);

    worker->modulo = malloc(worker->len_modulo + 1);
    memcpy(worker->modulo, stream, worker->len_modulo);
    worker->modulo[worker->len_modulo] = '\0'; 
    stream += worker->len_modulo;

    memcpy(&(worker->len_id), stream, sizeof(int));
    stream += sizeof(int);

    worker->id_worker = malloc(worker->len_id + 1);
    memcpy(worker->id_worker, stream, worker->len_id);
    worker->id_worker[worker->len_id] = '\0';
    stream += worker->len_id;

    if (buffer->size >= (stream - buffer->stream) + sizeof(int)) {
        memcpy(&(worker->query_id), stream, sizeof(int));
    } else {
        worker->query_id = -1; 
    }

    return worker;
}

static void *atender_worker_storage(void *args) {
    t_workerStorage *worker = (t_workerStorage *)args;
    int socket = worker->socket_cliente;
    
    // Bucle principal de atención
    while (1) {
        t_paquete *paquete = recibir_paquete(socket);
        if (!paquete) {
            // SECCIÓN CRÍTICA: Decrementar contador
            pthread_mutex_lock(&mutex_conteo_workers);
            cantidad_workers_conectados--;
            int cant_actual = cantidad_workers_conectados;
            pthread_mutex_unlock(&mutex_conteo_workers);

            // LOG OBLIGATORIO: Desconexión con cantidad
            log_info(logger, "##Se desconecta el Worker %s Cantidad de Workers: %d", worker->id_worker, cant_actual);
            break;
        }

        if (!paquete->buffer || !paquete->buffer->stream) {
            log_error(logger, "Paquete vacío o corrupto recibido de %s", worker->id_worker);
            eliminar_paquete(paquete);
            continue;
        }

        // Retardo simulado configurado
        int retardo_op = config_get_int_value(config, "RETARDO_OPERACION");
        usleep(1000 * retardo_op);

        void* stream = paquete->buffer->stream;
        int query_id;
        memcpy(&query_id, stream, sizeof(int));
        stream += sizeof(int);
        
        char* file = NULL;
        char* tag  = NULL;
        int respuesta = 0;

        switch (paquete->codigo_operacion) {
        case OP_CREATE:
            file = (char*)stream;
            stream += strlen(file) + 1;
            tag = (char*)stream;
            
            respuesta = op_crear_file_tag(file, tag, query_id);
            send(socket, &respuesta, sizeof(int), 0);
            break;

        case OP_TRUNCATE:
            file = (char*)stream;
            stream += strlen(file) + 1;
            tag = (char*)stream;
            stream += strlen(tag) + 1;
            
            uint32_t nuevo_tamanio;
            memcpy(&nuevo_tamanio, stream, sizeof(uint32_t));
            
            respuesta = op_truncar_file_tag(file, tag, nuevo_tamanio, query_id);
            send(socket, &respuesta, sizeof(int), 0);
            break;

        case OP_STORAGE_WRITE:
            file = (char*)stream;
            stream += strlen(file) + 1;
            tag = (char*)stream;
            stream += strlen(tag) + 1;

            uint32_t bloque_logico_w;
            memcpy(&bloque_logico_w, stream, sizeof(uint32_t));
            stream += sizeof(uint32_t);
    
            // 1. Recibir Payload
            // Calculamos cuánto vino realmente en el paquete (incluyendo posibles \0 de padding)
            int bytes_procesados = (void*)stream - (void*)paquete->buffer->stream;
            int tam_payload = paquete->buffer->size - bytes_procesados;

            // Reservamos memoria + 1 para seguridad extrema
            void* contenido_recibido = malloc(tam_payload + 1);
            if (contenido_recibido) {
                memcpy(contenido_recibido, stream, tam_payload);
                ((char*)contenido_recibido)[tam_payload] = '\0'; // Terminador de seguridad

                // 2. Medir Tamaño Real
                int tam_a_escribir = strlen((char*)contenido_recibido);

                // 3. Escribir (Preservando el fondo de '0' del disco)
                respuesta = op_escribir_bloque(file, tag, bloque_logico_w, contenido_recibido, tam_a_escribir, query_id);
                
                free(contenido_recibido);
            } else {
                log_error(logger, "Fallo malloc en WRITE");
                respuesta = -1;
            }
    
            send(socket, &respuesta, sizeof(int), 0);
            break;
            
        case OP_STORAGE_READ:
            file = (char*)stream;
            stream += strlen(file) + 1;
            tag = (char*)stream;
            stream += strlen(tag) + 1;
            
            uint32_t bloque_logico_r;
            memcpy(&bloque_logico_r, stream, sizeof(uint32_t));
            
            void* datos_leidos = op_leer_bloque(file, tag, bloque_logico_r, query_id);
            
            if (datos_leidos) {
                t_paquete* resp = crear_paquete(OP_STORAGE_READ);
                agregar_a_paquete(resp, datos_leidos, block_size_global);
                enviar_paquete(resp, socket);
                destruir_paquete(resp);
                free(datos_leidos);
            } else {
                t_paquete* paquete_error = crear_paquete(-1);
                
                int basura = 0;
                agregar_a_paquete(paquete_error, &basura, sizeof(int));
                
                enviar_paquete(paquete_error, socket);
                
                eliminar_paquete(paquete_error);
            
                log_error(logger, "##%d - READ: Falló. Paquete de error enviado correctamente.", query_id);
            }
            break;

        case OP_STORAGE_COMMIT:
            file = (char*)stream;
            stream += strlen(file) + 1;
            tag = (char*)stream;
            
            respuesta = op_commit_file_tag(file, tag, query_id);
            send(socket, &respuesta, sizeof(int), 0);
            break;

        case OP_STORAGE_DELETE:
            file = (char*)stream;
            stream += strlen(file) + 1;
            tag = (char*)stream;
            
            respuesta = op_eliminar_tag(file, tag, query_id);
            send(socket, &respuesta, sizeof(int), 0);
            break;

        case OP_STORAGE_TAG:
            {
                char* file_src = (char*)stream;
                stream += strlen(file_src) + 1;
                char* tag_src = (char*)stream;
                stream += strlen(tag_src) + 1;
                char* file_dst = (char*)stream;
                stream += strlen(file_dst) + 1;
                char* tag_dst = (char*)stream;
                
                respuesta = op_crear_tag(file_src, tag_src, file_dst, tag_dst, query_id);
                send(socket, &respuesta, sizeof(int), 0);            
            break;
            }

        case OP_STORAGE_FLUSH:
            file = (char*)stream;
            stream += strlen(file) + 1;
            tag = (char*)stream;
            
            log_info(logger, "##%d - Solicitud de FLUSH para %s:%s", query_id, file, tag);
            
            respuesta = 0;
            send(socket, &respuesta, sizeof(int), 0);
            break;
        
        default:
            log_warning(logger, "Operación desconocida recibida: %d", paquete->codigo_operacion);
            break;
        }

        eliminar_paquete(paquete);
    }

    // Limpieza al desconectar
    if(worker->id_worker) free(worker->id_worker);
    if(worker->modulo) free(worker->modulo);
    free(worker);
    
    return NULL;
}
