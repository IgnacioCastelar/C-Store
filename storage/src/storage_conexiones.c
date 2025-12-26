#include "storage_conexiones.h"
#include <pthread.h>
#include <string.h>
#include <stdint.h> // T-001: Necesario para uint8_t, uint32_t

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
    
    // 1. Recibir identificación del Worker (Protocolo Fix T-001)
    t_workerStorage *paquete_worker = recibir_paquete_id(fd_cliente);

    if (paquete_worker == NULL) {
        log_error(logger, "Error al recibir identificación del WORKER");
        // FIX: Enviar error como paquete estructurado para evitar desync del Worker
        // (Aunque el Worker actual podría esperar un int raw, lo ideal es mantener protocolo)
        send(fd_cliente, &result_error, sizeof(int), 0);
        return NULL;
    }

    paquete_worker->socket_cliente = fd_cliente;

    // 2. Responder con el tamaño de bloque (Handshake)
    // Usamos block_size_global cargado desde Superbloque/Config
    int tam_block = block_size_global;
    
    if (tam_block == 0) {
        log_warning(logger, "CUIDADO: Enviando BlockSize=0 (¿Superbloque no cargado?)");
    }

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

    // FIX T-001: Leer OpCode como 1 Byte (uint8_t)
    uint8_t cod_op;
    if (recv(socket_worker, &cod_op, sizeof(uint8_t), MSG_WAITALL) <= 0) {
        log_error(logger, "Error recibiendo op code en handshake");
        free(paquete->buffer); free(paquete);
        return NULL;
    }
    paquete->codigo_operacion = (op_code)cod_op;

    // FIX T-001: Leer Size como 4 Bytes (uint32_t)
    uint32_t size;
    if (recv(socket_worker, &size, sizeof(uint32_t), MSG_WAITALL) <= 0) {
        log_error(logger, "Error recibiendo size en handshake");
        free(paquete->buffer); free(paquete);
        return NULL;
    }
    paquete->buffer->size = size;

    // Leer Stream
    if (size > 0) {
        paquete->buffer->stream = malloc(size);
        if (recv(socket_worker, paquete->buffer->stream, size, MSG_WAITALL) <= 0) {
            log_error(logger, "Error recibiendo stream en handshake");
            free(paquete->buffer->stream); free(paquete->buffer); free(paquete);
            return NULL;
        }
    } else {
        paquete->buffer->stream = NULL;
    }

    if (paquete->codigo_operacion != HANDSHAKE_WORKER) {
        log_error(logger, "Código incorrecto en handshake: %d (Esperado: %d)", paquete->codigo_operacion, HANDSHAKE_WORKER);
        send(socket_worker, &result_error, sizeof(int), 0);
        eliminar_paquete(paquete); // Esto libera stream, buffer y paquete
        return NULL;
    }

    t_workerStorage *id_worker = paquete_deserializar_s(paquete->buffer);
    
    // Liberamos el paquete temporal (id_worker ya tiene copia de los datos necesarios)
    if (paquete->buffer->stream) free(paquete->buffer->stream);
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

    // Validación extra por si el paquete es más corto de lo esperado
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
        t_paquete *paquete = recibir_paquete(socket); // Ya usa utils correctamente
        if (!paquete) {
            // SECCIÓN CRÍTICA: Decrementar contador
            pthread_mutex_lock(&mutex_conteo_workers);
            cantidad_workers_conectados--;
            int cant_actual = cantidad_workers_conectados;
            pthread_mutex_unlock(&mutex_conteo_workers);

            log_info(logger, "##Se desconecta el Worker %s Cantidad de Workers: %d", worker->id_worker, cant_actual);
            break;
        }

        // Validación de payload
        if (paquete->buffer->size == 0 || !paquete->buffer->stream) {
             // Ops sin payload (como FLUSH a veces) o errores
        }

        // Retardo simulado configurado
        int retardo_op = config_get_int_value(config, "RETARDO_OPERACION");
        usleep(1000 * retardo_op);

        void* stream = paquete->buffer->stream;
        int query_id = -1;
        
        // Extraer Query ID si hay stream suficiente
        if (paquete->buffer->size >= sizeof(int)) {
            memcpy(&query_id, stream, sizeof(int));
            stream += sizeof(int);
        }
        
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
            
            // Calculo seguro del payload restante
            long bytes_header = (void*)stream - (void*)paquete->buffer->stream;
            long tam_payload = paquete->buffer->size - bytes_header;

            if (tam_payload > 0) {
                void* contenido = malloc(tam_payload);
                memcpy(contenido, stream, tam_payload);
                respuesta = op_escribir_bloque(file, tag, bloque_logico_w, contenido, tam_payload, query_id);
                free(contenido);
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
            
            void* datos = op_leer_bloque(file, tag, bloque_logico_r, query_id);
            if (datos) {
                t_paquete* resp = crear_paquete(OP_STORAGE_READ);
                agregar_a_paquete(resp, datos, block_size_global); // Envía bloque completo
                enviar_paquete(resp, socket);
                destruir_paquete(resp);
                free(datos);
            } else {
                // Error en lectura: Enviamos paquete vacío o código error
                // Protocolo: Podríamos enviar un paquete con size 0 o un int error
                // Manteniendo lógica legacy: enviar int error en un paquete
                t_paquete* err_pkg = crear_paquete(OP_ERROR); // Usar OP_ERROR preferiblemente
                int err_code = -1;
                agregar_a_paquete(err_pkg, &err_code, sizeof(int));
                enviar_paquete(err_pkg, socket);
                destruir_paquete(err_pkg);
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
        
        case OP_STORAGE_FLUSH:
             respuesta = 0;
             send(socket, &respuesta, sizeof(int), 0);
             break;

        default:
            log_warning(logger, "Operación desconocida: %d", paquete->codigo_operacion);
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