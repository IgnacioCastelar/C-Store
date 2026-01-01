#include "storage_conexiones.h"
#include <pthread.h>
#include <string.h>
#include "operaciones.h" //DEBUG... UNA VEZ SOLUCIONADO SE VA
#include <commons/config.h> //DEBUG... UNA VEZ SOLUCIONADO SE VA
#include <stdint.h> // T-001: Necesario para uint8_t, uint32_t

// Variables globales externas necesarias
extern t_log* logger;
extern t_config* config;
extern int block_size_global; // Usado para el handshake y el buffer de lectura/escritura
extern char* punto_montaje_global; //DEBUG, UNA VEZ SOLUCIONADO SE VA

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
    log_info(logger, "Servidor Storage LISTO. Esperando Workers...");
    
    while (1) {
        //Pasamos logger explícitamente
        int socket_cliente = esperar_cliente(socket_server, logger);
        
        // Handshake inicial
        t_workerStorage *worker = recibir_id_worker(socket_cliente);
        
        if (worker) {
            pthread_mutex_lock(&mutex_conteo_workers);
            cantidad_workers_conectados++;
            int cant = cantidad_workers_conectados;
            pthread_mutex_unlock(&mutex_conteo_workers);

            log_info(logger, "## Nuevo Worker Conectado: %s | Total: %d", worker->id_worker, cant);
            
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
            log_error(logger, "Handshake fallido. Rechazando conexión.");
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
        log_error(logger, "Error recibiendo ID del Worker");
        // FIX: Enviar error como paquete estructurado para evitar desync del Worker
        // (Aunque el Worker actual podría esperar un int raw, lo ideal es mantener protocolo)
        send(fd_cliente, &result_error, sizeof(int), 0);
        return NULL;
    }

    paquete_worker->socket_cliente = fd_cliente;

    // 2. Responder con el tamaño de bloque (Handshake)
    // Usamos block_size_global cargado desde Superbloque/Config
    int tam_block = block_size_global;
    
    // --- SAFEGUARD: Validación de integridad de memoria ---
    // Si por alguna razón la variable global se corrompió o no se inició,
    // intentamos recuperarla del disco como último recurso.
    if (tam_block == 0) {
        log_warning(logger, "[Self-Healing] BlockSize en memoria es 0. Intentando recuperar de disco...");
        char* path_sb = string_from_format("%s/superblock.config", punto_montaje_global);
        t_config* sb = config_create(path_sb);
        
        if (sb) {
            tam_block = config_get_int_value(sb, "BLOCK_SIZE");
            block_size_global = tam_block; // Reparamos la memoria
            log_info(logger, "[Self-Healing] BlockSize recuperado: %d", tam_block);
            config_destroy(sb);
        } else {
            // Si falla aquí, sí es un error fatal real
            log_error(logger, "[FATAL] Imposible recuperar BlockSize.");
        }
        free(path_sb);
    }
    if (tam_block == 0) {
        log_warning(logger, "CUIDADO: Enviando BlockSize=0 (¿Superbloque no cargado?)");
    }
    // Responder Handshake
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

    // Lectura manual byte a byte para T-001 (uint8_t)
    uint8_t cod_op;
    if (recv(socket_worker, &cod_op, sizeof(uint8_t), MSG_WAITALL) <= 0) {
        log_error(logger, "Error recibiendo op code en handshake");
        free(paquete->buffer); free(paquete);
        return NULL;
    }
    paquete->codigo_operacion = (op_code)cod_op;

    uint32_t size;
    if (recv(socket_worker, &size, sizeof(uint32_t), MSG_WAITALL) <= 0) {
        log_error(logger, "Error recibiendo size en handshake");
        free(paquete->buffer); free(paquete);
        return NULL;
    }
    paquete->buffer->size = size;

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
        if(paquete->buffer->stream) free(paquete->buffer->stream);
        free(paquete->buffer); free(paquete);
        return NULL;
    }

    t_workerStorage *id_worker = paquete_deserializar_s(paquete->buffer);
    
    // Liberamos el paquete temporal (id_worker ya tiene copia de los datos necesarios)
    if(paquete->buffer->stream) free(paquete->buffer->stream);
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

// =================================================================================
// 🔥 FUNCIÓN CRÍTICA CORREGIDA PARA T-016 (STRESS TEST)
// =================================================================================
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
            int cant = cantidad_workers_conectados;
            pthread_mutex_unlock(&mutex_conteo_workers);
            log_info(logger, "## Worker Desconectado: %s | Restantes: %d", worker->id_worker, cant);
            break;
        }

        // Simulación de retardo (si existe en config)
        if (config_has_property(config, "RETARDO_OPERACION")) {
            int retardo = config_get_int_value(config, "RETARDO_OPERACION");
            if(retardo > 0) usleep(retardo * 1000);
        }

        void* stream = paquete->buffer->stream;
        int query_id = -1;
        
        // 1. Extracción de Header Común (Query ID)
        if (paquete->buffer->size >= sizeof(int)) {
            memcpy(&query_id, stream, sizeof(int));
            stream += sizeof(int); // AVANZAMOS EL STREAM 4 BYTES
        }
        
        // Variables auxiliares
        char* file = NULL;
        char* tag  = NULL;
        int respuesta = 0;

        switch (paquete->codigo_operacion) {
        
        // --- OPERACIONES LEGACY (Sin Cambios) ---
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

        case OP_STORAGE_WRITE: // Escritura Legacy (No MD5)
            file = (char*)stream; stream += strlen(file) + 1;
            tag = (char*)stream; stream += strlen(tag) + 1;
            uint32_t blk_w;
            memcpy(&blk_w, stream, sizeof(uint32_t));
            stream += sizeof(uint32_t);
            
            long header_size = (void*)stream - (void*)paquete->buffer->stream;
            long payload_size = paquete->buffer->size - header_size;

            if (payload_size > 0) {
                void* contenido = malloc(payload_size);
                memcpy(contenido, stream, payload_size);
                respuesta = op_escribir_bloque(file, tag, blk_w, contenido, payload_size, query_id);
                free(contenido);
            } else respuesta = -1;
            
            send(socket, &respuesta, sizeof(int), 0);
            break;
            
        case OP_STORAGE_READ:
            file = (char*)stream; stream += strlen(file) + 1;
            tag = (char*)stream; stream += strlen(tag) + 1;
            uint32_t blk_r;
            memcpy(&blk_r, stream, sizeof(uint32_t));
            
            void* datos = op_leer_bloque(file, tag, blk_r, query_id);
            if (datos) {
                t_paquete* resp = crear_paquete(OP_STORAGE_READ);
                agregar_a_paquete(resp, datos, block_size_global);
                enviar_paquete(resp, socket);
                destruir_paquete(resp);
                free(datos);
            } else {
                // Error en lectura: Enviamos paquete vacío o código error
                // Protocolo: Podríamos enviar un paquete con size 0 o un int error
                // Manteniendo lógica legacy: enviar int error en un paquete
                t_paquete* err = crear_paquete(OP_ERROR);
                int err_code = -1; // Payload mínimo para que no explote
                agregar_a_paquete(err, &err_code, sizeof(int)); 
                enviar_paquete(err, socket);
                destruir_paquete(err);
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

        // --- OPERACIONES NUEVAS (FIX T-016) ---
        
        case OP_CHECK_MD5:
            // Stream ya avanzó 4 bytes (QueryID). Quedan los 32 del MD5.
            if ((paquete->buffer->size - sizeof(int)) >= 32) {
                char md5_hex[33]; // +1 para \0
                memcpy(md5_hex, stream, 32);
                md5_hex[32] = '\0'; // Null-terminate para manejo seguro de strings

                int existe = op_verificar_bloque_md5(md5_hex, query_id);
                
                // RESPUESTA OBLIGATORIA (ACK)
                t_paquete* resp_md5 = crear_paquete((existe == 1) ? OP_BLOCK_EXIST : OP_BLOCK_MISSING);
                // Agregamos un int dummy para consistencia, aunque no es estricto
                int dummy = 0;
                agregar_a_paquete(resp_md5, &dummy, sizeof(int));
                enviar_paquete(resp_md5, socket);
                destruir_paquete(resp_md5);
                
            } else {
                log_error(logger, "##%d - CHECK_MD5: Payload inválido", query_id);
                // Incluso en error, respondemos para no freezar al worker
                t_paquete* err = crear_paquete(OP_ERROR);
                enviar_paquete(err, socket);
                destruir_paquete(err);
            }
            break;

        case OP_WRITE_BLOCK:
            // Protocolo: [QueryID (4)] + [MD5 (32)] + [DATA (N)]
            // Stream ya está después del QueryID.
            if ((paquete->buffer->size - sizeof(int)) >= 32) {
                char md5_write[33];
                memcpy(md5_write, stream, 32);
                md5_write[32] = '\0';
                stream += 32; // Avanzamos puntero sobre MD5

                // [FIX CRÍTICO] Cálculo correcto del tamaño de datos
                // Tamaño Total - (QueryID 4 bytes) - (MD5 32 bytes)
                int data_size = paquete->buffer->size - sizeof(int) - 32;
                
                if (data_size > 0) {
                    if (op_escribir_bloque_md5_safe(md5_write, stream, data_size, query_id) == 0) {
                        t_paquete* resp_ok = crear_paquete(OP_OK);
                        enviar_paquete(resp_ok, socket); // ACK DE ÉXITO
                        destruir_paquete(resp_ok);
                    } else {
                        t_paquete* resp_err = crear_paquete(OP_ERROR);
                        enviar_paquete(resp_err, socket); // ACK DE FALLO (Disco lleno, etc)
                        destruir_paquete(resp_err);
                    }
                } else {
                     log_error(logger, "##%d - WRITE_BLOCK: Data size 0 o negativo", query_id);
                     t_paquete* resp_err = crear_paquete(OP_ERROR);
                     enviar_paquete(resp_err, socket);
                     destruir_paquete(resp_err);
                }
            } else {
                log_error(logger, "##%d - WRITE_BLOCK: Payload insuficiente para MD5", query_id);
                t_paquete* resp_err = crear_paquete(OP_ERROR);
                enviar_paquete(resp_err, socket);
                destruir_paquete(resp_err);
            }
            break;

        default:
            log_warning(logger, "Operación desconocida: %d", paquete->codigo_operacion);
            break;
        }

        eliminar_paquete(paquete);
    }

    // Limpieza
    if(worker->id_worker) free(worker->id_worker);
    if(worker->modulo) free(worker->modulo);
    free(worker);
    
    return NULL;
}