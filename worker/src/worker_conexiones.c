#include "worker_conexiones.h"
#include "conexiones_cliente.h"
#include "handshake.h"
#include "conexiones_servidor.h"
#include "serializacion_envio.h"
#include "worker_query.h"
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <openssl/md5.h>

// --- Estructura Privada para pasar argumentos al hilo de Upload ---
typedef struct {
    t_worker* worker;
    int socket_cliente;
} t_args_upload;

// --- Prototipos (Forward Declarations) para evitar errores de compilación ---
void* atender_cliente_gateway(void* arg);
void procesar_bloque_completo(t_worker* w, void* datos, uint32_t tamanio, t_list* lista_md5);

// Helper interno para convertir raw bytes a Hex String (32 chars + null)
void _binario_a_hex_string(unsigned char* digest, char* output) {
    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf(&output[i * 2], "%02x", (unsigned int)digest[i]);
    }
    output[32] = '\0'; // Null terminator de seguridad
}

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

    if (resp->buffer->size >= sizeof(int)) {
        memcpy(&block_size, resp->buffer->stream, sizeof(int));
    }

    if (resp->buffer->size >= sizeof(int) * 2) {
        memcpy(&result_ok, resp->buffer->stream + sizeof(int), sizeof(int));
    }
    // ---------------------------------------------
    // --- FIX DESERIALIZACIÓN (Lectura Directa) ---
    // El payload es simplemente: [BLOCK_SIZE (4 bytes)] [RESULT (4 bytes)]

    destruir_paquete(resp);

    if (block_size <= 0) {
        log_error(logger, "Storage envió un BLOCK_SIZE inválido (%d). ¿FS no formateado?", block_size);
        // Retornamos error pero NO crasheamos, para ver logs
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

// Función auxiliar que encapsula la lógica MD5 + Storage
void procesar_bloque_completo(t_worker* w, void* datos, uint32_t tamanio, t_list* lista_md5) {
    
    // 1. Calcular MD5 (Raw Bytes)
    unsigned char digest[MD5_DIGEST_LENGTH]; // 16 bytes
    
    // SUPRESIÓN DE WARNINGS: MD5 está deprecado en OpenSSL 3.0, pero lo usamos igual.
    #pragma GCC diagnostic push
    #pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    MD5_CTX context;
    MD5_Init(&context);
    MD5_Update(&context, datos, tamanio);
    MD5_Final(digest, &context);
    #pragma GCC diagnostic pop

    // 2. SEGURIDAD: Convertir a Hex String (32 bytes)
    char* hash_hex_string = malloc(33); // 32 chars + \0
    _binario_a_hex_string(digest, hash_hex_string);
    
    // Agregamos a la lista local para reporte final
    list_add(lista_md5, strdup(hash_hex_string)); // Guardamos copia

    log_info(w->logger, "Bloque procesado. MD5 Hex: %s", hash_hex_string);

    // 3. Consultar al Storage (OP_CHECK_MD5)
    t_paquete* check = crear_paquete(OP_CHECK_MD5);
    uint32_t len_hash = 33; // Enviamos el null terminator por seguridad o manejamos 32 fijos
    agregar_a_paquete(check, &len_hash, sizeof(uint32_t));
    agregar_a_paquete(check, hash_hex_string, len_hash);
    
    // Enviamos request (usando conexión persistente fd_storage)
    pthread_mutex_lock(&w->archivos_mutex); // Proteger socket storage si es compartido
    enviar_paquete(check, w->fd_storage);
    destruir_paquete(check);

    // 4. Esperar respuesta del Storage
    t_paquete* resp = recibir_paquete(w->fd_storage);
    
    // FIX: Verificar si resp es NULL (posible desconexión del storage)
    if (resp) {
        if (resp->codigo_operacion == OP_BLOCK_MISSING) {
            log_info(w->logger, "Bloque nuevo detectado. Enviando contenido al Storage...");
            
            // 5. Escribir Bloque (OP_WRITE_BLOCK)
            // Payload: [Len Hash][Hash Hex][Len Data][Data]
            t_paquete* write = crear_paquete(OP_WRITE_BLOCK);
            
            agregar_a_paquete(write, &len_hash, sizeof(uint32_t));
            agregar_a_paquete(write, hash_hex_string, len_hash);
            
            agregar_a_paquete(write, &tamanio, sizeof(uint32_t));
            agregar_a_paquete(write, datos, tamanio);
            
            enviar_paquete(write, w->fd_storage);
            
            // Esperar OK de escritura para sincronismo
            t_paquete* ack = recibir_paquete(w->fd_storage); 
            if(ack) destruir_paquete(ack);
            
            destruir_paquete(write);
        } else {
            log_info(w->logger, "Bloque existente (Deduplicado). Ahorrando escritura.");
        }
        destruir_paquete(resp);
    } else {
        log_error(w->logger, "Error crítico: El Storage no respondió al Check MD5.");
    }
    
    pthread_mutex_unlock(&w->archivos_mutex);
    free(hash_hex_string);
}

void* worker_servidor_datos(void* arg) {
    t_worker* w = (t_worker*)arg;
    // FIX T-007: Pasamos w->logger explícitamente
    int socket_servidor = iniciar_servidor(w->config->puerto_escucha_datos, w->logger);
    
    if (socket_servidor == -1) {
        log_error(w->logger, "Error fatal: No se pudo iniciar el servidor de datos.");
        return NULL;
    }

    log_info(w->logger, "Servidor de Datos escuchando en puerto %d", w->config->puerto_escucha_datos);

    while(1) {
        int socket_cliente = esperar_cliente(socket_servidor, w->logger);
        if (socket_cliente < 0) continue;

        log_info(w->logger, "Conexión entrante de Gateway (Socket %d)", socket_cliente);

        // Usamos la estructura correcta: t_args_upload
        t_args_upload* args = malloc(sizeof(t_args_upload));
        args->worker = w;
        args->socket_cliente = socket_cliente; 
        
        pthread_t hilo_upload;
        pthread_create(&hilo_upload, NULL, atender_cliente_gateway, (void*)args); 
        pthread_detach(hilo_upload);
    }
}

void* atender_cliente_gateway(void* arg) {
    // Recuperar argumentos correctamente casteados
    t_args_upload* args = (t_args_upload*)arg;
    t_worker* w = args->worker;
    int socket_gateway = args->socket_cliente;

    // Buffer acumulador para llegar al BLOCK_SIZE
    void* buffer_bloque = malloc(w->block_size);
    uint32_t bytes_acumulados = 0;
    
    // Lista para guardar los MD5s ordenados de este archivo
    t_list* lista_bloques_md5 = list_create();

    log_info(w->logger, "Iniciando stream de datos con Gateway...");

    while (1) {
        t_paquete* paq = recibir_paquete(socket_gateway);
        if (!paq) {
            log_error(w->logger, "Gateway se desconectó inesperadamente.");
            break;
        }

        if (paq->codigo_operacion == OP_STREAM_DATA) {
            // Lógica de buffering: El Gateway puede mandar chunks de cualquier tamaño
            void* stream_chunk = paq->buffer->stream;
            uint32_t stream_size = paq->buffer->size; // Tamaño del chunk recibido
            uint32_t procesado = 0;

            while (procesado < stream_size) {
                // Cuánto espacio me queda en el bloque actual
                uint32_t espacio_libre = w->block_size - bytes_acumulados;
                // Cuánto voy a copiar ahora
                uint32_t a_copiar = (stream_size - procesado < espacio_libre) ? (stream_size - procesado) : espacio_libre;

                memcpy(buffer_bloque + bytes_acumulados, stream_chunk + procesado, a_copiar);
                bytes_acumulados += a_copiar;
                procesado += a_copiar;

                // Si llenamos el bloque, procesamos
                if (bytes_acumulados == w->block_size) {
                    procesar_bloque_completo(w, buffer_bloque, w->block_size, lista_bloques_md5);
                    bytes_acumulados = 0; // Reset para el siguiente bloque
                }
            }
        } 
        else if (paq->codigo_operacion == OP_STREAM_FINISH) {
            log_info(w->logger, "Stream finalizado. Procesando remanente...");
            // Si quedó algo en el buffer (último bloque incompleto), lo procesamos igual
            if (bytes_acumulados > 0) {
                 procesar_bloque_completo(w, buffer_bloque, bytes_acumulados, lista_bloques_md5);
            }
            
            // TODO: Notificar al Master el éxito (OP_UPLOAD_SUCCESS) con la lista de MD5s
            // enviar_confirmacion_master(w, lista_bloques_md5);

            // Responder OK al Gateway
            t_paquete* ok = crear_paquete(OP_OK);
            enviar_paquete(ok, socket_gateway);
            destruir_paquete(ok);
            destruir_paquete(paq);
            break; // Salir del loop
        }
        
        destruir_paquete(paq);
    }

    free(buffer_bloque);
    // Destruir lista pero NO los elementos si se usaron en otro lado
    list_destroy(lista_bloques_md5); 
    close(socket_gateway);
    free(args);
    return NULL;
}