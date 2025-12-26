#include "worker_query.h"
#include "worker_memoria.h"
#include "worker_instrucciones.h"
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#define MIN(a,b) ((a) < (b) ? (a) : (b))

int worker_realizar_flush_completo(t_worker* worker, const char* file, const char* tag) {
    log_info(worker->logger, "## Query: Iniciando FLUSH para %s:%s...", file, tag);
    
    t_memoria_worker* memoria = worker->memoria;
    int bloques_escritos = 0;
    int errores = 0;

    // Bloquear memoria para iterar de forma segura sobre los marcos
    pthread_mutex_lock(&memoria->mutex);

    for (int i = 0; i < memoria->max_frames; i++) {
        t_pagina* frame = &memoria->frames[i];

        // CRITERIO: marco ocupado, pertenecer al mismo File y Tag, y estar Modificado
        if (frame->ocupado && frame->bit_modificado &&
            frame->path_archivo && strcmp(frame->path_archivo, file) == 0 &&
            frame->tag_archivo && strcmp(frame->tag_archivo, tag) == 0) {
            
            log_info(worker->logger, "Query: Acción: ESCRIBIR - Bloque lógico %d - Marco %d", frame->id_bloque, i);

            // Liberamos mutex momentáneamente para I/O de red
            // evita que el Worker se congele si el Storage tarda
            // re-verificamos mínimamente o confiamos en que somos el único hilo ejecutando la query??
            pthread_mutex_unlock(&memoria->mutex);

            // Llamada a Storage para persistir el bloque
            int res = worker_storage_write_block(worker, file, tag, frame->id_bloque, frame->contenido);

            pthread_mutex_lock(&memoria->mutex); // Volvemos a tomar el control

            if (res == 0) {
                // Si el Storage confirmó, limpio bit de modificado. Write-Back completo
                frame->bit_modificado = false; 
                bloques_escritos++;
            } else {
                log_error(worker->logger, "Query: Error al escribir bloque %d en Storage.", frame->id_bloque);
                errores++;
            }
        }
    }
    pthread_mutex_unlock(&memoria->mutex);

    if (errores > 0) {
        return -1; // Si falló al menos un bloque, el flush no es confiable 
    }

    log_info(worker->logger, "## Query - FLUSH finalizado con éxito. Bloques persistidos: %d", bloques_escritos);
    
    return 0;
}

int worker_query_execute_instruction(t_worker* worker, t_instruccion* instr) {
    if (!worker || !instr) return -1;

    pthread_mutex_lock(&worker->mutex_query);
    int id_query_local = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    if (instr->tipo == INSTR_INVALID) {
        log_error(worker->logger, "Instrucción inválida en PC=%d — omitiendo", instr->pc);
        return -1;
    }

    int rc = 0;

    switch (instr->tipo) {
        case INSTR_CREATE: {
            int resultado = worker_storage_send_create(worker, instr->file, instr->tag);

            if (resultado == 0) {
                log_info(worker->logger, "## Query %d: File creado correctamente: %s:%s",
                         id_query_local, instr->file, instr->tag);

                t_archivo_worker* nuevo = archivo_worker_create(instr->file, instr->tag);
                worker_archivo_agregar(worker, nuevo);
            }
            else {
                log_error(worker->logger, "CREATE fallido en Storage para %s:%s", instr->file, instr->tag);
                rc = -1;
            }
            break;
        }
        case INSTR_TRUNCATE: {
            if (instr->size % worker->block_size != 0) {
                log_error(worker->logger, "TRUNCATE inválido: tamaño %u no es múltiplo del tamaño de bloque (%u)",
                    instr->size, worker->block_size);
                return -1;
            }

            int res = worker_storage_truncate(worker, instr->file, instr->tag, instr->size);

            if (res == 0) {
                log_info(worker->logger, "## Query %d: File Truncado %s:%s - Nuevo tamaño: %u",
                    id_query_local, instr->file, instr->tag, instr->size);
            } else {
                log_error(worker->logger, "TRUNCATE fallido en Storage para %s:%s (size=%u)",
                    instr->file, instr->tag, instr->size);
                return -1;
            }
            break;
        }
        case INSTR_WRITE: {
            uint32_t remaining = instr->size;
            uint32_t cursor = 0;

            while (remaining > 0) {
                uint32_t logical_block = (instr->base + cursor) / worker->block_size;
                uint32_t offset        = (instr->base + cursor) % worker->block_size;
                uint32_t can_write     = MIN(worker->block_size - offset, remaining);

                // CORREGIDO: id_query_local
                t_pagina* pagina = memoria_buscar_bloque(
                    worker->memoria,
                    id_query_local, 
                    instr->file,
                    instr->tag,
                    logical_block
                );

                if (pagina) {
                    // Hit
                } else {
                    log_info(worker->logger, "Query %d: - Memoria Miss - File: %s - Tag: %s - Pagina: %u",
                        id_query_local, instr->file, instr->tag, logical_block);

                    void* contenido = worker_storage_read_block(worker, instr->file, instr->tag, logical_block);

                    if (!contenido) {
                        log_error(worker->logger, "WRITE no pudo cargar bloque %u desde Storage", logical_block);
                        return -1;
                    }

                    // CORRECCIÓN: Agregado worker->fd_storage // id_query_local
                    int marco_reemplazado = memoria_agregar_bloque(
                        worker->memoria,
                        id_query_local, 
                        worker->fd_storage, // <--- SOCKET STORAGE AGREGADO
                        instr->file,
                        instr->tag,
                        logical_block,
                        contenido);

                    if (marco_reemplazado >= 0) {
                        log_info(worker->logger, "Query %d: - Memoria Add - File: %s - Tag: %s - Pagina: %u - Marco: %d",
                            id_query_local, instr->file, instr->tag, logical_block, marco_reemplazado);
                    }

                    free(contenido);

                    pagina = memoria_buscar_bloque(worker->memoria, id_query_local, instr->file, instr->tag, logical_block);

                    if (!pagina) {
                        log_error(worker->logger, "After add: no se encontró la página en memoria.");
                        return -1;
                    }
                }

                // Dirección física
                int marco = (int)(pagina - worker->memoria->frames);
                uint32_t direccion = marco * worker->block_size + offset;

                log_info(worker->logger, "Query %d: Acción: ESCRIBIR - Dirección Física: %u - Valor: \"%.*s\"",
                    id_query_local, direccion, (int)can_write, instr->contenido + cursor);

                memcpy(pagina->contenido + offset, instr->contenido + cursor, can_write);

                // CORREGIDO: id_query_local
                memoria_marcar_usado(worker->memoria, id_query_local, instr->file, instr->tag, logical_block, true);

                cursor += can_write;
                remaining -= can_write;

                usleep(worker->config->retardo_memoria * 1000);
            }
            break;
        }
        case INSTR_READ: {

            uint8_t* out = malloc(instr->size);
            if (!out) {
                log_error(worker->logger, "OOM al reservar buffer de READ");
                return -1;
            }

            uint32_t remaining = instr->size;
            uint32_t cursor = 0;

            while (remaining > 0) {
                uint32_t logical_block = (instr->base + cursor) / worker->block_size;
                uint32_t offset        = (instr->base + cursor) % worker->block_size;
                uint32_t can_read      = MIN(worker->block_size - offset, remaining);

                t_pagina* pagina = memoria_buscar_bloque(
                    worker->memoria,
                    id_query_local,
                    instr->file,
                    instr->tag,
                    logical_block
                );

                if (pagina) {
                    // Hit
                } else {
                    log_info(worker->logger, "Query %d: - Memoria Miss - File: %s - Tag: %s - Pagina: %u",
                        id_query_local, instr->file, instr->tag, logical_block);

                    // pedir bloque entero al Storage
                    void* contenido = worker_storage_read_block(worker, instr->file, instr->tag, logical_block);

                    if (!contenido) {
                        log_error(worker->logger, "READ falló al cargar bloque %u desde Storage", logical_block);
                        free(out);
                        return -1;
                    }

                    // CORRECCIÓN: Agregado worker->fd_storage
                    int marco_reemplazado = memoria_agregar_bloque(
                        worker->memoria,
                        id_query_local, 
                        worker->fd_storage, // <--- SOCKET STORAGE AGREGADO
                        instr->file,
                        instr->tag,
                        logical_block,
                        contenido);

                    free(contenido);

                    if (marco_reemplazado >= 0) {
                        log_info(worker->logger, "Query %d: - Memoria Add - File: %s - Tag: %s - Pagina: %u - Marco: %d",
                            id_query_local, instr->file, instr->tag, logical_block, marco_reemplazado);
                    }

                    pagina = memoria_buscar_bloque(worker->memoria, id_query_local, instr->file, instr->tag, logical_block);

                    if (!pagina) {
                        log_error(worker->logger, "After add: no se encontró la página en memoria (imposible).");
                        free(out);
                        return -1;
                    }
                }

                // Dirección física
                int marco = (int)(pagina - worker->memoria->frames);
                uint32_t direccion = marco * worker->block_size + offset;

                memcpy(out + cursor, pagina->contenido + offset, can_read);

                log_info(worker->logger, "Query %d: Acción: LEER - Dirección Física: %u Valor: \"%.*s\"",
                    id_query_local, direccion, (int)can_read, (char*)(out + cursor));
                // Leer del frame
                memcpy(out + cursor, pagina->contenido + offset, can_read);

                memoria_marcar_usado(worker->memoria, id_query_local, instr->file, instr->tag, logical_block, false);

                cursor += can_read;
                remaining -= can_read;

                usleep(worker->config->retardo_memoria * 1000);
            }

            char* text = malloc(instr->size + 1);
            if (!text) { free(out); return -1; }
            memcpy(text, out, instr->size);
            text[instr->size] = '\0';

            log_info(worker->logger, "## Query %d: READ obtenido: %s", id_query_local, text);

            t_paquete* paquete = crear_paquete(OP_RESULTADO_LECTURA);

            uint32_t len_file = strlen(instr->file) + 1;
            agregar_a_paquete(paquete, &len_file, sizeof(uint32_t));
            agregar_a_paquete(paquete, instr->file, len_file);

            uint32_t len_tag = strlen(instr->tag) + 1;
            agregar_a_paquete(paquete, &len_tag, sizeof(uint32_t));
            agregar_a_paquete(paquete, instr->tag, len_tag);

            uint32_t len = instr->size + 1;
            agregar_a_paquete(paquete, &len, sizeof(uint32_t));
            agregar_a_paquete(paquete, text, len);

            enviar_paquete(paquete, worker->fd_master);
            destruir_paquete(paquete);

            free(text);
            free(out);

            break;
        }

        case INSTR_TAG:{
            int r = storage_send_tag(worker, worker->fd_storage, instr->file, instr->tag, instr->file_destino, instr->tag_destino);

            if (r == 0) {
                log_info(worker->logger, "## Query %d: TAG creado %s:%s -> %s:%s",
                    id_query_local, instr->file, instr->tag, instr->file_destino, instr->tag_destino);

                t_archivo_worker* nuevo = archivo_worker_create(instr->file_destino, instr->tag_destino);
                worker_archivo_agregar(worker, nuevo);
            }
            else {
                log_error(worker->logger, "## Query %d: ERROR TAG %s:%s -> %s:%s",
                        id_query_local, instr->file, instr->tag, instr->file_destino, instr->tag_destino);
                rc = -1;
            }
            break;
        }
        case INSTR_COMMIT: {
            int flush_ok = worker_realizar_flush_completo(worker, instr->file, instr->tag);

            if (flush_ok != 0) {
                log_error(worker->logger, "COMMIT fallido: FLUSH previo falló para %s:%s", instr->file, instr->tag);
                rc = -1;
                break;
            }

            int res = storage_send_commit(worker, worker->fd_storage, instr->file, instr->tag);

            if (res == 0)
                log_info(worker->logger, "## Query %d: COMMIT realizado para %s:%s", id_query_local, instr->file, instr->tag);
            else {
                log_error(worker->logger, "COMMIT fallido en Storage para %s:%s", instr->file, instr->tag);
                rc = -1;
            }
            break;
        }

        case INSTR_FLUSH: {
            usleep(worker->config->retardo_memoria * 1000);
            int res = worker_realizar_flush_completo(worker, instr->file, instr->tag);

            if (res == 0) {
                log_info(worker->logger, "## Query %d: Flush completado correctamente para %s:%s", id_query_local, instr->file, instr->tag);
            } else {
                log_error(worker->logger, "FLUSH fallido: error en persistencia de %s:%s", instr->file, instr->tag);
                rc = -1;
            }
            break;
        }
        case INSTR_DELETE: {
            usleep(worker->config->retardo_memoria * 1000);
            int res = worker_storage_delete(worker, instr->file, instr->tag);

            if (res == 0) {
                log_info(worker->logger, "## Query %d: File Eliminado %s:%s", id_query_local, instr->file, instr->tag);
                memoria_eliminar_bloques(worker->memoria, id_query_local, instr->file, instr->tag);
                worker_archivo_eliminar(worker, instr->file, instr->tag);
            }
            else {
                log_error(worker->logger, "DELETE fallido: Storage no pudo eliminar %s:%s", instr->file, instr->tag);
                rc = -1;
            }
            break;
        }
        case INSTR_END: {
            log_info(worker->logger, "## Query %d: END ejecutado (Fin del script)", id_query_local);
            t_paquete* paquete = crear_paquete(OP_QUERY_END);
            enviar_paquete(paquete, worker->fd_master);
            destruir_paquete(paquete);
            break;
        }
        case INSTR_INVALID:{
            log_error(worker->logger, "Instrucción inválida en PC=%d", instr->pc);
            rc = -1;
            break;
        }
    }

    log_info(worker->logger, "## Query %d: - Instruccion realizada: %s (PC=%d)",
             id_query_local, worker_instruccion_nombre_sin_param(instr), instr->pc);
    return rc;
}

int worker_query_execute_all(t_worker* worker, t_list* instrucciones) {
    if (!worker || !instrucciones) return -1;

    pthread_mutex_lock(&worker->mutex_query);
    int id_query_local = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    int pc_actual = -1; // almaceno PC actual
    int pc_final = -1;  // almacena PC final (END o desalojo)

    for (int i = 0; i < list_size(instrucciones); i++) {
        t_instruccion* instr = list_get(instrucciones, i);
        pc_actual = instr->pc;

        // VERIFICACIÓN DE DESALOJO
        pthread_mutex_lock(&worker->mutex_desalojo);
        bool solicitud_desalojo = worker->solicitud_desalojo_pendiente;
        pthread_mutex_unlock(&worker->mutex_desalojo);

        if (solicitud_desalojo) {
            // Si hay desalojo, nos detenemos ANTES de ejecutar esta instrucción.
            // El PC a retomar será el de la instrucción actual.
            pc_final = pc_actual;
            pthread_mutex_lock(&worker->mutex_desalojo);
            worker->pc_guardado_desalojo = pc_final; // Almacenar PC para que el hilo de escucha lo lea
            pthread_mutex_unlock(&worker->mutex_desalojo);
            
            log_info(worker->logger, "## Query %d: Desalojada por pedido del Master", id_query_local);
            // AVISAR al hilo de escucha que ya nos detuvimos
            sem_post(&worker->sem_confirmacion_desalojo);
            return 0; // Salimos de la función (y por tanto muere el hilo detached)
        }

        log_info(worker->logger, "## Query %d: FETCH - Program Counter: %d - %s",
                 id_query_local, instr->pc, worker_instruccion_nombre_sin_param(instr));

        int rc = worker_query_execute_instruction(worker, instr);

        if (rc != 0) {
            log_error(worker->logger, "Query %d: Error ejecutando instruccion %s en PC=%d. Abortando.", 
                      id_query_local, worker_instruccion_nombre_sin_param(instr), instr->pc);
                      
            t_paquete* p = crear_paquete(OP_QUERY_END);
            char* msg_error = string_from_format("FALLO_%s", worker_instruccion_nombre_sin_param(instr));
            uint32_t len = strlen(msg_error) + 1;
            agregar_a_paquete(p, &len, sizeof(uint32_t));
            agregar_a_paquete(p, msg_error, len);
            enviar_paquete(p, worker->fd_master);
            destruir_paquete(p);
            free(msg_error);
            return -1; 
        }

        if (instr->tipo == INSTR_END) {
             pc_final = instr->pc; // Guardar PC de END
             break;
        }
    }
    // Si la query terminó sola, guardamos el último PC
    pthread_mutex_lock(&worker->mutex_desalojo);
    worker->pc_guardado_desalojo = pc_actual;
    pthread_mutex_unlock(&worker->mutex_desalojo);
    // post x si Master manda desalojo justo ahora, evita que el hilo principal quede esperando un hilo muerto.
    sem_post(&worker->sem_confirmacion_desalojo);
    
    return 0;
}

t_archivo_worker* archivo_worker_create(const char* file, const char* tag) {
    t_archivo_worker* a = malloc(sizeof(t_archivo_worker));
    a->file = strdup(file);
    a->tag = strdup(tag);
    a->size = 0;
    a->state = FILE_STATE_WORK_IN_PROGRESS;
    a->paginas = dictionary_create();
    return a;
}

void archivo_worker_destroy(void* archivo_ptr) {
    t_archivo_worker* a = (t_archivo_worker*)archivo_ptr;
    if (!a) return;
    void _destroy_entry(void* value) { free(value); }
    dictionary_destroy_and_destroy_elements(a->paginas, _destroy_entry);
    free(a->file); free(a->tag); free(a);
}

t_archivo_worker* worker_archivo_buscar(t_worker* w, const char* file, const char* tag) {
    pthread_mutex_lock(&w->archivos_mutex);
    t_archivo_worker* result = NULL;
    for (int i = 0; i < list_size(w->archivos); i++) {
        t_archivo_worker* a = list_get(w->archivos, i);
        if (strcmp(a->file, file) == 0 && strcmp(a->tag, tag) == 0) {
            result = a; break;
        }
    }
    pthread_mutex_unlock(&w->archivos_mutex);
    return result;
}

int worker_archivo_eliminar(t_worker* w, const char* file, const char* tag) {
    pthread_mutex_lock(&w->archivos_mutex);
    int result = -1;
    for (int i = 0; i < list_size(w->archivos); i++) {
        t_archivo_worker* a = list_get(w->archivos, i);
        if (strcmp(a->file, file) == 0 && strcmp(a->tag, tag) == 0) {
            // liberar páginas en memoria interna
            void _free_page(char* key, void* value) {
                int frame = *(int*)value;
                if (frame >= 0 && frame < w->memoria->max_frames) {
                    // int frame = *(int*)value; 
                // Podríamos limpiar bit de ocupado aquí si tuviéramos acceso directo
                    w->memoria->frames[frame].ocupado = false;
                    w->memoria->frames[frame].bit_uso = false;
                    w->memoria->frames[frame].bit_modificado = false;
                }
                free(value);
            }
            dictionary_iterator(a->paginas, _free_page);
            list_remove_and_destroy_element(w->archivos, i, archivo_worker_destroy);
            result = 0; break;
        }
    }
    pthread_mutex_unlock(&w->archivos_mutex);
    return result;
}

int worker_archivo_agregar(t_worker* w, t_archivo_worker* archivo) {
    pthread_mutex_lock(&w->archivos_mutex);
    for (int i = 0; i < list_size(w->archivos); i++) {
        t_archivo_worker* a = list_get(w->archivos, i);
        if (strcmp(a->file, archivo->file) == 0 && strcmp(a->tag, archivo->tag) == 0) {
            pthread_mutex_unlock(&w->archivos_mutex); return -1;
        }
    }
    list_add(w->archivos, archivo);
    pthread_mutex_unlock(&w->archivos_mutex);
    return 0;
}