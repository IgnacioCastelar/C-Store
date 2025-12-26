#include "worker_memoria.h"
#include <stdlib.h>
#include <string.h> 
#include <commons/log.h>
#include <commons/collections/list.h>
#include <stdbool.h>
#include <stdint.h>
#include "conexiones_cliente.h"
#include "paquete.h"
#include "serializacion_envio.h"


// ===================== DECLARACIONES ESTÁTICAS ======================
static t_pagina* memoria_buscar_bloque_sin_lock(t_memoria_worker* memoria, const char* file, const char* tag, int id_bloque);
static void _mover_pagina_al_final(t_memoria_worker* cache, t_pagina* pagina_a_mover);
static t_pagina* _obtener_victima_lru(t_memoria_worker* cache);

// ===================== FUNCIONES AUXILIARES LRU ======================

// mover un nodo (página) al final de lista LRU
static void _mover_pagina_al_final(t_memoria_worker* cache, t_pagina* pagina_a_mover) {
    int index = -1;
    for (int i = 0; i < list_size(cache->paginas); i++) {
        if (list_get(cache->paginas, i) == pagina_a_mover) {
            index = i;
            break;
        }
    }
    if (index != -1) { // Si la página está en la lista
        list_remove(cache->paginas, index); // Remover de la posición actual
        list_add(cache->paginas, pagina_a_mover); // Agregar al final
    } else {
        // Si no estaba, lo agrego al final
        list_add(cache->paginas, pagina_a_mover);
    }
}

// obtener víctima LRU (el primer nodo de la lista)
static t_pagina* _obtener_victima_lru(t_memoria_worker* cache) {
    if (list_is_empty(cache->paginas)) return NULL; 
    return (t_pagina*)list_get(cache->paginas, 0); // El primer elemento es el LRU
}

// ===================== CREACIÓN Y DESTRUCCIÓN ======================

t_memoria_worker* memoria_crear_cache(int tam_memoria_bytes, int tam_frame_bytes, char* algoritmo, t_log* logger) {
    t_memoria_worker* memoria = malloc(sizeof(t_memoria_worker));
    if (memoria == NULL) return NULL;

    memoria->paginas = list_create();
    if (memoria->paginas == NULL) { free(memoria); return NULL; }

    if (tam_frame_bytes <= 0) {
        list_destroy(memoria->paginas); free(memoria); return NULL;
    }

    memoria->max_frames = tam_memoria_bytes / tam_frame_bytes;
    memoria->frames = calloc(memoria->max_frames, sizeof(t_pagina));
    
    if (memoria->frames == NULL) {
        list_destroy(memoria->paginas); free(memoria); return NULL;
    }

    // Inicializar frames
    for (int i = 0; i < memoria->max_frames; i++) {
        memoria->frames[i].contenido = calloc(1, tam_frame_bytes);
        memoria->frames[i].id_bloque = -1;
        memoria->frames[i].path_archivo = NULL;
        memoria->frames[i].tag_archivo = NULL;
        memoria->frames[i].ocupado = false;
        memoria->frames[i].bit_uso = false;
        memoria->frames[i].bit_modificado = false;
    }

    memoria->tam_frame = tam_frame_bytes;
    memoria->puntero = 0; 
    memoria->algoritmo = strdup(algoritmo); // Guardamos lo que recibimos
    memoria->logger = logger;
    pthread_mutex_init(&memoria->mutex, NULL);

    return memoria;
}

void memoria_destruir_cache(t_memoria_worker* memoria) {
    if (memoria != NULL) {
        if (memoria->frames != NULL) {
            for (int i = 0; i < memoria->max_frames; i++) {
                if (memoria->frames[i].contenido) free(memoria->frames[i].contenido);
                if (memoria->frames[i].path_archivo) free(memoria->frames[i].path_archivo);
                if (memoria->frames[i].tag_archivo) free(memoria->frames[i].tag_archivo);
            }
            free(memoria->frames);
        }
        if (memoria->paginas) list_destroy(memoria->paginas);
        if (memoria->algoritmo) free(memoria->algoritmo);
        
        pthread_mutex_destroy(&memoria->mutex);
        free(memoria);
    }
}

// ===================== OPERACIONES PÚBLICAS ======================

t_pagina* memoria_buscar_bloque(t_memoria_worker* memoria, int id_query, const char* file, const char* tag, int id_bloque) {
    pthread_mutex_lock(&memoria->mutex);
    t_pagina* ret = memoria_buscar_bloque_sin_lock(memoria, file, tag, id_bloque);
    pthread_mutex_unlock(&memoria->mutex);
    return ret;
}

int memoria_agregar_bloque(t_memoria_worker* cache, int id_query, int fd_storage, const char* file, const char* tag, int id_bloque, void* contenido){
    pthread_mutex_lock(&cache->mutex);

    // 1. Buscar marco libre
    for (int i = 0; i < cache->max_frames; i++) {
        if (!cache->frames[i].ocupado) {

            cache->frames[i].id_bloque = id_bloque;

            if (cache->frames[i].path_archivo) free(cache->frames[i].path_archivo);
            cache->frames[i].path_archivo = strdup(file);

            if (cache->frames[i].tag_archivo) free(cache->frames[i].tag_archivo);
            cache->frames[i].tag_archivo = strdup(tag);

            memcpy(cache->frames[i].contenido, contenido, cache->tam_frame);

            cache->frames[i].ocupado = true;
            cache->frames[i].bit_uso = true;
            cache->frames[i].bit_modificado = false;

            if (strcmp(cache->algoritmo, "LRU") == 0) {
                _mover_pagina_al_final(cache, &(cache->frames[i]));
            }

            // LOG ASIGNAR MARCO
            log_info(cache->logger,
                "Query %d: Se asigna el Marco: %d a la Página: %d perteneciente al File: %s Tag: %s",
                id_query, i, id_bloque, file, tag);

            int marco_asignado = i;
            pthread_mutex_unlock(&cache->mutex);
            return marco_asignado;
        }
    }

    // 2. Reemplazo (Pasamos fd_storage)
    int marco_reemplazado = memoria_reemplazar_bloque(cache, id_query, fd_storage, file, tag, id_bloque, contenido);

    pthread_mutex_unlock(&cache->mutex);
    return marco_reemplazado; // marco reemplazado real
}

void memoria_marcar_usado(t_memoria_worker* cache, int id_query, const char* file, const char* tag, int id_bloque, bool modificado) {
    pthread_mutex_lock(&cache->mutex);
    t_pagina* p = memoria_buscar_bloque_sin_lock(cache, file, tag, id_bloque);
    if (p) {
        p->bit_uso = true;
        if (modificado) {
            p->bit_modificado = true;
        }
        if (strcmp(cache->algoritmo, "LRU") == 0) {
             _mover_pagina_al_final(cache, p);
        }
    }
    pthread_mutex_unlock(&cache->mutex);
}

void memoria_eliminar_bloques(t_memoria_worker* memoria, int id_query, const char* file, const char* tag) {
    if (!memoria || !file || !tag) return;

    pthread_mutex_lock(&memoria->mutex);

    for (int i = list_size(memoria->paginas) - 1; i >= 0; i--) {
        t_pagina* p = list_get(memoria->paginas, i);

        if (p->ocupado &&
            p->path_archivo && strcmp(p->path_archivo, file) == 0 &&
            p->tag_archivo  && strcmp(p->tag_archivo,  tag)  == 0) {

            list_remove(memoria->paginas, i); 
        }
    }

    for (int i = 0; i < memoria->max_frames; i++) {
        t_pagina* frame = &memoria->frames[i];

        if (!frame->ocupado) continue;

        if (frame->path_archivo && strcmp(frame->path_archivo, file) == 0 &&
            frame->tag_archivo  && strcmp(frame->tag_archivo,  tag)  == 0) {

            log_info(memoria->logger,
                    "Query %d: Se libera el Marco: %d perteneciente al File: %s Tag: %s",
                    id_query, i, file, tag);

            free(frame->path_archivo);
            free(frame->tag_archivo);

            frame->path_archivo = NULL;
            frame->tag_archivo = NULL;
            frame->id_bloque = -1;
            frame->ocupado = false;
            frame->bit_uso = false;
            frame->bit_modificado = false;
            // No borramos contenido → queda sucio, pero no importa porque ocupado=false
        }
    }
    pthread_mutex_unlock(&memoria->mutex);
}

// ===================== REEMPLAZO Y FLUSH ======================

int memoria_buscar_victima(t_memoria_worker* cache) {
    if (cache->max_frames == 0) return -1;
    
    int es_lru = strcmp(cache->algoritmo, "LRU");
    int es_clock = strcmp(cache->algoritmo, "CLOCK-M");

    if (es_lru == 0) {
        log_warning(cache->logger, "DEBUG DECISION: Entrando en lógica LRU");
        t_pagina* victima = _obtener_victima_lru(cache);
        if (victima != NULL) {
            int idx = (int)(victima - cache->frames);
            // list_remove(cache->paginas, 0); // Ojo con esto si usas la lista para algo más
            return idx;
        }
        return -1;
    } 
    else if (es_clock == 0) {
        log_warning(cache->logger, "DEBUG DECISION: Entrando en lógica CLOCK-M");
        
        // --- COMIENZO LOGICA CLOCK-M ---
        int vueltas = 0;
        bool segunda_pasada = false;

        while (vueltas < 2) {
            int puntero_inicio = cache->puntero;
            do {
                t_pagina* p = &cache->frames[cache->puntero];
                if (!p->ocupado) {
                    int victima = cache->puntero;
                    cache->puntero = (cache->puntero + 1) % cache->max_frames;
                    return victima;
                }
                
                if (!segunda_pasada) {
                    if (!p->bit_uso && !p->bit_modificado) {
                        int victima = cache->puntero;
                        cache->puntero = (cache->puntero + 1) % cache->max_frames;
                        return victima;
                    }
                } else {
                    if (!p->bit_uso && p->bit_modificado) {
                        int victima = cache->puntero;
                        cache->puntero = (cache->puntero + 1) % cache->max_frames;
                        return victima;
                    }
                    if (p->bit_uso) p->bit_uso = false;
                }
                cache->puntero = (cache->puntero + 1) % cache->max_frames;
            } while (cache->puntero != puntero_inicio);

            if (!segunda_pasada) segunda_pasada = true;
            else {
                segunda_pasada = false;
                vueltas++;
            }
        }
        return cache->puntero;
        // --- FIN LOGICA CLOCK-M ---
    }
    
    log_error(cache->logger, "DEBUG ERROR: El algoritmo [%s] no coincide con nada conocido.", cache->algoritmo);
    return -1;
}

int memoria_reemplazar_bloque(t_memoria_worker* memoria, int id_query, int fd_storage, const char* file, const char* tag, int id_bloque_nuevo, void* contenido_nuevo)
{
    // Asume lock tomado
    int idx_victima = memoria_buscar_victima(memoria);
    if (idx_victima < 0) return -2;

    t_pagina* pagina_victima = &memoria->frames[idx_victima];

    // LOG REEMPLAZO
    log_info(memoria->logger,
        "Query %d: Se reemplaza la página %s:%s/%d por la %s:%s/%d",
        id_query,
        pagina_victima->path_archivo, pagina_victima->tag_archivo, pagina_victima->id_bloque,
        file, tag, id_bloque_nuevo);

    // ==============================================================================
    // WRITE-BACK LOGIC (Persistencia de víctima sucia)
    // ==============================================================================
    if (pagina_victima->ocupado && pagina_victima->bit_modificado) {
        log_info(memoria->logger, "Query %d: Victima sucia (Marco %d). Ejecutando FLUSH (Write-Back) al Storage...", id_query, idx_victima);
        
        t_paquete* paquete = crear_paquete(OP_STORAGE_WRITE); 

        agregar_a_paquete(paquete, &id_query, sizeof(int));
        agregar_a_paquete(paquete, (void*)pagina_victima->path_archivo, strlen(pagina_victima->path_archivo) + 1);
        agregar_a_paquete(paquete, (void*)pagina_victima->tag_archivo, strlen(pagina_victima->tag_archivo) + 1);
        
        uint32_t block_index = (uint32_t)pagina_victima->id_bloque;
        agregar_a_paquete(paquete, &block_index, sizeof(uint32_t));
        
        agregar_a_paquete(paquete, pagina_victima->contenido, memoria->tam_frame);

        // 1. Enviamos la petición de escritura
        enviar_paquete(paquete, fd_storage);
        eliminar_paquete(paquete);

        // 2. [CORRECCIÓN] ESPERAR ACK COMO INT (Protocolo Storage)
        // El Storage responde con un int (0 o -1), NO con un paquete completo.
        int ack = 0;
        if (recv(fd_storage, &ack, sizeof(int), MSG_WAITALL) != sizeof(int)) {
             log_error(memoria->logger, "Error crítico: Storage desconectado o respuesta incompleta en Write-Back");
        } else {
             if(ack != 0) {
                 log_error(memoria->logger, "Storage respondió error %d en Write-Back de bloque %d", ack, block_index);
             } else {
                 log_debug(memoria->logger, "Write-Back confirmado (OK). Socket limpio.");
             }
        }

        pagina_victima->bit_modificado = false;
    }

    // Liberar metadatos viejos
    if (pagina_victima->path_archivo) free(pagina_victima->path_archivo);
    if (pagina_victima->tag_archivo) free(pagina_victima->tag_archivo);

    // Guardar nuevos
    pagina_victima->path_archivo = strdup(file);
    pagina_victima->tag_archivo = strdup(tag);
    pagina_victima->id_bloque = id_bloque_nuevo;

    memcpy(pagina_victima->contenido, contenido_nuevo, memoria->tam_frame);

    pagina_victima->ocupado = true;
    pagina_victima->bit_uso = true;
    pagina_victima->bit_modificado = false;

    if (!strcmp(memoria->algoritmo, "LRU"))
        _mover_pagina_al_final(memoria, pagina_victima);

    return idx_victima;
}

void memoria_flush_frame(t_memoria_worker* memoria, t_pagina* pagina) {
    // Deprecado o solo log
    log_info(memoria->logger, "FLUSH: Enviando a Storage %s:%s Bloque %d", 
             pagina->path_archivo, pagina->tag_archivo, pagina->id_bloque);
}

// ===================== AUXILIARES INTERNAS ======================

static t_pagina* memoria_buscar_bloque_sin_lock(t_memoria_worker* memoria, const char* file, const char* tag, int id_bloque) {
    for (int i = 0; i < memoria->max_frames; i++) {
        if (memoria->frames[i].ocupado && 
            memoria->frames[i].id_bloque == id_bloque &&
            memoria->frames[i].path_archivo && 
            strcmp(memoria->frames[i].path_archivo, file) == 0 &&
            memoria->frames[i].tag_archivo &&
            strcmp(memoria->frames[i].tag_archivo, tag) == 0) {
            
            if (strcmp(memoria->algoritmo, "LRU") == 0) {
                 _mover_pagina_al_final(memoria, &(memoria->frames[i]));
            }
            return &(memoria->frames[i]);
        }
    }
    return NULL;
}
