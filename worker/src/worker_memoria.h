#ifndef WORKER_MEMORIA_H_
#define WORKER_MEMORIA_H_

#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <pthread.h>
#include <stdbool.h>

typedef struct {
    int id_bloque;          // Número de bloque lógico
    // Identif. dueño del marco
    char* path_archivo;
    char* tag_archivo;
    void* contenido;        // Puntero al contenido real (malloc)
    
    bool ocupado;
    bool bit_uso;           // Para algoritmos de reemplazo
    bool bit_modificado;    // Para Write-Back
    int marco;              // Indice del marco físico (opcional)
} t_pagina;

/* memoria propiamente Worker*/
typedef struct {
    int tam_memoria;
    int tam_frame;          // Igual a block_size
    int max_frames;
    char* algoritmo;        // "LRU" o "CLOCK"
    
    t_pagina* frames;       // Array de marcos
    t_list* paginas;        // Lista auxiliar para LRU
    
    int puntero;            // Puntero Clock
    
    pthread_mutex_t mutex;
    t_log* logger;
} t_memoria_worker;

// Funciones
t_memoria_worker* memoria_crear_cache(int tam_memoria, int tam_block, char* algoritmo, t_log* logger);
void memoria_destruir_cache(t_memoria_worker* memoria);

int memoria_agregar_bloque(t_memoria_worker* cache, int id_query, int fd_storage, const char* file, const char* tag, int id_bloque, void* contenido);

t_pagina* memoria_buscar_bloque(t_memoria_worker* memoria, int id_query, const char* file, const char* tag, int id_bloque);

void memoria_marcar_usado(t_memoria_worker* cache, int id_query, const char* file, const char* tag, int id_bloque, bool modificado);

void memoria_eliminar_bloques(t_memoria_worker* memoria, int id_query, const char* file, const char* tag);

int memoria_reemplazar_bloque(t_memoria_worker* memoria, int id_query, int fd_storage, const char* file, const char* tag, int id_bloque_nuevo, void* contenido_nuevo);

int memoria_buscar_victima(t_memoria_worker* memoria);
void memoria_flush_frame(t_memoria_worker* memoria, t_pagina* pagina);

#endif /* WORKER_MEMORIA_H_ */