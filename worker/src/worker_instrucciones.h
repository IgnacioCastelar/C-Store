#ifndef WORKER_INSTRUCCIONES_H
#define WORKER_INSTRUCCIONES_H

#include <commons/collections/list.h>
#include <commons/log.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <commons/collections/dictionary.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <commons/string.h>
#include <ctype.h>

typedef struct t_worker t_worker; 

typedef enum {
    INSTR_CREATE,
    INSTR_TRUNCATE,
    INSTR_WRITE,
    INSTR_READ,
    INSTR_TAG,
    INSTR_COMMIT,
    INSTR_FLUSH,
    INSTR_DELETE,
    INSTR_END,
    INSTR_INVALID
} t_tipo_instruccion;


typedef struct {
    t_tipo_instruccion tipo;
    char* file; 
    char* tag;   
    char* file_destino;
    char* tag_destino;
    uint32_t base;    // dirección base (bytes) para READ/WRITE
    uint32_t size;    // tamaño en bytes (para READ/TRUNCATE)
    char* contenido;
    int pc;
} t_instruccion;

// carga instrucciones desde filepath, empezando desde start_pc (pc indicado por master)
t_list* worker_instrucciones_cargar(const char* path_query_construido, int start_pc, t_log* logger);

// parsea una sola linea
t_instruccion* worker_instruccion_parsear_linea(const char* linea, t_log* logger, int pc);

// devuelve el nombre de la instrucción sin parámetros (para logs tipo FETCH)
const char* worker_instruccion_nombre_sin_param(const t_instruccion* instr);

void worker_instruccion_destroy(t_instruccion* instr);
void worker_instrucciones_destroy_list(t_list* instrucciones);

int worker_storage_send_create(t_worker* worker, const char* file, const char* tag);
int worker_storage_truncate(t_worker* worker, const char* file, const char* tag, uint32_t new_size); 
int storage_send_tag(t_worker* worker, int fd_storage, const char* file_src, const char* tag_src,
                     const char* file_dst, const char* tag_dst);
int storage_send_commit(t_worker* worker, int fd_storage, const char* file, const char* tag);
int worker_storage_flush(t_worker* worker, const char* file, const char* tag); 
int worker_storage_delete(t_worker* worker, const char* file, const char* tag);
void* worker_storage_read_block(t_worker* worker, const char* file, const char* tag, uint32_t block_index);
int worker_storage_write_block(t_worker* worker, const char* file, const char* tag, uint32_t block_index, void* contenido);
#endif