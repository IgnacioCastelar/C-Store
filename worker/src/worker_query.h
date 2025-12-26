#ifndef WORKER_QUERY_H
#define WORKER_QUERY_H

#include <commons/collections/list.h>
#include <commons/collections/dictionary.h>
#include "worker.h"
#include "worker_instrucciones.h"
#include "worker_memoria.h"

typedef enum {
    FILE_STATE_WORK_IN_PROGRESS = 0,
    FILE_STATE_COMMITED = 1
} t_file_state;

typedef struct {
    char* file;
    char* tag;
    uint32_t size;
    t_file_state state;
    t_dictionary* paginas;
} t_archivo_worker;

t_archivo_worker* archivo_worker_create(const char* file, const char* tag);
void archivo_worker_destroy(void* archivo_ptr);

int worker_query_execute_all(t_worker* worker, t_list* instrucciones);
int worker_query_execute_instruction(t_worker* worker, t_instruccion* instr);

t_archivo_worker* worker_archivo_buscar(t_worker* w, const char* file, const char* tag);
int worker_archivo_eliminar(t_worker* w, const char* file, const char* tag);
int worker_archivo_agregar(t_worker* w, t_archivo_worker* archivo);

int worker_realizar_flush_completo(t_worker* worker, const char* file, const char* tag);
#endif