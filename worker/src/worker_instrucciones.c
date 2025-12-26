#include "worker_instrucciones.h"
#include "worker.h"

static char* str_trim(char* s) {
    if (!s) return s;
    // trim left
    while(isspace((unsigned char)*s)) s++;
    if (*s == 0) return s;
    // trim right
    char* end = s + strlen(s) - 1;
    while(end > s && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return s;
}

// divide token del formato FILE:TAG en file_out y tag_out (strdup) 
// devuelve 0 si ok, -1 si formato inválido 
static int split_file_tag(const char* token, char** file_out, char** tag_out) {
    if (!token) return -1;
    const char* colon = strchr(token, ':');
    if (!colon) return -1;
    size_t file_len = colon - token;
    size_t tag_len = strlen(token) - file_len - 1;
    if (file_len == 0 || tag_len == 0) return -1;
    *file_out = strndup(token, file_len);
    *tag_out  = strndup(colon + 1, tag_len);
    return 0;
}

const char* worker_instruccion_nombre_sin_param(const t_instruccion* instr) {
    if (!instr) return "INVALID";
    switch (instr->tipo) {
        case INSTR_CREATE:   return "CREATE";
        case INSTR_TRUNCATE: return "TRUNCATE";
        case INSTR_WRITE:    return "WRITE";
        case INSTR_READ:     return "READ";
        case INSTR_TAG:      return "TAG";
        case INSTR_COMMIT:   return "COMMIT";
        case INSTR_FLUSH:    return "FLUSH";
        case INSTR_DELETE:   return "DELETE";
        case INSTR_END:      return "END";
        default:             return "INVALID";
    }
}

t_instruccion* worker_instruccion_parsear_linea(const char* linea_orig, t_log* logger, int pc) {
    if (!linea_orig) return NULL;
    // copiar porque strtok modifica
    char* copia = strdup(linea_orig);
    char* linea = str_trim(copia);
    if (!linea || *linea == '\0') { free(copia); return NULL; }

    char* saveptr = NULL;
    char* token = strtok_r(linea, " \t\n", &saveptr);
    if (!token) { free(copia); return NULL; }

    t_instruccion* instr = calloc(1, sizeof(t_instruccion));
    instr->pc = pc;
    instr->base = -1;
    instr->size = -1;

    if (strcasecmp(token, "CREATE") == 0) {
        instr->tipo = INSTR_CREATE;
        char* filetag = strtok_r(NULL, " \t\n", &saveptr);
        if (!filetag || split_file_tag(filetag, &instr->file, &instr->tag) != 0) {
            log_error(logger, "Parse error CREATE en linea %d", pc);
            instr->tipo = INSTR_INVALID;
        }
    }
    else if (strcasecmp(token, "TRUNCATE") == 0) {
        instr->tipo = INSTR_TRUNCATE;
        char* filetag = strtok_r(NULL, " \t\n", &saveptr);
        char* size_s   = strtok_r(NULL, " \t\n", &saveptr);
        if (!filetag || !size_s || split_file_tag(filetag, &instr->file, &instr->tag) != 0) {
            log_error(logger, "Parse error TRUNCATE en linea %d", pc);
            instr->tipo = INSTR_INVALID;
        } else {
            instr->size = strtoll(size_s, NULL, 10);
        }
    }
    else if (strcasecmp(token, "WRITE") == 0) {
        instr->tipo = INSTR_WRITE;
        char* filetag = strtok_r(NULL, " \t\n", &saveptr);
        char* base_s  = strtok_r(NULL, " \t\n", &saveptr);
        char* contenido = strtok_r(NULL, "\n", &saveptr);

        if (!filetag || !base_s || !contenido ||
            split_file_tag(filetag, &instr->file, &instr->tag) != 0)
        {
            log_error(logger, "Parse error WRITE en linea %d", pc);
            instr->tipo = INSTR_INVALID;
        } else {
            instr->base = strtoll(base_s, NULL, 10);

            char* cont_trim = str_trim(contenido);

            // Quitar comillas si existen
            if (cont_trim[0] == '"' && cont_trim[strlen(cont_trim)-1] == '"') {
                cont_trim[strlen(cont_trim)-1] = '\0'; // quitar comilla final
                cont_trim++; // saltar comilla inicial
            }

            instr->contenido = strdup(cont_trim);
            instr->size = strlen(instr->contenido);
        }
    }
    else if (strcasecmp(token, "READ") == 0) {
        instr->tipo = INSTR_READ;
        char* filetag = strtok_r(NULL, " \t\n", &saveptr);
        char* base_s  = strtok_r(NULL, " \t\n", &saveptr);
        char* size_s  = strtok_r(NULL, " \t\n", &saveptr);
        if (!filetag || !base_s || !size_s || split_file_tag(filetag, &instr->file, &instr->tag) != 0) {
            log_error(logger, "Parse error READ en linea %d", pc);
            instr->tipo = INSTR_INVALID;
        } else {
            instr->base = strtoll(base_s, NULL, 10);
            instr->size = strtoll(size_s, NULL, 10);
        }
    }
    else if (strcasecmp(token, "TAG") == 0) {
        instr->tipo = INSTR_TAG;
        char* origen = strtok_r(NULL, " \t\n", &saveptr);
        char* destino= strtok_r(NULL, " \t\n", &saveptr);
        if (!origen || !destino) {
            log_error(logger, "Parse error TAG (falta params) linea %d", pc);
            instr->tipo = INSTR_INVALID;
        } else {
            if (split_file_tag(origen, &instr->file, &instr->tag) != 0 ||
                split_file_tag(destino, &instr->file_destino, &instr->tag_destino) != 0) {
                log_error(logger, "Parse error TAG (format) linea %d", pc);
                instr->tipo = INSTR_INVALID;
            }
        }
    }
    else if (strcasecmp(token, "COMMIT") == 0) {
        instr->tipo = INSTR_COMMIT;
        char* filetag = strtok_r(NULL, " \t\n", &saveptr);
        if (!filetag || split_file_tag(filetag, &instr->file, &instr->tag) != 0) {
            log_error(logger, "Parse error COMMIT linea %d", pc);
            instr->tipo = INSTR_INVALID;
        }
    }
    else if (strcasecmp(token, "FLUSH") == 0) {
        instr->tipo = INSTR_FLUSH;
        char* filetag = strtok_r(NULL, " \t\n", &saveptr);
        if (!filetag || split_file_tag(filetag, &instr->file, &instr->tag) != 0) {
            log_error(logger, "Parse error FLUSH linea %d", pc);
            instr->tipo = INSTR_INVALID;
        }
    }
    else if (strcasecmp(token, "DELETE") == 0) {
        instr->tipo = INSTR_DELETE;
        char* filetag = strtok_r(NULL, " \t\n", &saveptr);
        if (!filetag || split_file_tag(filetag, &instr->file, &instr->tag) != 0) {
            log_error(logger, "Parse error DELETE linea %d", pc);
            instr->tipo = INSTR_INVALID;
        }
    }
    else if (strcasecmp(token, "END") == 0) {
        instr->tipo = INSTR_END;
    }
    else {
        instr->tipo = INSTR_INVALID;
        log_error(logger, "Instrucción desconocida '%s' en linea %d", token, pc);
    }

    free(copia);
    return instr;
}

t_list* worker_instrucciones_cargar(const char* path_query_construido, int start_pc, t_log* logger) {
    if (!path_query_construido) return NULL;
    FILE* f = fopen(path_query_construido, "r");
    if (!f) {
        log_error(logger, "No se pudo abrir script: %s", path_query_construido);
        return NULL;
    }

    t_list* lista = list_create();
    char* line = NULL;
    size_t len = 0;
    ssize_t read;
    int linea_num = 0;

    while ((read = getline(&line, &len, f)) != -1) {
        if (linea_num < start_pc) {
            linea_num++;
            continue;
        }

        t_instruccion* instr = worker_instruccion_parsear_linea(line, logger, linea_num);
        if (instr) {
            list_add(lista, instr);
        } else {
            // linea vacía -> ignorar
        }
        linea_num++;
    }

    free(line);
    fclose(f);
    return lista;
}

void worker_instruccion_destroy(t_instruccion* instr) {
    if (!instr) return;
    free(instr->file);
    free(instr->tag);
    free(instr->file_destino);
    free(instr->tag_destino);
    free(instr->contenido);
    free(instr);
}

void worker_instrucciones_destroy_list(t_list* instrucciones) {
    if (!instrucciones) return;
    for (int i = 0; i < list_size(instrucciones); i++) {
        t_instruccion* instr = list_get(instrucciones, i);
        worker_instruccion_destroy(instr);
    }
    list_destroy(instrucciones);
}


int worker_storage_send_create(t_worker* worker, const char* file, const char* tag) {

    pthread_mutex_lock(&worker->mutex_query);
    int qid = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    t_paquete* paquete = crear_paquete(OP_CREATE);

    agregar_a_paquete(paquete, &qid, sizeof(int));
    agregar_a_paquete(paquete, (void*)file, strlen(file) + 1);
    agregar_a_paquete(paquete, (void*)tag, strlen(tag) + 1);

    enviar_paquete(paquete, worker->fd_storage);
    destruir_paquete(paquete);

    int respuesta = -1;
    recv(worker->fd_storage, &respuesta, sizeof(int), MSG_WAITALL);
    return respuesta;
}


int worker_storage_truncate(t_worker* worker, const char* file, const char* tag, uint32_t new_size) {
    if (!worker || worker->fd_storage <= 0) return -1;

    pthread_mutex_lock(&worker->mutex_query);
    int qid = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    t_paquete* paquete = crear_paquete(OP_TRUNCATE);

    agregar_a_paquete(paquete, &qid, sizeof(int));
    agregar_a_paquete(paquete, (void*)file, strlen(file) + 1);
    agregar_a_paquete(paquete, (void*)tag, strlen(tag) + 1);
    agregar_a_paquete(paquete, &new_size, sizeof(uint32_t));

    enviar_paquete(paquete, worker->fd_storage);
    destruir_paquete(paquete);

    int result = -1;
    recv(worker->fd_storage, &result, sizeof(int), 0);

    return result;
}

int storage_send_tag(t_worker* worker, int fd_storage, const char* file_src, const char* tag_src,
                     const char* file_dst, const char* tag_dst) {

    pthread_mutex_lock(&worker->mutex_query);
    int qid = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    t_paquete* p = crear_paquete(OP_STORAGE_TAG);

    agregar_a_paquete(p, &qid, sizeof(int));
    agregar_a_paquete(p, (void*)file_src, strlen(file_src)+1);
    agregar_a_paquete(p, (void*)tag_src,  strlen(tag_src)+1);
    agregar_a_paquete(p, (void*)file_dst, strlen(file_dst)+1);
    agregar_a_paquete(p, (void*)tag_dst,  strlen(tag_dst)+1);

    enviar_paquete(p, fd_storage);
    destruir_paquete(p);

    int res = -1;
    recv(fd_storage, &res, sizeof(int), MSG_WAITALL);
    return res;
}

int storage_send_commit(t_worker* worker, int fd, const char* file, const char* tag) {

    pthread_mutex_lock(&worker->mutex_query);
    int qid = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    t_paquete* p = crear_paquete(OP_STORAGE_COMMIT);

    agregar_a_paquete(p, &qid, sizeof(int));    
    agregar_a_paquete(p, (void*)file, strlen(file)+1);
    agregar_a_paquete(p, (void*)tag,  strlen(tag)+1);

    enviar_paquete(p, fd);
    destruir_paquete(p);

    int res;
    recv(fd, &res, sizeof(int), MSG_WAITALL);
    return res;
}

int worker_storage_flush(t_worker* worker, const char* file, const char* tag) {
    t_paquete* paquete = crear_paquete(OP_STORAGE_FLUSH);

    agregar_a_paquete(paquete, (void*)file, strlen(file) + 1);
    agregar_a_paquete(paquete, (void*)tag, strlen(tag) + 1);

    enviar_paquete(paquete, worker->fd_storage);
    destruir_paquete(paquete);

    int res;
    if (recv(worker->fd_storage, &res, sizeof(int), MSG_WAITALL) <= 0)
        return -1;

    return res;
}

int worker_storage_delete(t_worker* worker, const char* file, const char* tag) {
    if (!worker || worker->fd_storage <= 0) return -1;

    pthread_mutex_lock(&worker->mutex_query);
    int qid = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    t_paquete* paquete = crear_paquete(OP_STORAGE_DELETE);

    agregar_a_paquete(paquete, &qid, sizeof(int)); 
    agregar_a_paquete(paquete, (void*)file, strlen(file) + 1);
    agregar_a_paquete(paquete, (void*)tag, strlen(tag) + 1);
    enviar_paquete(paquete, worker->fd_storage);
    destruir_paquete(paquete);

    int res = -1;
    if (recv(worker->fd_storage, &res, sizeof(int), MSG_WAITALL) <= 0)
        return -1;

    return res;
}

void* worker_storage_read_block(t_worker* worker, const char* file, const char* tag, uint32_t block_index){
    t_paquete* paquete = crear_paquete(OP_STORAGE_READ);

    pthread_mutex_lock(&worker->mutex_query);
    int qid = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    agregar_a_paquete(paquete, &qid, sizeof(int));
    agregar_a_paquete(paquete, (void*)file, strlen(file)+1);
    agregar_a_paquete(paquete, (void*)tag,  strlen(tag)+1);
    agregar_a_paquete(paquete, &block_index, sizeof(uint32_t));

    enviar_paquete(paquete, worker->fd_storage);
    destruir_paquete(paquete);

    t_paquete* resp = recibir_paquete(worker->fd_storage);
    if (!resp || resp->codigo_operacion != OP_STORAGE_READ) {
        if(resp) destruir_paquete(resp);
        return NULL;
    }

    void* contenido = malloc(worker->block_size);
    memcpy(contenido, resp->buffer->stream, worker->block_size);

    destruir_paquete(resp);
    return contenido;
}

int worker_storage_write_block(t_worker* worker, const char* file, const char* tag,
                               uint32_t block_index, void* contenido)
{
    pthread_mutex_lock(&worker->mutex_query);
    int qid = worker->id_query_actual;
    pthread_mutex_unlock(&worker->mutex_query);

    t_paquete* p = crear_paquete(OP_STORAGE_WRITE);

    agregar_a_paquete(p, &qid, sizeof(int));
    agregar_a_paquete(p, (void*)file, strlen(file)+1);
    agregar_a_paquete(p, (void*)tag,  strlen(tag)+1);
    agregar_a_paquete(p, &block_index, sizeof(uint32_t));
    agregar_a_paquete(p, contenido, worker->block_size);

    enviar_paquete(p, worker->fd_storage);
    destruir_paquete(p);

    int result = -1;
    recv(worker->fd_storage, &result, sizeof(int), MSG_WAITALL);
    return result;
}