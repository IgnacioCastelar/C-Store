// blocks_hash_index.c
#include "blocks_hash_index.h"

extern t_log* logger;

t_dictionary* cargar_blocks_hash_index(const char* punto_montaje) {
    char* ruta = string_from_format("%s/blocks_hash_index.config", punto_montaje);
    FILE* f = fopen(ruta, "r");
    t_dictionary* index = dictionary_create();

    if (f == NULL) {
        log_warning(logger,"No se encontró blocks_hash_index.config. Creando nuevo índice vacío.");
        // Crear archivo vacío
        FILE* f_new = fopen(ruta, "w");
        if (f_new) fclose(f_new);
        free(ruta);
        return index;
    }

    // Verificar si el archivo está vacío
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    if (file_size == 0) {
        log_info(logger, "blocks_hash_index.config está vacío");
        fclose(f);
        free(ruta);
        return index;
    }

    char* linea = NULL;
    size_t len = 0;
    while (getline(&linea, &len, f) != -1) {
        char* copia_linea = strdup(linea);
        string_trim(&copia_linea);
        
        if (strlen(copia_linea) == 0) {
            free(copia_linea);
            continue;
        }

        char* igual = strchr(copia_linea, '=');
        if (igual == NULL) {
            log_warning(logger, "Línea inválida en blocks_hash_index: %s", copia_linea);
            free(copia_linea);
            continue;
        }

        *igual = '\0';
        char* hash = copia_linea;
        char* bloque = igual + 1;

        dictionary_put(index, hash, strdup(bloque));

        free(copia_linea); // Liberamos la copia de esta iteración
    }

    free(linea);
    fclose(f);
    free(ruta);
    return index;
}

//guardo el dictionary al archivo (sobrescribe)
void guardar_blocks_hash_index(const char* punto_montaje, t_dictionary* index) {
    char* ruta = string_from_format("%s/blocks_hash_index.config", punto_montaje);
    FILE* f = fopen(ruta, "w");
    if (!f) {
        log_error(logger, "No se pudo abrir %s para escritura", ruta);
        free(ruta);
        return;
    }

    void guardar_linea(char* hash, void* bloque_ptr) {
        fprintf(f, "%s=%s\n", hash, (char*)bloque_ptr);
    }
    dictionary_iterator(index, (void*)guardar_linea);

    fclose(f);
    free(ruta);
}

//busco bloque físico por hash
char* bloque_fisico_por_hash(t_dictionary* index, const char* hash) {
    return dictionary_get(index, (char*)hash);
}

/*
void agregar_bloque_hash(t_dictionary* index, const char* hash, const char* bloque_fisico) {
    dictionary_put(index, strdup(hash), strdup(bloque_fisico));
}
*/
void agregar_bloque_hash(t_dictionary* index, const char* hash, const char* bloque_fisico) {
    // CORRECCIÓN: No hacer strdup al hash (key), la commons lo hace internamente.
    // Sí hacer strdup al bloque_fisico (value), para que persista y luego lo libere el destroy_elements.
    dictionary_put(index, (char*)hash, strdup(bloque_fisico));
}

//calculo el hash MD5 de un bloque
char* hash_de_bloque(void* contenido, size_t tamanio) {
    return crypto_md5(contenido, tamanio);
}