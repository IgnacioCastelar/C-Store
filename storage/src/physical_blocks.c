#include "physical_blocks.h"
#include <commons/log.h>
#include <commons/string.h>
#include <errno.h>
#include <sys/stat.h> 
extern t_log* logger;


int physical_blocks_crear_todos(const char* punto_montaje, int fs_size, int block_size) {
    // Validacion de parametros
    if (block_size <= 0 || fs_size <= 0 || fs_size % block_size != 0) {
        log_error(logger, "Parametros invalidos para crear bloques físicos");
        return -1;
    }

    // calculo de bloques a crear en la ruta
    int total_blocks = fs_size / block_size;
    char* ruta_physical = string_from_format("%s/physical_blocks", punto_montaje);;
    //snprintf(ruta_physical, sizeof(ruta_physical), "%s/physical_blocks", punto_montaje);

    // Crear directorio para bloques fisicos
    if (mkdir(ruta_physical, 0755) == -1 && errno != EEXIST) {
        log_error(logger, "No se pudo crear physical_blocks");
        return -1;
    }

    for (int i = 0; i < total_blocks; i++) {
        char* nombre_bloque = string_from_format("block%04d.dat", i);
        char* ruta_bloque = string_from_format("%s/%s", ruta_physical, nombre_bloque);
    
        FILE* f = fopen(ruta_bloque, "wb");
        if (!f) {
            log_error(logger, "No se pudo crear %s", nombre_bloque);
            free(nombre_bloque);
            free(ruta_bloque);
            free(ruta_physical);
            return -1;
        }

        if (i == 0) {
            for (int j = 0; j < block_size; j++) {
                fputc('0', f);
            }
        } else {
            char cero = '\0';
            for (int j = 0; j < block_size; j++) {
                fwrite(&cero, 1, 1, f);
            }
        }

        
        fclose(f);
        free(nombre_bloque);
        free(ruta_bloque);
    }

    log_info(logger, "Creados %d bloques físicos en %s", total_blocks, ruta_physical);
    free(ruta_physical);
    
    return 0;
}   