#include "main_storage_d.h"
#include "storage_conexiones.h"
#include "operaciones.h"
#include <signal.h>

// DEFINICIÓN DE VARIABLES GLOBALES
// (Aca se reservan en memoria, el resto de módulos las usan con 'extern')

t_dictionary *blocks_hash_index = NULL;
char *punto_montaje_global = NULL;
t_bitmap* bitmap_global = NULL;
int block_size_global = 0;

// DEFINICIÓN DE MUTEX (Sincronización)
pthread_mutex_t mutex_bitmap;
pthread_mutex_t mutex_blocks_hash;

t_config* config = NULL;
t_log *logger = NULL;
t_config *config_superblock = NULL; // Usado temporalmente o globalmente según necesidad

// =================================================================================
// FUNCIÓN DE LIMPIEZA
// =================================================================================

void limpiar_recursos() {
    log_debug(logger, "Cerrando Storage y liberando recursos...");
    
    if (bitmap_global) {
        bitmap_destruir(bitmap_global);
        bitmap_global = NULL;
    }
    
    if (blocks_hash_index) {
        dictionary_destroy_and_destroy_elements(blocks_hash_index, free);
        blocks_hash_index = NULL;
    }
    
    // Destruir Mutex
    pthread_mutex_destroy(&mutex_bitmap);
    pthread_mutex_destroy(&mutex_blocks_hash);
    
    if (punto_montaje_global) {
        free(punto_montaje_global);
        punto_montaje_global = NULL;
    }
    
    if (config) {
        config_destroy(config);
        config = NULL;
    }
    
    if (logger) {
        log_destroy(logger);
        logger = NULL;
    }
}

// MANEJADOR DE SEÑALES (CTRL+C, exit, interrupcion de ejecucion)
void sighandler(int x) {
    switch (x) {
        case SIGINT:
            log_info(logger, "Recibida señal de finalización (SIGINT). Cerrando...");
            limpiar_recursos();
            exit(EXIT_SUCCESS);
            break;
    }
}

// MAIN - Puntos de Entrada

int main(int argc, char *argv[])
{   
    // 0. Inicialización preventiva para evitar basura en punteros
    blocks_hash_index = NULL;
    punto_montaje_global = NULL;
    bitmap_global = NULL;
    block_size_global = 0;
    config = NULL;
    logger = NULL;

    // Inicializar Mutex antes de cualquier operación
    pthread_mutex_init(&mutex_bitmap, NULL);
    pthread_mutex_init(&mutex_blocks_hash, NULL);

    // 1. Validación de Argumentos
    if (argc < 2) {
        fprintf(stderr, "ERROR: Faltan argumentos.\nUso: %s <nombre_config>\n", argv[0]);
        return EXIT_FAILURE;
    }

    // 2. Iniciar Logger
    logger = log_create("logger_storage.log", "Storage", 1, LOG_LEVEL_INFO);
    log_info(logger, "---------------------------------------------");
    log_info(logger, "       INICIANDO MÓDULO STORAGE");
    log_info(logger, "---------------------------------------------");

    // REGISTRAR SEÑAL PARA CIERRE SEGURO
    signal(SIGINT, sighandler);

    // 3. Cargar Configuración (Ruta Absoluta Correcta)
    char *nombre_config = argv[1];
    char *ruta_config = string_from_format("%s.config", nombre_config);
    
    config = config_create(ruta_config);
    
    // Fallback: Si no carga, intentamos ruta local relativa (útil para pruebas rápidas)
    if (config == NULL) {
        log_warning(logger, "No se encontró config en ruta absoluta. Probando local ./config/...");
        free(ruta_config);
        ruta_config = string_from_format("./config/%s.config", nombre_config);
        config = config_create(ruta_config);
    }

    if (config == NULL) {
        log_error(logger, "¡ERROR FATAL! No se pudo abrir el archivo de configuración: %s", ruta_config);
        free(ruta_config);
        limpiar_recursos();
        return EXIT_FAILURE;
    }
    
    log_info(logger, "Configuración cargada desde: %s", ruta_config);
    free(ruta_config);

    // 4. Inicializar FileSystem (Fresh Start o Restore)
    // Esta función (en operaciones.c) se encarga de crear directorios, cargar bitmap y superbloque.
    consulta_fresh();

    // 5. Iniciar Servidor
    int puerto = config_get_int_value(config, "PUERTO_ESCUCHA");
    int server_fd = iniciar_servidor(puerto);
    
    if (server_fd == -1) {
        log_error(logger, "Error al iniciar el servidor en puerto %d", puerto);
        limpiar_recursos();
        return EXIT_FAILURE;
    }

    log_info(logger, "Servidor Storage LISTO en puerto %d", puerto);

    // 6. Bucle de Atención a Workers (Bloqueante)
    iniciar_conexiones_worker(server_fd);

    // 7. Finalización
    limpiar_recursos();
    return EXIT_SUCCESS;
}