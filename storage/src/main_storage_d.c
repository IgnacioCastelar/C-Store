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
    log_debug(logger, "Cerrando Storage...");
    if (bitmap_global) bitmap_destruir(bitmap_global);
    if (blocks_hash_index) dictionary_destroy_and_destroy_elements(blocks_hash_index, free);
    pthread_mutex_destroy(&mutex_bitmap);
    pthread_mutex_destroy(&mutex_blocks_hash);
    if (punto_montaje_global) free(punto_montaje_global);
    if (config) config_destroy(config);
    if (logger) log_destroy(logger);
}

void sighandler(int x) {
    if (x == SIGINT) {
        limpiar_recursos();
        exit(EXIT_SUCCESS);
    }
}

int main(int argc, char *argv[])
{   
    // 0. Inicialización preventiva para evitar basura en punteros
    
    blocks_hash_index = NULL;
    punto_montaje_global = NULL;
    bitmap_global = NULL;
    // Inicializar Mutex antes de cualquier operación
    pthread_mutex_init(&mutex_bitmap, NULL);
    pthread_mutex_init(&mutex_blocks_hash, NULL);

    // 1. Validación de Argumentos
    if (argc < 2) {
        fprintf(stderr, "Uso: %s <archivo_config>\n", argv[0]);
        return EXIT_FAILURE;
    }

    // 2. Iniciar Logger
    logger = log_create("storage.log", "Storage", 1, LOG_LEVEL_INFO);
    log_info(logger, "---------------------------------------------");
    log_info(logger, "       INICIANDO MÓDULO STORAGE");
    log_info(logger, "---------------------------------------------");
    // REGISTRAR SEÑAL PARA CIERRE SEGURO
    signal(SIGINT, sighandler);

    // FIX T-004: Ruta Config Consistente
    char *nombre_config = argv[1];
    char *ruta_config;
    
    if (strstr(nombre_config, "/") != NULL) {
         ruta_config = strdup(nombre_config);
    } else {
         ruta_config = string_from_format("./config/%s", nombre_config);
    }
    
    config = config_create(ruta_config);
    
    if (config == NULL) {
        log_error(logger, "¡ERROR FATAL! No se pudo abrir config: %s", ruta_config);
        free(ruta_config);
        limpiar_recursos();
        return EXIT_FAILURE;
    }
    log_info(logger, "Config cargada: %s", ruta_config);
    free(ruta_config);

    consulta_fresh(); // Inicializar FS

    // 5. Iniciar Servidor
    int puerto = config_get_int_value(config, "PUERTO_ESCUCHA");
    
    // --- FIX REFACTOR UTILS: Pasamos logger explícitamente ---
    int server_fd = iniciar_servidor(puerto, logger);
    // ---------------------------------------------------------
    
    if (server_fd == -1) {
        log_error(logger, "Error al iniciar el servidor en puerto %d", puerto);
        limpiar_recursos();
        return EXIT_FAILURE;
    }

    log_info(logger, "Storage escuchando en puerto %d", puerto);
    // 6. Bucle de Atención a Workers (Bloqueante)
    iniciar_conexiones_worker(server_fd);

    // 7. Finalización
    limpiar_recursos();
    return EXIT_SUCCESS;
}