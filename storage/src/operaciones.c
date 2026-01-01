#include "operaciones.h"
#include "bitmap.h"
#include <libgen.h>
#include "blocks_hash_index.h"
// Variables globales externas
extern t_log *logger;
extern char *punto_montaje_global;
extern t_bitmap *bitmap_global;
extern int block_size_global;
extern t_config *config; // Necesario para consulta_fresh

// EXTERNS DE MUTEX (Definidos en main_storage_d.c)
extern pthread_mutex_t mutex_bitmap;
extern pthread_mutex_t mutex_blocks_hash;

// Prototipos locales de auxiliares
static void crear_initial_file(const char *punto_montaje, int block_size);
int borrar_directorio_recursivo(const char* dir_path); // Usado por op_eliminar_tag

// ADMINISTRACION DEL FILESYSTEM

int evaluar_valor(char *valor_string) {
    if (strcmp(valor_string, "TRUE") == 0) return 1;
    if (strcmp(valor_string, "FALSE") == 0) return 2;
    return -1;
}

void borrar_contenido_directorio(char *path) {
    DIR *dir = opendir(path);
    struct dirent *entry;
    char filepath[512];

    if (dir == NULL) {
        log_error(logger, "No se pudo abrir el directorio para borrar contenido: %s", path);
        return;
    }
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        
        if (strcmp(entry->d_name, "superblock.config") == 0)
            continue;

        snprintf(filepath, sizeof(filepath), "%s/%s", path, entry->d_name);

        if (entry->d_type == DT_REG) {
            if(remove(filepath) == 0)
                log_trace(logger, "Eliminado archivo: %s", filepath);
            else
                log_error(logger, "Error eliminando: %s", filepath);
        } else if (entry->d_type == DT_DIR) {
            borrar_contenido_directorio(filepath);
            rmdir(filepath);
            log_trace(logger, "Eliminado directorio: %s", filepath);
        }
    }
    closedir(dir);
}

static void crear_initial_file(const char *punto_montaje, int block_size) {
    char *ruta_file = string_from_format("%s/files/initial_file", punto_montaje);
    char *ruta_tag = string_from_format("%s/files/initial_file/BASE", punto_montaje);
    char *ruta_logical = string_from_format("%s/files/initial_file/BASE/logical_blocks", punto_montaje);
    
    mkdir(ruta_file, 0755);
    mkdir(ruta_tag, 0755);
    mkdir(ruta_logical, 0755);

    // metadata.config
    char *ruta_meta = string_from_format("%s/files/initial_file/BASE/metadata.config", punto_montaje);
    FILE *meta = fopen(ruta_meta, "w");
    if (meta) {
        fprintf(meta, "TAMAÑO=%d\nBLOCKS=[0]\nESTADO=WORK_IN_PROGRESS\n", block_size);
        fclose(meta);
    }
    free(ruta_meta);

    // HARD LINK al bloque 0
    char *ruta_hard = string_from_format("%s/files/initial_file/BASE/logical_blocks/000000.dat", punto_montaje);
    char *ruta_bloque_fisico = string_from_format("%s/physical_blocks/block0000.dat", punto_montaje);

    if (link(ruta_bloque_fisico, ruta_hard) == -1) {
        log_error(logger, "Falló hard link initial_file: %s", strerror(errno));
    }

    free(ruta_file); free(ruta_tag); free(ruta_logical);
    free(ruta_hard); free(ruta_bloque_fisico);
}


// FUNCIÓN AUXILIAR (Copiar antes de inicializar_fs_fresh)
// Esta función garantiza que el Bloque 0 físico exista y esté lleno de '0'

void formatear_bloque_cero(const char* punto_montaje, int block_size) {
    char *ruta_bloque_0 = string_from_format("%s/physical_blocks/block0000.dat", punto_montaje);
    
    // Usamos "w+b" para crearlo o sobrescribirlo desde cero.
    FILE *f = fopen(ruta_bloque_0, "w+b");
    
    if (f) {
        void* buffer = malloc(block_size);
        
        // LAZY TRUNCATE --- Llenamo bloque base con caracter '0'
        // Así, cuando se haga un TRUNCATE y apunte aquí, el archivo parecerá lleno de ceros.
        memset(buffer, '0', block_size); 
        
        fwrite(buffer, block_size, 1, f);
        
        free(buffer);
        fclose(f);
        log_info(logger, "Bloque Físico 0 inicializado con padding '0' correctamente.");
    } else {
        log_error(logger, "Error CRÍTICO creando Bloque Físico 0: %s", strerror(errno));
    }
    free(ruta_bloque_0);
}

void inicializar_fs_fresh(const char *punto_montaje) {
    // 1. Crear directorios base
    char *ruta_files = string_from_format("%s/files", punto_montaje);
    mkdir(ruta_files, 0755);
    free(ruta_files);
    
    char *ruta_pb = string_from_format("%s/physical_blocks", punto_montaje);
    mkdir(ruta_pb, 0755);
    free(ruta_pb);

    // 2. Superblock (Lógica T-014 modificada)
    char *ruta_sb = string_from_format("%s/superblock.config", punto_montaje);
    FILE *f_sb = fopen(ruta_sb, "r");
    
    // Si no existe, lo creamos tomando valores del ENTORNO si existen
    if (!f_sb) {
        f_sb = fopen(ruta_sb, "w");
        if(f_sb) {
            char* env_bs = getenv("BLOCK_SIZE");
            char* env_fs = getenv("FS_SIZE");
            
            // Valores por defecto o del entorno
            int block_size = env_bs ? atoi(env_bs) : 16;
            int fs_size = env_fs ? atoi(env_fs) : 65536;

            fprintf(f_sb, "FS_SIZE=%d\nBLOCK_SIZE=%d\n", fs_size, block_size);
            fclose(f_sb);
            log_info(logger, "Creado superblock.config con BS=%d (Source: %s)", 
                     block_size, env_bs ? "ENV" : "DEFAULT");
        }
    } else fclose(f_sb);

    t_config *superblock = config_create(ruta_sb);
    if(!superblock) {
        log_error(logger, "Error crítico: No se pudo cargar superblock.config");
        free(ruta_sb);
        return;
    }

    int fs_size = config_get_int_value(superblock, "FS_SIZE");
    int block_size = config_get_int_value(superblock, "BLOCK_SIZE");
    int total_blocks = fs_size / block_size;
    config_destroy(superblock);
    free(ruta_sb);

    // 3. Hash Index vacío
    char *ruta_hash = string_from_format("%s/blocks_hash_index.config", punto_montaje);
    FILE *f_hash = fopen(ruta_hash, "w"); // "w" trunca el archivo a 0 bytes
    if (f_hash) fclose(f_hash);
    free(ruta_hash);
    
    // Reiniciamos el diccionario en memoria
    pthread_mutex_lock(&mutex_blocks_hash);
    if(blocks_hash_index) dictionary_destroy_and_destroy_elements(blocks_hash_index, free);
    blocks_hash_index = dictionary_create();
    pthread_mutex_unlock(&mutex_blocks_hash);

    // 4. BLOQUES FÍSICOS
    // Crear todos los archivos .dat (probablemente vacíos o nulos)
    if (physical_blocks_crear_todos(punto_montaje, fs_size, block_size) != 0) {
        log_error(logger, "Error creando bloques físicos");
        return;
    }
    
    // FIX] Formatar explícitamente el Bloque 0 con caracteres '0'
    formatear_bloque_cero(punto_montaje, block_size);

    // 5. BITMAP
    pthread_mutex_lock(&mutex_bitmap);
    t_bitmap *bm = bitmap_crear(punto_montaje, total_blocks);
    
    if(bm) {
        // NOTA: Como modifico 'bitmap_crear' para que haga: bitarray_set_bit(bm->bitarray, 0);
        // YA NO ES NECESARIO llamar a bitmap_ocupar(bm, 0). El bloque 0 ya nace ocupado desde adentro.
        
        // Actualizamos las globales
        bitmap_global = bm;   
        block_size_global = block_size;
    } else {
        log_error(logger, "Falló la creación del Bitmap en Fresh Start");
        pthread_mutex_unlock(&mutex_bitmap);
        return;
    }
    pthread_mutex_unlock(&mutex_bitmap);

    if (physical_blocks_crear_todos(punto_montaje, fs_size, block_size) != 0) {
        log_error(logger, "Error creando bloques físicos");
        return;
    }
    
    // 5. Initial File
    crear_initial_file(punto_montaje, block_size);
    log_info(logger, "FileSystem inicializado correctamente (FRESH START).");
}

void consulta_fresh() {
    char *punto_montaje = config_get_string_value(config, "PUNTO_MONTAJE");
    // Actualizamos la global si no estaba seteada
    if(punto_montaje_global) free(punto_montaje_global);
    punto_montaje_global = strdup(config_get_string_value(config, "PUNTO_MONTAJE"));

    // T-014: Prioridad Variable de Entorno
    char *fresh_str = getenv("FRESH_START");
    char *origen = "ENV";

    if (fresh_str == NULL) {
        fresh_str = config_get_string_value(config, "FRESH_START");
        origen = "CONFIG";
    }

    int fresh = evaluar_valor(fresh_str);

    if (fresh == 1) { // TRUE
        log_info(logger, "---------------------------------------------");
        log_info(logger, "FRESH_START = TRUE (Source: %s). Inicializando FS limpio...", origen);
        log_info(logger, "---------------------------------------------");
        borrar_contenido_directorio(punto_montaje_global);
        inicializar_fs_fresh(punto_montaje_global);
    }
    else if (fresh == 2) { // FALSE
        log_info(logger, "---------------------------------------------");
        log_info(logger, "FRESH_START = FALSE. Levantando FS existente...");
        log_info(logger, "---------------------------------------------");
        
        // Cargar índice desde archivo
        pthread_mutex_lock(&mutex_blocks_hash);
        if(blocks_hash_index) dictionary_destroy_and_destroy_elements(blocks_hash_index, free);
        blocks_hash_index = cargar_blocks_hash_index(punto_montaje);
        if(!blocks_hash_index) {
            log_warning(logger, "No se pudo cargar índice hash, creando uno vacío.");
            blocks_hash_index = dictionary_create();
        }
        pthread_mutex_unlock(&mutex_blocks_hash);

        // Cargar config para saber tamaño de bloque y crear bitmap
        char *ruta_sb = string_from_format("%s/superblock.config", punto_montaje);
        t_config *sb = config_create(ruta_sb);
        if(!sb) {
             log_error(logger, "¡ERROR FATAL! No existe superblock.config y FRESH_START=FALSE.");
             log_error(logger, "El sistema no puede iniciar sin un FS previo. Cambie a FRESH_START=TRUE.");
             free(ruta_sb);
             // Opcional: exit(EXIT_FAILURE); para detener el Storage aquí mismo
             return;
        }

        int fs_size = config_get_int_value(sb, "FS_SIZE");
        int block_size = config_get_int_value(sb, "BLOCK_SIZE");
        int total_blocks = fs_size / block_size;
        
        block_size_global = block_size;
        config_destroy(sb);
        free(ruta_sb);

        // Cargar bitmap existente
        pthread_mutex_lock(&mutex_bitmap);
        bitmap_global = bitmap_crear(punto_montaje, total_blocks);
        pthread_mutex_unlock(&mutex_bitmap);
    } else {
        log_error(logger, "Valor de FRESH_START inválido en config. Use TRUE o FALSE.");    
    }
}

// OPERACIONES DEL WORKER

int op_crear_file_tag(const char *file, const char *tag, int query_id)
{
    if (!file || !tag || query_id < 0)
    {
        log_error(logger, "##%d - CREATE: Parámetros inválidos", query_id);
        return -1;
    }

    char *ruta_file = string_from_format("%s/files/%s", punto_montaje_global, file);
    char *ruta_tag = string_from_format("%s/%s", ruta_file, tag);

    if (access(ruta_tag, F_OK) == 0) {
        log_error(logger, "##%d - CREATE: El File:Tag %s:%s ya existe", query_id, file, tag);
        
        // Limpieza de memoria y retorno de ERROR
        free(ruta_file);
        free(ruta_tag);
        return -1; // <--- Esto hará que el Worker reciba error y corte la ejecución
    }

    char *ruta_logical = string_from_format("%s/logical_blocks", ruta_tag);
    char *ruta_meta = string_from_format("%s/metadata.config", ruta_tag);

    if (mkdir(ruta_file, 0755) == -1 && errno != EEXIST)
    {
        log_error(logger, "##%d - CREATE: No se pudo crear el directorio File", query_id);
        free(ruta_file); free(ruta_tag); free(ruta_logical); free(ruta_meta);
        return -1;
    }

    if (mkdir(ruta_tag, 0755) == -1 && errno != EEXIST)
    {
        log_error(logger, "##%d - CREATE: No se pudo crear el directorio Tag", query_id);
        free(ruta_file); free(ruta_tag); free(ruta_logical); free(ruta_meta);
        return -1;
    }

    if (mkdir(ruta_logical, 0755) == -1 && errno != EEXIST)
    {
        log_error(logger, "##%d - CREATE: No se pudo crear el directorio logical_blocks", query_id);
        free(ruta_file); free(ruta_tag); free(ruta_logical); free(ruta_meta);
        return -1;
    }

    FILE *meta = fopen(ruta_meta, "w");
    if (!meta)
    {
        log_error(logger, "##%d - CREATE: No se pudo crear el archivo metadata", query_id);
        free(ruta_file); free(ruta_tag); free(ruta_logical); free(ruta_meta);
        return -1;
    }

    fprintf(meta, "TAMAÑO=0\nBLOCKS=[]\nESTADO=WORK_IN_PROGRESS\n");
    fclose(meta);

    log_info(logger, "##%d File Creado %s:%s", query_id, file, tag);

    free(ruta_file);
    free(ruta_tag);
    free(ruta_logical);
    free(ruta_meta);
    return 0;
}

// OPTIMIZACIÓN ST_NLINK: Cambio de firma para usar ID de bloque físico
// NOTA: La firma anterior usaba nombres de archivo y tag, pero para st_nlink
// solo necesitamos saber a qué bloque físico apuntar.
// Mantenemos la estructura pero cambiamos la lógica interna.
bool bloque_fisico_tiene_otras_refs(const char *bloque_fisico_nombre, const char *file_actual, const char *tag_actual)
{
    // Construimos la ruta al bloque físico en physical_blocks
    // bloque_fisico_nombre ya viene como "blockXXXX.dat"
    char *ruta_bloque_fisico = string_from_format("%s/physical_blocks/%s", punto_montaje_global, bloque_fisico_nombre);
    
    struct stat st;
    bool tiene_otras = false;

    if (stat(ruta_bloque_fisico, &st) == 0) {
        // Un bloque físico tiene 1 referencia (su entrada en physical_blocks)
        // + N referencias (hardlinks desde logical_blocks).
        // Si st_nlink > 2, significa que hay al menos 2 logical_blocks apuntándolo
        // (el actual + otro), por lo tanto TIENE otras referencias.
        if (st.st_nlink > 2) {
            tiene_otras = true;
        }
    } else {
        log_error(logger, "Error stat en bloque físico %s: %s", bloque_fisico_nombre, strerror(errno));
    }

    free(ruta_bloque_fisico);
    return tiene_otras;
}

int op_truncar_file_tag(const char *file, const char *tag, uint32_t nuevo_tam, int query_id)
{
    log_info(logger, "##%d - Iniciando TRUNCATE para %s:%s a tamaño %u", query_id, file, tag, nuevo_tam);

    if (!file || !tag || query_id < 0)
    {
        log_error(logger, "##%d - TRUNCATE: Parámetros inválidos", query_id);
        return -1;
    }

    char *ruta_tag_dir = string_from_format("%s/files/%s/%s", punto_montaje_global, file, tag);
    struct stat st;
    if (stat(ruta_tag_dir, &st) != 0) 
    {
        log_error(logger, "##%d - TRUNCATE: File:Tag %s:%s no existe", query_id, file, tag);
        free(ruta_tag_dir);
        return -1;
    }
    free(ruta_tag_dir);

    char *ruta_meta = string_from_format("%s/files/%s/%s/metadata.config", punto_montaje_global, file, tag);
    t_config *metadata = config_create(ruta_meta);
    if (!metadata)
    {
        log_error(logger, "##%d - TRUNCATE: No se pudo leer metadata.config de %s:%s", query_id, file, tag);
        free(ruta_meta);
        return -1;
    }

    char *estado_str = config_get_string_value(metadata, "ESTADO");
    if (estado_str && strcmp(estado_str, "COMMITED") == 0)
    {
        log_error(logger, "##%d - TRUNCATE: File:Tag %s:%s está en estado COMMITED, no se puede truncar", query_id, file, tag);
        config_destroy(metadata);
        free(ruta_meta);
        return -1;
    }

    uint32_t tam_actual = config_get_int_value(metadata, "TAMAÑO");
    char *bloques_str = config_get_string_value(metadata, "BLOCKS");
    
    t_list *bloques_fisicos_actuales = parsear_lista_bloques(bloques_str);

    uint32_t bloques_actuales_necesarios = (tam_actual + block_size_global - 1) / block_size_global;
    if (tam_actual == 0) bloques_actuales_necesarios = 0; // Corrección para tamaño 0
    
    uint32_t bloques_nuevos_necesarios = (nuevo_tam + block_size_global - 1) / block_size_global;
    if (nuevo_tam == 0) bloques_nuevos_necesarios = 0;

    char *ruta_logical_blocks_dir = string_from_format("%s/files/%s/%s/logical_blocks", punto_montaje_global, file, tag);
    int resultado = 0;

    if (bloques_nuevos_necesarios > bloques_actuales_necesarios)
    {
        // Buffer de ceros para limpieza (Zero-Fill)
        //void* buffer_ceros = calloc(1, block_size_global);
        
        char *ruta_bloque_fisico_cero = string_from_format("%s/physical_blocks/block0000.dat", punto_montaje_global);

        for (int i = bloques_actuales_necesarios; i < bloques_nuevos_necesarios; i++) {
            
            // Nombre del nuevo bloque lógico
            char *nombre_bloque_logico = string_from_format("%06d.dat", i);
            char *ruta_bloque_logico = string_from_format("%s/%s", ruta_logical_blocks_dir, nombre_bloque_logico);

            // [CORRECCIÓN] Crear Hard Link al Bloque 0
            if (link(ruta_bloque_fisico_cero, ruta_bloque_logico) == -1) {
                log_error(logger, "##%d - TRUNCATE: error creando hard link al bloque 0 para lógico %d - %s", 
                         query_id, i, strerror(errno));
                resultado = -1;
                free(nombre_bloque_logico);
                free(ruta_bloque_logico);
                break; // Salir del for ante error
            } else {
                // Hard Link Agregado (Log obligatorio)
                log_info(logger, "##%d - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico 0", query_id, file, tag, i);
            }
            
            // Agregamos el ID 0 a la lista en memoria
            list_add(bloques_fisicos_actuales, (void *)(intptr_t)0);
            
            free(nombre_bloque_logico);
            free(ruta_bloque_logico);
        }  
        free(ruta_bloque_fisico_cero);
    }
    else if (bloques_nuevos_necesarios < bloques_actuales_necesarios)
    {
        for (int i = bloques_actuales_necesarios - 1; i >= (int)bloques_nuevos_necesarios; i--)
        {
            if (i < list_size(bloques_fisicos_actuales))
            {
                int bloque_fisico_a_liberar = (int)(intptr_t)list_get(bloques_fisicos_actuales, i);

                char *nombre_bloque_logico = string_from_format("%06d.dat", i);
                char *ruta_bloque_logico = string_from_format("%s/%s", ruta_logical_blocks_dir, nombre_bloque_logico);

                // PRIMERO desvincular (baja el contador st_nlink)
                if (unlink(ruta_bloque_logico) == 0) {
                     //  Hard Link Eliminado
                     log_info(logger, "##%d %s:%s Se eliminó el hard link del bloque lógico %d al bloque físico %d", query_id, file, tag, i, bloque_fisico_a_liberar);
                }
                free(nombre_bloque_logico);
                free(ruta_bloque_logico);

                // SEGUNDO verificar si quedó libre usando la optimización st_nlink
                // En vez de llamar a la funcion auxiliar, usamos stat directo aqui para decidir si liberar
                char *ruta_fisica = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje_global, bloque_fisico_a_liberar);
                struct stat st_check;
                
                if (stat(ruta_fisica, &st_check) == 0) {
                    // Si nlink == 1, significa que solo queda la entrada en physical_blocks -> LIBERAR
                    if (st_check.st_nlink == 1) {
                        pthread_mutex_lock(&mutex_bitmap);
                        bitmap_liberar(bitmap_global, bloque_fisico_a_liberar);
                        pthread_mutex_unlock(&mutex_bitmap);
                        log_info(logger, "##%d - Bloque Físico Liberado - Número de bloque: %d", query_id, bloque_fisico_a_liberar);
                    } else {
                        log_debug(logger, "##%d - Bloque físico %d no se liberó porque tiene otras refs", query_id, bloque_fisico_a_liberar);
                    }
                }
                free(ruta_fisica);

                list_remove(bloques_fisicos_actuales, i);
            }
        }
    }

    if (resultado == 0)
    {
        char *bloques_str_nueva = serializar_lista_bloques(bloques_fisicos_actuales);
        
        char* nuevo_tam_str = string_itoa(nuevo_tam);
        config_set_value(metadata, "TAMAÑO", nuevo_tam_str);
        free(nuevo_tam_str);
        config_set_value(metadata, "BLOCKS", bloques_str_nueva);

        if (config_save(metadata)) {
             //  File Truncado
             log_info(logger, "##%d File Truncado %s:%s Tamaño: %u", query_id, file, tag, nuevo_tam);
        } else {
             resultado = -1;
        }
        free(bloques_str_nueva);
    }

    config_destroy(metadata);
    list_destroy(bloques_fisicos_actuales);
    free(ruta_meta);
    free(ruta_logical_blocks_dir);

    return resultado;
}

int op_eliminar_tag(const char* file, const char* tag, int query_id){
    if(!file || !tag || query_id < 0){
        log_error(logger, "##%d - DELETE: Parámetros inválidos ", query_id);
        return -1; //
    }
    
    char* ruta_tag_dir = string_from_format("%s/files/%s/%s", punto_montaje_global, file, tag);
    struct stat st;
    if (stat(ruta_tag_dir, &st) != 0){
        log_error(logger, "##%d - DELETE: File:Tag %s:%s no existe ", query_id, file, tag);
        free(ruta_tag_dir);
        return -1;
    }

    char* ruta_meta = string_from_format("%s/metadata.config", ruta_tag_dir);
    t_config* metadata = config_create(ruta_meta);
    if(!metadata){
        log_error(logger, "##%d - DELETE: No se pudo leer metadata.config ", query_id);
        free(ruta_meta); //
        free(ruta_tag_dir); //
        return -1; //
    }

    char* bloques_str = config_get_string_value(metadata, "BLOCKS");
    t_list* bloques_fisicos_a_liberar = parsear_lista_bloques(bloques_str);

    // Cerramos metadata ya que vamos a borrar el directorio
    config_destroy(metadata);
    free(ruta_meta);

    // CAMBIO DE LÓGICA: Primero borramos el directorio recursivamente.
    // Esto ejecuta unlink() sobre todos los bloques lógicos, decrementando sus st_nlink.
    int resultado_eliminacion = borrar_directorio_recursivo(ruta_tag_dir);
    if(resultado_eliminacion != 0){
        log_error(logger, "##%d - DELETE: Error al eliminar directorio de %s:%s - %s", query_id, file, tag, strerror(errno));
        free(ruta_tag_dir);
        list_destroy(bloques_fisicos_a_liberar);
        return -1;
    }
    
    free(ruta_tag_dir);

    // LUEGO verificamos qué bloques quedaron libres
    for(int i =0; i < list_size(bloques_fisicos_a_liberar); i++){
        int bloque_fisico = (int)(intptr_t)list_get(bloques_fisicos_a_liberar,i);
        char* nombre_bloque_fisico = string_from_format("block%04d.dat", bloque_fisico);
        
        // Usamos la nueva lógica optimizada
        // IMPORTANTE: Como acabamos de borrar los hardlinks lógicos de este tag,
        // si st_nlink == 1, significa que no hay NADIE MÁS usándolo.
        // La función auxiliar retorna true si st_nlink > 2.
        // Aquí queremos liberar si st_nlink == 1.
        
        char *ruta_fisica = string_from_format("%s/physical_blocks/%s", punto_montaje_global, nombre_bloque_fisico);
        struct stat st_check;
        
        if (stat(ruta_fisica, &st_check) == 0 && st_check.st_nlink == 1) {
            pthread_mutex_lock(&mutex_bitmap);
            bitmap_liberar(bitmap_global, bloque_fisico);
            pthread_mutex_unlock(&mutex_bitmap);
            log_info(logger, "##%d - Bloque Físico Liberado - Número de bloque: %d", query_id, bloque_fisico);
        } else {
            log_debug(logger, "##%d - Bloque Físico %d no se liberó porque tiene otras refs.", query_id, bloque_fisico);
        }
        
        free(ruta_fisica);
        free(nombre_bloque_fisico);
    }

    list_destroy(bloques_fisicos_a_liberar);
    log_info(logger, "##%d- Tag Eliminado %s:%s", query_id, file, tag);

    return 0;
}

int borrar_directorio_recursivo(const char* dir_path){
    DIR* dir = opendir(dir_path);
    if(dir == NULL){
        return -1;
    }

    struct dirent* entry;
    int ret = 0;
    //char* filepath;
    
    while(ret == 0){
        errno =0;
        entry = readdir(dir);
        
        if(entry == NULL){
            if(errno != 0){
                log_error(logger, "Error al leer directorio '%s:%s'", dir_path, strerror(errno));
                ret = -1;
                break;
            }
            break;
        }

        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0){
            continue;
        }

        char* ruta_hijo = string_from_format("%s/%s", dir_path, entry->d_name);
        
        if(entry->d_type == DT_DIR){
            ret = borrar_directorio_recursivo(ruta_hijo);
            if(ret !=0){
                log_error(logger, "Error al borrar subdirectorio '%s", ruta_hijo);
            }
        }
        else if(entry->d_type == DT_REG){
            ret = remove(ruta_hijo);
            if(ret != 0){
                log_error(logger, "Error eliminando archivo '%s'", ruta_hijo);
            }
            // free(ruta_hijo); // Esto estaba comentado o mal puesto en la versión original
        }
        free(ruta_hijo);
    }

    if (ret == 0) {
        closedir(dir);
        ret = rmdir(dir_path);
        if (ret != 0) {
             log_error(logger, "Error al eliminar directorio vacío '%s': %s", dir_path, strerror(errno));
        }
    } else {
        closedir(dir);
    }

    return ret;
}

int op_escribir_bloque(const char* file, const char* tag, uint32_t bloque_logico, void* contenido, int tam_contenido, int query_id) {
    if(!file || !tag || query_id < 0) return -1;

    // 1. Validaciones y Metadata (Standard)
    char* ruta_meta = string_from_format("%s/files/%s/%s/metadata.config", punto_montaje_global, file, tag);
    t_config* metadata = config_create(ruta_meta);
    if(!metadata) { free(ruta_meta); return -1; }
    
    char* estado = config_get_string_value(metadata, "ESTADO");
        if(estado && strcmp(estado, "COMMITED") == 0) {

        log_error(logger,
            "##%d - WRITE: intento de escritura sobre File:Tag %s:%s en estado COMMITED",
            query_id, file, tag);

        config_destroy(metadata);
        free(ruta_meta);
        return -1;
    }

    char* bloques_str = config_get_string_value(metadata, "BLOCKS");
    t_list* bloques = parsear_lista_bloques(bloques_str);
    int bloque_fisico_actual = (int)(intptr_t)list_get(bloques, bloque_logico);

    // ------------------------------------------------------------
    // 1. READ: Traer el bloque viejo a memoria (Preservar fondo)
    // ------------------------------------------------------------
    void* buffer_final = malloc(block_size_global);
    memset(buffer_final, '0', block_size_global); // Fondo default: ceros
    
    char* ruta_bloque_viejo = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje_global, bloque_fisico_actual);
    FILE* f_viejo = fopen(ruta_bloque_viejo, "rb");
    if (f_viejo) {
        fread(buffer_final, block_size_global, 1, f_viejo);
        fclose(f_viejo);
    }
    free(ruta_bloque_viejo);

    // ------------------------------------------------------------
    // 2. CoW: Gestionar cambio de bloque si es necesario
    // ------------------------------------------------------------
    char* nombre_bloque_actual = string_from_format("block%04d.dat", bloque_fisico_actual);
    bool tiene_otras_refs = bloque_fisico_tiene_otras_refs(nombre_bloque_actual, file, tag);
    int bloque_fisico_a_escribir = bloque_fisico_actual;
    
    if(tiene_otras_refs) {
        pthread_mutex_lock(&mutex_bitmap);
        int nuevo_bloque = bitmap_buscar_libre(bitmap_global);
        if(nuevo_bloque != -1) bitmap_ocupar(bitmap_global, nuevo_bloque);

        log_info(logger, "##%d - Bloque Físico Reservado - Número de Bloque: %d", query_id, nuevo_bloque);

        pthread_mutex_unlock(&mutex_bitmap);
        
        if(nuevo_bloque == -1) {
            free(buffer_final); free(nombre_bloque_actual); config_destroy(metadata); free(ruta_meta); list_destroy(bloques);
            return -1;
        }
        
        bloque_fisico_a_escribir = nuevo_bloque;
        list_replace(bloques, bloque_logico, (void*)(intptr_t)nuevo_bloque);
        
        // Actualizar links
        char* ruta_hard_link = string_from_format("%s/files/%s/%s/logical_blocks/%06d.dat", punto_montaje_global, file, tag, bloque_logico);
        char* ruta_nuevo_fisico = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje_global, nuevo_bloque);
        unlink(ruta_hard_link); 
        link(ruta_nuevo_fisico, ruta_hard_link);

        log_info(logger, "##%d - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico %d", 
                     query_id, file, tag, bloque_logico, nuevo_bloque);

        free(ruta_hard_link); free(ruta_nuevo_fisico);

        // Actualizar metadata
        char* nuevos_bloques_str = serializar_lista_bloques(bloques);
        config_set_value(metadata, "BLOCKS", nuevos_bloques_str);
        config_save(metadata);
        free(nuevos_bloques_str);
    }

    // ------------------------------------------------------------
    // 3. MODIFY & WRITE: Pegar datos nuevos y guardar
    // ------------------------------------------------------------
    char* ruta_bloque = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje_global, bloque_fisico_a_escribir);
    FILE* f = fopen(ruta_bloque, "w+b");
    
    if(f) {
        // AQUI ESTÁ LA MAGIA: Solo copiamos tam_contenido (20 bytes)
        // El resto del buffer_final sigue teniendo "Styles_of_Beyond..."
        if(tam_contenido > 0) {
            memcpy(buffer_final, contenido, tam_contenido);
        }
        
        fwrite(buffer_final, block_size_global, 1, f);
        fflush(f);        
        fsync(fileno(f)); 
        fclose(f);
        ///////// log intocable
        log_info(logger, "##%d Bloque Lógico Escrito %s:%s Número de Bloque: %u", query_id, file, tag, bloque_logico);
    } else {
         log_error(logger, "##%d - WRITE: Error abriendo bloque físico %d", query_id, bloque_fisico_a_escribir);
    }

    free(buffer_final);
    int retardo_bloque = config_get_int_value(config, "RETARDO_ACCESO_BLOQUE");
    usleep(retardo_bloque * 1000);
    
    free(ruta_bloque); free(nombre_bloque_actual); config_destroy(metadata); free(ruta_meta); list_destroy(bloques);
    return 0;
}

////
int op_commit_file_tag(const char* file, const char* tag, int query_id) {
    if(!file || !tag || query_id < 0) {
        log_error(logger, "##%d - COMMIT: Parámetros inválidos", query_id);
        return -1;
    }

    // VERIFICACIÓN CRÍTICA CON MUTEX
    pthread_mutex_lock(&mutex_blocks_hash);
    if (blocks_hash_index == NULL) {
        log_error(logger, "##%d - COMMIT: blocks_hash_index es NULL - inicializando emergencia", query_id);
        blocks_hash_index = dictionary_create();
    }
    pthread_mutex_unlock(&mutex_blocks_hash);

    char* ruta_meta = string_from_format("%s/files/%s/%s/metadata.config", punto_montaje_global, file, tag);
    t_config* metadata = config_create(ruta_meta);
    if(!metadata) {
        log_error(logger, "##%d - COMMIT: File:Tag %s:%s no existe", query_id, file, tag);
        free(ruta_meta);
        return -1;
    }

    // 1. Verificar que no esté ya COMMITED
    char* estado = config_get_string_value(metadata, "ESTADO");
    if(estado && strcmp(estado, "COMMITED") == 0) {
        config_destroy(metadata);
        free(ruta_meta);
        return 0;
    }

    // 2. Obtener lista de bloques
    char* bloques_str = config_get_string_value(metadata, "BLOCKS");
    t_list* bloques = parsear_lista_bloques(bloques_str);

    bool hubo_cambios = false;
    int resultado = 0; // 0 = éxito, -1 = error

    // 3. Por cada bloque lógico, verificar deduplicación
    for(int i = 0; i < list_size(bloques); i++) {
        int bloque_fisico_actual = (int)(intptr_t)list_get(bloques, i);
        
        // Leer contenido del bloque físico actual
        char* ruta_bloque_actual = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje_global, bloque_fisico_actual);
        FILE* f = fopen(ruta_bloque_actual, "rb");
        if(!f) {
            log_error(logger, "##%d - COMMIT: No se pudo leer bloque físico %d", query_id, bloque_fisico_actual);
            free(ruta_bloque_actual);
            resultado = -1;
            continue;
        }
        
        void* contenido = malloc(block_size_global);
        if (contenido == NULL) {
            log_error(logger, "##%d - COMMIT: No se pudo allocar memoria para contenido", query_id);
            fclose(f);
            free(ruta_bloque_actual);
            resultado = -1;
            continue;
        }
        
        size_t leidos = fread(contenido, 1, block_size_global, f);
        fclose(f);
        
        if (leidos != block_size_global) {
            log_error(logger, "##%d - COMMIT: Error leyendo bloque físico %d", query_id, bloque_fisico_actual);
            free(contenido);
            free(ruta_bloque_actual);
            resultado = -1;
            continue;
        }
        
        // Calcular hash
        char* hash = hash_de_bloque(contenido, block_size_global);
        if (hash == NULL) {
            log_error(logger, "##%d - COMMIT: Error calculando hash para bloque %d", query_id, bloque_fisico_actual);
            free(contenido);
            free(ruta_bloque_actual);
            resultado = -1;
            continue;
        }

        // Buscar en índice de hash (CON MUTEX)
        pthread_mutex_lock(&mutex_blocks_hash);
        char* bloque_existente = bloque_fisico_por_hash(blocks_hash_index, hash);
        
        if(bloque_existente) {
            // Extraer número del bloque existente
            int bloque_fisico_existente;
            sscanf(bloque_existente, "block%d", &bloque_fisico_existente);
            
            if(bloque_fisico_existente != bloque_fisico_actual) {
                // DEDUPLICACIÓN: Usar el bloque existente
                char* ruta_logical_actual = string_from_format("%s/files/%s/%s/logical_blocks/%06d.dat", 
                    punto_montaje_global, file, tag, i);
                
                char* ruta_fisica_existente = string_from_format("%s/physical_blocks/block%04d.dat", 
                    punto_montaje_global, bloque_fisico_existente);
                
                // Reemplazar hard link
                unlink(ruta_logical_actual); // Borramos el link al bloque viejo
                
                if (link(ruta_fisica_existente, ruta_logical_actual) == -1) {
                    log_error(logger, "##%d - COMMIT: Error creando hard link: %s", query_id, strerror(errno));
                    resultado = -1;
                } else {
                    // Actualizar lista de bloques
                    list_replace(bloques, i, (void*)(intptr_t)bloque_fisico_existente);
                    hubo_cambios = true;
                    
                    // --- CORRECCIÓN CRÍTICA: LIBERACIÓN SEGURA POR NLINK ---
                    // Verificamos el bloque viejo FÍSICO.
                    struct stat st;
                    char* ruta_bloque_fisico_viejo = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje_global, bloque_fisico_actual);
                    
                    if (stat(ruta_bloque_fisico_viejo, &st) == 0) {
                        // st_nlink == 1 significa que solo queda la entrada en la carpeta physical_blocks.
                        // O sea, ningún archivo lógico apunta a él. SE PUEDE BORRAR.
                        if (st.st_nlink <= 1) {
                             // PROTECCIÓN BLOQUE 0 (Nunca liberar el 0)
                             if (bloque_fisico_actual != 0) {
                                pthread_mutex_lock(&mutex_bitmap);
                                bitmap_liberar(bitmap_global, bloque_fisico_actual);
                                pthread_mutex_unlock(&mutex_bitmap);
                                log_info(logger, "##%d - Bloque Físico Liberado - Número de Bloque: %d", query_id, bloque_fisico_actual);
                             }
                        }
                    }
                    free(ruta_bloque_fisico_viejo);
                    
                    //  Deduplicación
                    log_info(logger, "##%d %s:%s Bloque Lógico %d se reasigna de %d a %d", query_id, file, tag, i, bloque_fisico_actual, bloque_fisico_existente);
                }
                
                free(ruta_logical_actual);
                free(ruta_fisica_existente);
            }
        } else {
            // Agregar nuevo hash al índice
            char* nombre_bloque = string_from_format("block%04d", bloque_fisico_actual);
            agregar_bloque_hash(blocks_hash_index, hash, nombre_bloque);
            free(nombre_bloque);
        }
        pthread_mutex_unlock(&mutex_blocks_hash);
        
        free(contenido);
        free(hash);
        free(ruta_bloque_actual);
        
        // Retardo por acceso a bloque
        int retardo_bloque = config_get_int_value(config, "RETARDO_ACCESO_BLOQUE");
        usleep(retardo_bloque * 1000);
    }

    // 4. Actualizar metadata solo si no hubo errores
    if(resultado == 0) {
        if(hubo_cambios) {
            char* nuevos_bloques_str = serializar_lista_bloques(bloques);
            config_set_value(metadata, "BLOCKS", nuevos_bloques_str);
            free(nuevos_bloques_str);
        }
        
        config_set_value(metadata, "ESTADO", "COMMITED");
        if (!config_save(metadata)) {
            log_error(logger, "##%d - COMMIT: Error guardando metadata", query_id);
            resultado = -1;
        }
        
        // Guardar índice de hash actualizado
        pthread_mutex_lock(&mutex_blocks_hash);
        guardar_blocks_hash_index(punto_montaje_global, blocks_hash_index);
        pthread_mutex_unlock(&mutex_blocks_hash);
        
        if (resultado == 0) {
            log_info(logger, "##%d - Commit de File:Tag %s:%s", query_id, file, tag);
        }
    }
    
    config_destroy(metadata);
    free(ruta_meta);
    list_destroy(bloques);
    return resultado;
}

// Función para parsear la lista de bloques del metadata
t_list* parsear_lista_bloques(const char* bloques_str) {
    t_list* bloques = list_create();
    
    if(bloques_str && strlen(bloques_str) > 2) {
        char* copia = strdup(bloques_str + 1); // Saltar '['
        copia[strlen(copia) - 1] = '\0'; // Remover ']'
        
        char** bloques_array = string_split(copia, ",");
        for(int i = 0; bloques_array[i] != NULL; i++) {
            int num_bloque = atoi(bloques_array[i]);
            list_add(bloques, (void*)(intptr_t)num_bloque);
        }
        
        for(int j = 0; bloques_array[j] != NULL; j++) {
            free(bloques_array[j]);
        }
        free(bloques_array);
        free(copia);
    }
    
    return bloques;
}

// Función para serializar lista de bloques a string
char* serializar_lista_bloques(t_list* bloques) {
    char* resultado = string_new();
    string_append(&resultado, "[");
    
    for(int i = 0; i < list_size(bloques); i++) {
        if(i > 0) string_append(&resultado, ",");
        char* num_str = string_itoa((int)(intptr_t)list_get(bloques, i));
        string_append(&resultado, num_str);
        free(num_str);
    }
    
    string_append(&resultado, "]");
    return resultado;
}


void* op_leer_bloque(const char* file, const char* tag, uint32_t bloque_logico, int query_id) {
    if(!file || !tag || query_id < 0) {
        log_error(logger, "##%d - READ: Parámetros inválidos", query_id);
        return NULL;
    }

    char* ruta_meta = string_from_format("%s/files/%s/%s/metadata.config", punto_montaje_global, file, tag);
    t_config* metadata = config_create(ruta_meta);
    if(!metadata) {
        log_error(logger, "##%d - READ: File:Tag %s:%s no existe", query_id, file, tag);
        free(ruta_meta);
        return NULL;
    }

    // Obtener lista de bloques
    char* bloques_str = config_get_string_value(metadata, "BLOCKS");
    t_list* bloques = parsear_lista_bloques(bloques_str);
    
    if(bloque_logico >= list_size(bloques)) {
        log_error(logger, "##%d - READ: Bloque lógico %u fuera de rango", query_id, bloque_logico);
        config_destroy(metadata);
        free(ruta_meta);
        list_destroy(bloques);
        return NULL;
    }

    int bloque_fisico = (int)(intptr_t)list_get(bloques, bloque_logico);
    
    // Leer contenido del bloque físico
    char* ruta_bloque = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje_global, bloque_fisico);
    FILE* f = fopen(ruta_bloque, "rb");
    if(!f) {
        log_error(logger, "##%d - READ: No se pudo leer bloque físico %d", query_id, bloque_fisico);
        free(ruta_bloque);
        config_destroy(metadata);
        free(ruta_meta);
        list_destroy(bloques);
        return NULL;
    }
    
    void* contenido = malloc(block_size_global);
    fread(contenido, 1, block_size_global, f);
    fclose(f);
    
    // Aplicar retardo por acceso a bloque
    int retardo_bloque = config_get_int_value(config, "RETARDO_ACCESO_BLOQUE");
    usleep(retardo_bloque * 1000);
    //
    log_info(logger, "##%d - Bloque Lógico Leído %s:%s - Número de Bloque: %u", query_id, file, tag, bloque_logico);
    //
    free(ruta_bloque);
    config_destroy(metadata);
    free(ruta_meta);
    list_destroy(bloques);
    
    return contenido;
}

// OPERACIÓN TAG - Crear nuevo Tag copiando desde origen
int op_crear_tag(const char* file_origen, const char* tag_origen, 
                 const char* file_destino, const char* tag_destino, int query_id) {
    
    t_config* metadata_origen = NULL;
    char* ruta_meta_origen = NULL;
    char* ruta_tag_destino = NULL;
    char* ruta_file_destino = NULL;
    char* ruta_logical_destino = NULL;
    char* ruta_meta_destino = NULL;
    t_list* bloques = NULL;

    void error_cleanup(){
        // Limpieza en caso de error
        if (metadata_origen) config_destroy(metadata_origen);
        if (metadata_origen) free(ruta_meta_origen);
        if (metadata_origen) free(ruta_tag_destino);
        if (metadata_origen) free(ruta_file_destino);
        if (metadata_origen) free(ruta_logical_destino);
        if (metadata_origen) free(ruta_meta_destino);
    }

    if(!file_origen || !tag_origen || !file_destino || !tag_destino || query_id < 0) {
        log_error(logger, "##%d - TAG: Parámetros inválidos", query_id);
        return -1;
    }

    // Verificar que origen existe
    ruta_meta_origen = string_from_format("%s/files/%s/%s/metadata.config", 
                                               punto_montaje_global, file_origen, tag_origen);
    metadata_origen = config_create(ruta_meta_origen);
    if(!metadata_origen) {
        log_error(logger, "##%d - TAG: File:Tag origen %s:%s no existe", query_id, file_origen, tag_origen);
        free(ruta_meta_origen);
        return -1;
    }

    // Verificar que destino NO existe
    ruta_tag_destino = string_from_format("%s/files/%s/%s", 
                                               punto_montaje_global, file_destino, tag_destino);
    if(access(ruta_tag_destino, F_OK) == 0) {
        log_error(logger, "##%d - TAG: File:Tag destino %s:%s ya existe", query_id, file_destino, tag_destino);
        config_destroy(metadata_origen);
        free(ruta_meta_origen);
        free(ruta_tag_destino);
        return -1;
    }

    // Crear estructura de directorios destino
    ruta_file_destino = string_from_format("%s/files/%s", punto_montaje_global, file_destino);
    ruta_logical_destino = string_from_format("%s/logical_blocks", ruta_tag_destino);

    // Crear directorios (file puede existir, tag no)
    mkdir(ruta_file_destino, 0755);  // Si existe, no importa
    if(mkdir(ruta_tag_destino, 0755) == -1) {
        log_error(logger, "##%d - TAG: No se pudo crear directorio tag destino", query_id);
        error_cleanup();
        return -1;
    }
    if(mkdir(ruta_logical_destino, 0755) == -1) {
        log_error(logger, "##%d - TAG: No se pudo crear logical_blocks destino", query_id);
        error_cleanup();
        return -1;
    }

    // Copiar metadata con estado WORK_IN_PROGRESS
    ruta_meta_destino = string_from_format("%s/metadata.config", ruta_tag_destino);
    FILE* meta_dest = fopen(ruta_meta_destino, "w");
    if(!meta_dest) {
        log_error(logger, "##%d - TAG: No se pudo crear metadata destino", query_id);
        error_cleanup();
        return -1;
    }

    uint32_t tamanio = config_get_int_value(metadata_origen, "TAMAÑO");
    char* bloques_str = config_get_string_value(metadata_origen, "BLOCKS");
    
    if(meta_dest) {
        fprintf(meta_dest, "TAMAÑO=%d\n", tamanio);
        fprintf(meta_dest, "BLOCKS=%s\n", bloques_str);
        fprintf(meta_dest, "ESTADO=WORK_IN_PROGRESS\n");
        fclose(meta_dest);
    }

    // Copiar hard links de logical_blocks
    bloques = parsear_lista_bloques(bloques_str);
    for(int i = 0; i < list_size(bloques); i++) {
        char* nombre_bloque_logico = string_from_format("%06d.dat", i);
        char* ruta_bloque_logico_origen = string_from_format("%s/files/%s/%s/logical_blocks/%s", 
            punto_montaje_global, file_origen, tag_origen, nombre_bloque_logico);
        char* ruta_bloque_logico_destino = string_from_format("%s/%s", 
            ruta_logical_destino, nombre_bloque_logico);

        // Crear hard link al mismo bloque físico
        if(link(ruta_bloque_logico_origen, ruta_bloque_logico_destino) == -1) {
            log_error(logger, "##%d - TAG: Error creando hard link para bloque %d - %s", 
                     query_id, i, strerror(errno));
            free(nombre_bloque_logico);
            free(ruta_bloque_logico_origen);
            free(ruta_bloque_logico_destino);
            list_destroy(bloques);
            error_cleanup();
            return -1;
        }

        free(nombre_bloque_logico);
        free(ruta_bloque_logico_origen);
        free(ruta_bloque_logico_destino);
    }

    // Éxito - limpiar recursos
    list_destroy(bloques);
    config_destroy(metadata_origen);
    free(ruta_meta_origen);
    free(ruta_tag_destino);
    free(ruta_file_destino);
    free(ruta_logical_destino);
    free(ruta_meta_destino);

    log_info(logger, "##%d - Tag creado %s:%s", query_id, file_destino, tag_destino);
    return 0;

}

// T-005: Verificación rápida en Memoria (RAM)
int op_verificar_bloque_md5(const char* md5_hex, int query_id) {
    if (!md5_hex) return -1;

    // Solo consultamos el índice en memoria protegiendo la lectura
    pthread_mutex_lock(&mutex_blocks_hash);
    bool existe = dictionary_has_key(blocks_hash_index, (char*)md5_hex);
    pthread_mutex_unlock(&mutex_blocks_hash);

    if (existe) {
        log_info(logger, "##%d - Check MD5: %s -> EXISTE", query_id, md5_hex);
        return 1; // Existe
    } else {
        log_info(logger, "##%d - Check MD5: %s -> NO EXISTE", query_id, md5_hex);
        return 0; // No existe
    }
}

// T-006: Escritura Segura e Idempotente (CAS)
int op_escribir_bloque_md5_safe(const char* md5_hex, void* contenido, int tam_contenido, int query_id) {
    
    // 1. Lock Global de Indexación (Critical Section: Entry Check)
    pthread_mutex_lock(&mutex_blocks_hash);
    
    // 2. Doble Verificación (Double-Check Locking pattern)
    // Puede que otro hilo haya escrito este bloque justo mientras esperábamos el mutex.
    if (dictionary_has_key(blocks_hash_index, (char*)md5_hex)) {
        log_warning(logger, "##%d - Race Condition Evitada: El bloque %s ya fue escrito por otro hilo.", query_id, md5_hex);
        pthread_mutex_unlock(&mutex_blocks_hash);
        return 0; // Éxito (Idempotente)
    }

    // 3. Si no existe, procedemos a persistir.
    // Mantenemos el lock de hash para evitar que otro intente escribir el mismo hash ahora.

    // A. Buscar bloque libre (Bitmap Lock)
    pthread_mutex_lock(&mutex_bitmap);
    int nuevo_bloque = bitmap_buscar_libre(bitmap_global);
    if (nuevo_bloque != -1) {
        bitmap_ocupar(bitmap_global, nuevo_bloque);
    }
    pthread_mutex_unlock(&mutex_bitmap);

    if (nuevo_bloque == -1) {
        log_error(logger, "##%d - Error: Disco Lleno al intentar escribir bloque MD5", query_id);
        pthread_mutex_unlock(&mutex_blocks_hash); // Liberamos lock principal
        return -1; // Fallo: Storage Full
    }

    // B. Escritura Física (I/O Pesado)
    // Nota: Podríamos liberar el lock de hash aquí si quisiéramos paralelismo máximo en I/O,
    // pero para garantizar consistencia estricta en V1, lo mantenemos hasta actualizar el índice.
    
    char* ruta_bloque = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje_global, nuevo_bloque);
    FILE* f = fopen(ruta_bloque, "w+b");
    
    if (f) {
        // Escribimos el contenido recibido (si es menor al block_size, rellenamos o escribimos justo? 
        // CAS standard: Escribimos el bloque completo. El Worker manda block_size).
        void* buffer_write = calloc(1, block_size_global);
        memcpy(buffer_write, contenido, (tam_contenido > block_size_global) ? block_size_global : tam_contenido);
        
        fwrite(buffer_write, block_size_global, 1, f);
        fclose(f);
        free(buffer_write);

        log_info(logger, "##%d - Bloque Físico Escrito - ID: %d - MD5: %s", query_id, nuevo_bloque, md5_hex);

        // C. Actualizar Índice Hash (Memoria + Persistencia)
        char* nombre_bloque = string_from_format("block%04d", nuevo_bloque);
        
        // Memoria
        agregar_bloque_hash(blocks_hash_index, md5_hex, nombre_bloque);
        
        // Persistencia (Actualizar archivo .config)
        // Nota: Esto podría optimizarse para no reescribir todo el archivo cada vez, 
        // pero por ahora reutilizamos la función existente.
        guardar_blocks_hash_index(punto_montaje_global, blocks_hash_index);

        free(nombre_bloque);

    } else {
        log_error(logger, "##%d - Fallo escritura física en %s", query_id, ruta_bloque);
        // Rollback bitmap? (Opcional, MVP V1 asumimos fatal error)
        pthread_mutex_unlock(&mutex_blocks_hash);
        free(ruta_bloque);
        return -1;
    }

    free(ruta_bloque);

    // 4. Fin Sección Crítica
    pthread_mutex_unlock(&mutex_blocks_hash);
    
    // Retardo simulado (Configuración)
    int retardo = config_get_int_value(config, "RETARDO_ACCESO_BLOQUE");
    usleep(retardo * 1000);

    return 0; // Éxito
}