#ifndef OPERACIONES_H_
#define OPERACIONES_H_

#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <math.h>
#include "bitmap.h"
#include "physical_blocks.h"
#include "blocks_hash_index.h"

// CORRECCIÓN: Quitamos "utils/src/"
#include "serializacion_envio.h" 

// Variables globales que operaciones necesita conocer/modificar
extern t_log *logger;
extern char *punto_montaje_global;
extern t_bitmap *bitmap_global;
extern int block_size_global;
extern t_dictionary *blocks_hash_index;
extern t_config *config; // Para consulta_fresh

// --- Funciones de Operatoria (Worker) ---
int op_crear_file_tag(const char *file, const char *tag, int query_id);
int op_truncar_file_tag(const char *file, const char *tag, uint32_t nuevo_tam, int query_id);
int op_escribir_bloque(const char* file, const char* tag, uint32_t bloque_logico, void* contenido, int tam_contenido, int query_id);
void* op_leer_bloque(const char* file, const char* tag, uint32_t bloque_logico, int query_id);
int op_commit_file_tag(const char* file, const char* tag, int query_id);
int op_eliminar_tag(const char* file, const char* tag, int query_id);
int op_crear_tag(const char* file_origen, const char* tag_origen, const char* file_destino, const char* tag_destino, int query_id);
int op_verificar_bloque_md5(const char* md5_hex, int query_id);
int op_escribir_bloque_md5_safe(const char* md5_hex, void* contenido, int tam_contenido, int query_id);

// --- Funciones de Administración del FS (Movidas desde main) ---
void consulta_fresh();
void inicializar_fs_fresh(const char *punto_montaje);
void borrar_contenido_directorio(char *path);
int evaluar_valor(char *valor_string);

// Auxiliares internas
t_list* parsear_lista_bloques(const char* bloques_str);
char* serializar_lista_bloques(t_list* bloques);
char* hash_de_bloque(void* contenido, size_t tamanio);

#endif
