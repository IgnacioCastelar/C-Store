#ifndef MAINSTORAGED_H_
#define MAINSTORAGED_H_

#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/dictionary.h>
#include "bitmap.h"

// VARIABLES GLOBALES (Extern)

extern t_log *logger;
extern t_config *config;
extern t_config *config_superblock;

// Estado del FileSystem
extern char *punto_montaje_global;
extern t_bitmap *bitmap_global;
extern int block_size_global;
extern t_dictionary *blocks_hash_index;

// PROTOTIPOS DEL MAIN

void limpiar_recursos();

#endif