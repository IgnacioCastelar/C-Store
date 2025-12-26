// blocks_hash_index.h
#ifndef BLOCKS_HASH_INDEX_H
#define BLOCKS_HASH_INDEX_H

#include <commons/string.h>
#include <commons/collections/dictionary.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/crypto.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


// Declaraciones de funciones

// Carga el archivo blocks_hash_index.config en un dictionary: hash -> "blockXXXX"
t_dictionary* cargar_blocks_hash_index(const char* punto_montaje);

// Guarda el dictionary de vuelta al archivo
void guardar_blocks_hash_index(const char* punto_montaje, t_dictionary* index);

// Obtiene el nombre del bloque físico (ej: "block0003") a partir de un hash
// Retorna NULL si no existe
char* bloque_fisico_por_hash(t_dictionary* index, const char* hash);

// Agrega una entrada (hash, "blockXXXX") al índice (en memoria y persistente)
void agregar_bloque_hash(t_dictionary* index, const char* hash, const char* bloque_fisico);

// Genera el hash MD5 de un bloque de datos (de tamaño block_size)
char* hash_de_bloque(void* contenido, size_t tamanio);

#endif