#ifndef PHYSICAL_BLOCKS_H_
#define PHYSICAL_BLOCKS_H_

#include <stdint.h>
#include <stdlib.h>

/**
 * @brief Crea todos los bloques físicos (block0000.dat a blockXXXX.dat)
 * en el directorio <punto_montaje>/physical_blocks.
 *
 * @param punto_montaje Ruta raíz del filesystem
 * @param fs_size Tamaño total del FS en bytes
 * @param block_size Tamaño de cada bloque en bytes
 * @return int 0 si éxito, -1 si error
 */
int physical_blocks_crear_todos(const char* punto_montaje, int fs_size, int block_size);

#endif