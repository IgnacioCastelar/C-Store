#ifndef BITMAP_H_
#define BITMAP_H_

#include <stdint.h>
#include <sys/types.h>
#include <stdio.h>

#include <commons/bitarray.h>
#include <commons/string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <stdlib.h>

typedef struct {
    size_t size_bytes;
    int total_bits;
    char* filepath;
    char* data;
    t_bitarray* bitarray;
} t_bitmap;

t_bitmap* bitmap_crear(const char* punto_montaje, int total_blocks);
void bitmap_ocupar(t_bitmap* bm, int bit_index);
void bitmap_liberar(t_bitmap* bm, int bit_index);
bool bitmap_esta_ocupado(t_bitmap* bm, int bit_index);
void bitmap_destruir(t_bitmap* bm);

int bitmap_buscar_libre(t_bitmap* bm);
#endif
