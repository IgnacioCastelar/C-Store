#include "bitmap.h"
#include <sys/stat.h>
#include <sys/mman.h>

t_bitmap* bitmap_crear(const char* punto_montaje, int total_blocks) {
    t_bitmap* bm = malloc(sizeof(t_bitmap));
    bm->total_bits = total_blocks;
    bm->size_bytes = (size_t) ceil((double)total_blocks / 8);
    bm->filepath = string_from_format("%s/bitmap.bin", punto_montaje);

    // corrijo: saco O_TRUNC para no borrar datos si el archivo ya existe
    // (hacer unlink en fresh_start para evitar huecos viejos)
    int fd = open(bm->filepath, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        perror("bitmap_crear: open");
        free(bm->filepath);
        free(bm);
        return NULL;
    }

    // Verificar si es nuevo para inicializar o cargar
    struct stat st;
    fstat(fd, &st);
    bool es_nuevo = (st.st_size == 0);

    if (es_nuevo) {
        if (ftruncate(fd, bm->size_bytes) == -1) {
            perror("bitmap_crear: ftruncate");
            close(fd);
            free(bm->filepath);
            free(bm);
            return NULL;
        }
    }

    bm->data = mmap(NULL, bm->size_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    if (bm->data == MAP_FAILED) {
        perror("bitmap_crear: mmap");
        free(bm->filepath);
        free(bm);
        return NULL;
    }

    // --- OPTIMIZACIÓN: Creamos el bitarray UNA VEZ y lo guardamos ---
    bm->bitarray = bitarray_create_with_mode(bm->data, bm->size_bytes, LSB_FIRST);

    // Inicialización del bitmap SOLO SI ES NUEVO
    if (es_nuevo) {
        memset(bm->data, 0, bm->size_bytes);
        
        // RESERVAR BLOQUE 0: protege bloque de ceros usado por Lazy Truncate.
        // Evita que 'bitmap_buscar_libre' devuelva 0 y Write lo sobrescriba.
        bitarray_set_bit(bm->bitarray, 0);
        // Sincronizar estado inicial
        msync(bm->data, bm->size_bytes, MS_SYNC);
    }
    
    return bm;
}

void bitmap_ocupar(t_bitmap* bm, int bit_index) {
    if (bit_index < 0 || bit_index >= bm->total_bits) return;
    
    // Usamos el bitarray de la estructura
    bitarray_set_bit(bm->bitarray, bit_index);
    
    msync(bm->data, bm->size_bytes, MS_SYNC);
}

void bitmap_liberar(t_bitmap* bm, int bit_index) {
    if (bit_index < 0 || bit_index >= bm->total_bits) return;

    // PROTECCIÓN DEL BLOQUE 0 compartido por el Lazy Truncate.
    // NUNCA debe liberarse, ni siquiera si el contador de referencias llega a 1.
    if (bit_index == 0) {
        return; // Ignoramos la orden de liberar el 0
    }

    // Usamos el bitarray persistente (creado en bitmap_crear)
    bitarray_clean_bit(bm->bitarray, bit_index);
    
    // IMPORTANTE: Guardar cambio en disco
    msync(bm->data, bm->size_bytes, MS_SYNC);
}

bool bitmap_esta_ocupado(t_bitmap* bm, int bit_index) {
    if (bit_index < 0 || bit_index >= bm->total_bits) return true; // Por seguridad
    
    return bitarray_test_bit(bm->bitarray, bit_index);
}

void bitmap_destruir(t_bitmap* bm) {
    if (bm) {
        // Liberar el bitarray auxiliar
        if (bm->bitarray) bitarray_destroy(bm->bitarray);
        
        if (bm->data != MAP_FAILED) {
            msync(bm->data, bm->size_bytes, MS_SYNC);
            munmap(bm->data, bm->size_bytes);
        }
        free(bm->filepath);
        free(bm);
    }
}

int bitmap_buscar_libre(t_bitmap* bm) {
    // Iteramos sobre el total de bits
    for(int i = 0; i < bm->total_bits; i++) {
        if(!bitarray_test_bit(bm->bitarray, i)) {
            return i;
        }
    }
    return -1;
}
