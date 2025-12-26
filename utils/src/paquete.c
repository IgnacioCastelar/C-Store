#include "paquete.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

/* T-001: Serialización Strict Typing (uint8_t OP, uint32_t Size) */
static void* serializar_paquete(t_paquete* paquete, int* bytes) {
    // Calculamos tamaño total: OpCode(1) + Size(4) + Payload(N)
    *bytes = paquete->buffer->size + sizeof(uint8_t) + sizeof(uint32_t);
    void* magic = malloc(*bytes);
    int desplazamiento = 0;

    // 1. OP_CODE (1 Byte)
    uint8_t codigo = (uint8_t)paquete->codigo_operacion;
    memcpy(magic + desplazamiento, &codigo, sizeof(uint8_t));
    desplazamiento += sizeof(uint8_t);

    // 2. BUFFER_SIZE (4 Bytes)
    uint32_t size = paquete->buffer->size;
    memcpy(magic + desplazamiento, &size, sizeof(uint32_t));
    desplazamiento += sizeof(uint32_t);

    // 3. STREAM (N Bytes) - T-002: Binary Safe
    if (paquete->buffer->size > 0) {
        memcpy(magic + desplazamiento, paquete->buffer->stream, paquete->buffer->size);
    }

    return magic;
}

t_paquete* crear_paquete(op_code codigo) {
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = codigo;
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = 0;
    paquete->buffer->stream = NULL;
    return paquete;
}

void agregar_a_paquete(t_paquete* paquete, void* valor, int tamanio) {
    paquete->buffer->stream = realloc(paquete->buffer->stream, paquete->buffer->size + tamanio);
    // T-002: memcpy para datos binarios
    memcpy(paquete->buffer->stream + paquete->buffer->size, valor, tamanio);
    paquete->buffer->size += tamanio;
}

void enviar_paquete(t_paquete* paquete, int socket) {
    int bytes;
    void* a_enviar = serializar_paquete(paquete, &bytes);
    send(socket, a_enviar, bytes, 0);
    free(a_enviar);
}

/* T-001: Deserialización Strict Typing */
t_paquete* recibir_paquete(int socket) {
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->buffer = NULL;

    // 1. Leer OP_CODE (1 Byte)
    uint8_t codigo;
    if (recv(socket, &codigo, sizeof(uint8_t), MSG_WAITALL) <= 0) {
        free(paquete);
        return NULL;
    }
    paquete->codigo_operacion = (op_code)codigo;

    // 2. Leer SIZE (4 Bytes)
    uint32_t size;
    if (recv(socket, &size, sizeof(uint32_t), MSG_WAITALL) <= 0) {
        free(paquete); // Ya no destruimos paquete completo porque buffer es NULL o basura
        return NULL;
    }

    // 3. Leer Payload
    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = NULL;

    if (size > 0) {
        paquete->buffer->stream = malloc(size);
        if (recv(socket, paquete->buffer->stream, size, MSG_WAITALL) <= 0) {
            destruir_paquete(paquete);
            return NULL;
        }
    }

    return paquete;
}

void destruir_paquete(t_paquete* paquete) {
    if (!paquete) return;
    if (paquete->buffer) {
        if(paquete->buffer->stream) free(paquete->buffer->stream);
        free(paquete->buffer);
    }
    free(paquete);
}