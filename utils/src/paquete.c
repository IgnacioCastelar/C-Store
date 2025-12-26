#include "paquete.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>


static void* serializar_paquete(t_paquete* paquete, int* bytes) {
    *bytes = paquete->buffer->size + 2*sizeof(int);
    void* magic = malloc(*bytes);

    int desplazamiento = 0;
    memcpy(magic + desplazamiento, &(paquete->codigo_operacion), sizeof(int));
    desplazamiento += sizeof(int);
    memcpy(magic + desplazamiento, &(paquete->buffer->size), sizeof(int));
    desplazamiento += sizeof(int);
    memcpy(magic + desplazamiento, paquete->buffer->stream, paquete->buffer->size);

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
    memcpy(paquete->buffer->stream + paquete->buffer->size, valor, tamanio);
    paquete->buffer->size += tamanio;
}

void enviar_paquete(t_paquete* paquete, int socket) {
    int bytes;
    void* a_enviar = serializar_paquete(paquete, &bytes);
    send(socket, a_enviar, bytes, 0);
    
    free(a_enviar);
}

t_paquete* recibir_paquete(int socket) {
    t_paquete* paquete = malloc(sizeof(t_paquete));

    int codigo;
    if (recv(socket, &codigo, sizeof(int), MSG_WAITALL) <= 0) {
        free(paquete);
        return NULL;
    }
    paquete->codigo_operacion = codigo;

    int size;
    if (recv(socket, &size, sizeof(int), MSG_WAITALL) <= 0) {
        free(paquete);
        return NULL;
    }

    paquete->buffer = malloc(sizeof(t_buffer));
    paquete->buffer->size = size;
    paquete->buffer->stream = malloc(size);

    if (recv(socket, paquete->buffer->stream, size, MSG_WAITALL) <= 0) {
        destruir_paquete(paquete);
        return NULL;
    }

    return paquete;
}



void destruir_paquete(t_paquete* paquete) {
    if (!paquete) return;
    if (paquete->buffer) {
        free(paquete->buffer->stream);
        free(paquete->buffer);
    }
    free(paquete);
}