#include "serializacion_envio.h"
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>

void crear_paquete_y_enviar_master(int socket_cliente, op_code codigo, t_buffer *buffer) // se saco el parametro codigo de operacion
{
    t_paquete *paquete = malloc(sizeof(t_paquete));

    paquete->codigo_operacion = codigo;
    paquete->buffer = buffer;

    // Tamaño total: Payload + OpCode(1) + Size(4)
    int total_bytes = buffer->size + sizeof(uint8_t) + sizeof(uint32_t);
    void *a_enviar = malloc(total_bytes);
    int offset = 0;

    // 1. OP_CODE (uint8_t)
    uint8_t op = (uint8_t)paquete->codigo_operacion;
    memcpy(a_enviar + offset, &op, sizeof(uint8_t));
    offset += sizeof(uint8_t);

    // 2. SIZE (uint32_t)
    uint32_t size = paquete->buffer->size;
    memcpy(a_enviar + offset, &size, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    // 3. STREAM
    if (buffer->size > 0) {
        memcpy(a_enviar + offset, paquete->buffer->stream, paquete->buffer->size);
    }

    // Por último enviamos
    send(socket_cliente, a_enviar, total_bytes, 0);

    free(a_enviar);
    // Nota: buffer->stream y buffer se liberan aquí según lógica original
    if (paquete->buffer->stream) free(paquete->buffer->stream);
    free(paquete->buffer);
    free(paquete);
}

t_buffer *buffer_create(uint32_t size)
{
    t_buffer *buffer = malloc(sizeof(t_buffer));
    if (buffer == NULL) return NULL;

    buffer->offset = 0;
    buffer->size = size;

    if (size > 0) {
        buffer->stream = calloc(1, size);
    } else {
        buffer->stream = NULL;
    }
    
    if (size > 0 && buffer->stream == NULL) {
        free(buffer);
        return NULL;
    }

    return buffer;
}

void buffer_destroy(t_buffer *buffer)
{
    if (buffer == NULL) return; // Nada que liberar
    if (buffer->stream != NULL) free(buffer->stream);
    free(buffer);
}

void buffer_add(t_buffer *buffer, void *data, uint32_t data_size)
{
    // Aumentar el tamaño total
    buffer->size += data_size;

    // Redimensionar el stream
    buffer->stream = realloc(buffer->stream, buffer->size);

    // Copiar los nuevos datos en la posición actual
    memcpy((char *)buffer->stream + buffer->offset, data, data_size);
    
    // Avanzar el offset
    buffer->offset += data_size;
}

void *buffer_read(t_buffer *buffer, uint32_t data_size)
{
    if (buffer == NULL || buffer->stream == NULL) return NULL; // Verificar que haya datos suficientes para leer
    if (buffer->offset + data_size > buffer->size) return NULL;  // intento de leer más de lo que hay

    // Reservar memoria para devolver los datos leídos
    void *data = malloc(data_size);
    if (data == NULL) return NULL; // error de memoria

    // Copiar desde el stream
    memcpy(data, (char *)buffer->stream + buffer->offset, data_size);
    
    // Avanzar el offset
    buffer->offset += data_size;

    return data;
}

// abstraccion de los buffers


// -----INT--
void buffer_add_int(t_buffer *buffer, int data)
{
    buffer_add(buffer, &data, sizeof(int));
}

int buffer_read_int(t_buffer *buffer)
{
    int *data = (int *)buffer_read(buffer, sizeof(int));
    if(!data) return 0; // Error handling simplificado
    int value = *data;
    free(data);
    return value;
}

// --- UINT32 ---
void buffer_add_uint32(t_buffer *buffer, uint32_t data)
{
    buffer_add(buffer, &data, sizeof(uint32_t));
}

uint32_t buffer_read_uint32(t_buffer *buffer)
{
    uint32_t *data = (uint32_t *)buffer_read(buffer, sizeof(uint32_t));
    if(!data) return 0;
    uint32_t value = *data;
    free(data);
    return value;
}

// --- UINT8 ---
void buffer_add_uint8(t_buffer *buffer, uint8_t data)
{
    buffer_add(buffer, &data, sizeof(uint8_t));
}

uint8_t buffer_read_uint8(t_buffer *buffer)
{
    uint8_t *data = (uint8_t *)buffer_read(buffer, sizeof(uint8_t));
    if(!data) return 0;
    uint8_t value = *data;
    free(data);
    return value;
}

// --- STRING ---
void buffer_add_string(t_buffer *buffer, uint32_t length, char *string)
{
    // Primero guardamos la longitud
    buffer_add_uint32(buffer, length);
    // Después guardamos el contenido del string (sin '\0')
    buffer_add(buffer, string, length);
}

char *buffer_read_string(t_buffer *buffer, uint32_t *length)
{
    // Leemos la longitud primero
    *length = buffer_read_uint32(buffer);
    if (*length == 0) return strdup(""); 

    // Reservamos memoria para el string (+1 para '\0')
    char *str = malloc(*length + 1);
    
    // Leemos los bytes del string
    void *data = buffer_read(buffer, *length);
    
    if (data) {
        memcpy(str, data, *length);
        free(data);
    }
    
    str[*length] = '\0'; // Null-terminated
    return str;
}