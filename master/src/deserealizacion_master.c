#include <deserealizacion_master.h>

t_paquete *recibir_paquete_completo(int socket_fd)
{
    t_paquete *paquete = malloc(sizeof(t_paquete));

    if (!paquete)
        return NULL;

    paquete->buffer = malloc(sizeof(t_buffer));
    if (!paquete->buffer)
    {
        free(paquete);
        return NULL;
    }

    // Código de la operación
    int bytes_recv = recv(socket_fd, &(paquete->codigo_operacion), sizeof(op_code), MSG_WAITALL);
    if (bytes_recv <= 0)
    {
        free(paquete->buffer);
        free(paquete);
        return NULL;
    }

    // Tamaño del buffer
    bytes_recv = recv(socket_fd, &(paquete->buffer->size), sizeof(int), MSG_WAITALL);
    if (bytes_recv <= 0)
    {
        free(paquete->buffer);
        free(paquete);
        return NULL;
    }

    // Contenido del buffer
    if (paquete->buffer->size > 0)
    {
        paquete->buffer->stream = malloc(paquete->buffer->size);
        if (!paquete->buffer->stream)
        {
            free(paquete->buffer);
            free(paquete);
            return NULL;
        }
        bytes_recv = recv(socket_fd, paquete->buffer->stream, paquete->buffer->size, MSG_WAITALL);
        if (bytes_recv <= 0)
        {
            eliminar_paquete(paquete);
            return NULL;
        }
    }
    else
    {
        paquete->buffer->stream = NULL;
    }

    return paquete;
}

void crear_paquete_y_enviar_worker(int socket_cliente, t_buffer *buffer) // se saco el parametro codigo de operacion
{
    t_paquete *paquete = malloc(sizeof(t_paquete));

    paquete->codigo_operacion = PAQUETE;
    paquete->buffer = buffer;

    // Armamos el stream a enviar
    void *a_enviar = malloc(buffer->size + sizeof(uint8_t) + sizeof(uint32_t));
    int offset = 0;

    memcpy(a_enviar + offset, &(paquete->codigo_operacion), sizeof(uint8_t));

    offset += sizeof(uint8_t);
    memcpy(a_enviar + offset, &(paquete->buffer->size), sizeof(uint32_t));
    offset += sizeof(uint32_t);
    memcpy(a_enviar + offset, paquete->buffer->stream, paquete->buffer->size);

    // Por último enviamos
    send(socket_cliente, a_enviar, buffer->size + sizeof(uint8_t) + sizeof(uint32_t), 0);

    // No nos olvidamos de liberar la memoria que ya no usaremos
    free(a_enviar);
    free(paquete->buffer->stream);
    free(paquete->buffer);
    free(paquete);
}

t_handshakeWorker *recibir_paquete_handshake_worker(t_paquete *paquete)
{
    t_handshakeWorker *paquete_worker = malloc(sizeof(t_handshakeWorker));
    int desplazamiento = 0;

    memcpy(&(paquete_worker->len_modulo), paquete->buffer->stream, sizeof(int));
    desplazamiento += sizeof(int);//paquete->buffer->stream += sizeof(int);

    paquete_worker->modulo = malloc(paquete_worker->len_modulo + 1);
    memcpy(paquete_worker->modulo, paquete->buffer->stream + desplazamiento, paquete_worker->len_modulo);
    paquete_worker->modulo[paquete_worker->len_modulo] = '\0'; //paquete->buffer->stream += paquete_worker->len_modulo;
    desplazamiento += paquete_worker->len_modulo;
    
    memcpy(&(paquete_worker->len_id_worker), paquete->buffer->stream + desplazamiento, sizeof(int));
    desplazamiento += sizeof(int); // paquete->buffer->stream += sizeof(int);

    paquete_worker->id_worker = malloc(paquete_worker->len_id_worker + 1);
    memcpy(paquete_worker->id_worker, paquete->buffer->stream + desplazamiento, paquete_worker->len_id_worker);
    paquete_worker->id_worker[paquete_worker->len_id_worker] = '\0';

    return paquete_worker;
}

t_handshakeQuery *recibir_paquete_handshake_query(t_paquete *paquete)
{
    t_handshakeQuery *paquete_query = malloc(sizeof(t_handshakeQuery));
    int desplazamiento = 0;

    //recibo la longitud y el texto del módulo
    memcpy(&(paquete_query->length_modulo), paquete->buffer->stream + desplazamiento, sizeof(uint32_t));
    desplazamiento += sizeof(uint32_t); //paquete->buffer->stream += sizeof(uint32_t);

    paquete_query->modulo = malloc(paquete_query->length_modulo + 1);
    memcpy(paquete_query->modulo, paquete->buffer->stream + desplazamiento, paquete_query->length_modulo);
    paquete_query->modulo[paquete_query->length_modulo] = '\0';
    desplazamiento += paquete_query->length_modulo; //paquete->buffer->stream += paquete_query->length_modulo;

    //recibo el nombre del archivo Query
    memcpy(&(paquete_query->length_nombre_query), paquete->buffer->stream + desplazamiento, sizeof(uint32_t));
    desplazamiento += sizeof(uint32_t); //paquete->buffer->stream += sizeof(uint32_t);

    paquete_query->nombre_query = malloc(paquete_query->length_nombre_query + 1);
    memcpy(paquete_query->nombre_query, paquete->buffer->stream + desplazamiento, paquete_query->length_nombre_query);
    paquete_query->nombre_query[paquete_query->length_nombre_query] = '\0';
    desplazamiento += paquete_query->length_nombre_query;//paquete->buffer->stream += paquete_query->length_nombre_query;

    //recibo la prioridad
    memcpy(&(paquete_query->prioridad), paquete->buffer->stream + desplazamiento, sizeof(uint8_t));

    return paquete_query;
}
int recibir_paquete_pc(t_paquete* paquete)
{
    int pc;

    memcpy(&pc, paquete->buffer->stream, sizeof(int));

    return pc;
}

char *deserializar_string(void *stream, int *desplazamiento)
{
    int tamanio;
    memcpy(&tamanio, stream + *desplazamiento, sizeof(int));
    *desplazamiento += sizeof(int);

    char *str = malloc(tamanio); // ya viene con \0 porque lo mandaste con strlen + 1
    memcpy(str, stream + *desplazamiento, tamanio);
    *desplazamiento += tamanio;

    return str;
}


int deserializar_int(void *stream, int *desplazamiento)
{
    int tamanio;
    memcpy(&tamanio, stream + *desplazamiento, sizeof(int));
    *desplazamiento += sizeof(int);

    int valor;
    memcpy(&valor, stream + *desplazamiento, tamanio);
    *desplazamiento += tamanio;

    return valor;
}
