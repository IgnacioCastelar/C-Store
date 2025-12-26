#include "conexiones_servidor.h"
#include <stdint.h> // Necesario para uint8_t, uint32_t

int iniciar_servidor(int puerto)
{
    int err;
    int socket_servidor;
    char puerto_str[8];
    snprintf(puerto_str, sizeof(puerto_str), "%d", puerto);

    struct addrinfo hints, *servinfo;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    err = getaddrinfo(NULL, puerto_str, &hints, &servinfo);
    if (err != 0) {
        log_info(logger, "Hubo algun tipo de error en getaddrinfo.");
        return -1;
    }

    socket_servidor = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
    
    // Configurar SO_REUSEPORT o SO_REUSEADDR
    int yes = 1;
    setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

    int resultado_bind = bind(socket_servidor, servinfo->ai_addr, servinfo->ai_addrlen);
    if (resultado_bind == -1) {
        log_error(logger, "ERROR al asociar el socket con el puerto");
        freeaddrinfo(servinfo);
        exit(-1);
    }

    int resultado_listen = listen(socket_servidor, SOMAXCONN);
    if (resultado_listen == -1) {
        log_error(logger, "ERROR al escuchar las conexiones entrantes");
        freeaddrinfo(servinfo);
        exit(-1);
    }

    log_info(logger, "Listo para escuchar a mi cliente en puerto %d", puerto);
    freeaddrinfo(servinfo);

    return socket_servidor;
}

int esperar_cliente(int socket_servidor)
{
    int socket_cliente = accept(socket_servidor, NULL, NULL);
    if(socket_cliente == -1) {
        log_info(logger, "Ocurrio un error al aceptar un nuevo cliente");
    }
    return socket_cliente;
}

int recibir_operacion(int socket_cliente)
{
    uint8_t cod_op; // T-001: Leer 1 byte
    if (recv(socket_cliente, &cod_op, sizeof(uint8_t), MSG_WAITALL) > 0)
    {
        log_info(logger, "Se recibio correctamente el codigo de operacion: %d", cod_op);
        return (int)cod_op;
    }
    else
    {
        close(socket_cliente);
        return -1;
    }
}

void *recibir_buffer(int *size, int socket_cliente)
{
    void *buffer;
    uint32_t size_u32; // T-001: Leer 4 bytes unsigned

    recv(socket_cliente, &size_u32, sizeof(uint32_t), MSG_WAITALL);
    *size = (int)size_u32; // Cast a int para compatibilidad de interfaz local, pero red es u32

    if (size_u32 > 0) {
        buffer = malloc(size_u32);
        recv(socket_cliente, buffer, size_u32, MSG_WAITALL);
    } else {
        buffer = NULL;
    }

    return buffer;
}

void recibir_mensaje(int socket_cliente)
{
    int size;
    char *buffer = recibir_buffer(&size, socket_cliente);
    if (buffer) {
        log_info(logger, "Me llego el mensaje %s", buffer);
        free(buffer);
    }
}

t_list *recibir_paquete_servidors(int socket_cliente)
{
    int size;
    int desplazamiento = 0;
    void *buffer;
    t_list *valores = list_create();
    uint32_t tamanio; // T-001: Tamaño de elemento interno debe ser consistente (u32)

    buffer = recibir_buffer(&size, socket_cliente);
    if (!buffer) return valores;

    while (desplazamiento < size)
    {
        memcpy(&tamanio, buffer + desplazamiento, sizeof(uint32_t));
        desplazamiento += sizeof(uint32_t);
        
        char *valor = malloc(tamanio + 1); // +1 para null terminator por seguridad si es string
        memcpy(valor, buffer + desplazamiento, tamanio);
        valor[tamanio] = '\0'; 
        
        desplazamiento += tamanio;
        list_add(valores, valor);
    }
    free(buffer);
    return valores;
}