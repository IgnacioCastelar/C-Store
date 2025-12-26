#include "main_query.h"

t_config *config;
t_log *logger;

int main(int argc, char *argv[])
{

    int socket_conexion;
    char *ip;
    int puerto;

    char ruta_config[120];
    char *path_config = argv[1];
    char *nombre_query_arg = argv[2];
    sprintf(ruta_config, "../config/%s.config", path_config);

    config = iniciar_config(ruta_config);
    logger = iniciar_logger(nombre_query_arg);

    ip = config_get_string_value(config, "IP_MASTER");
    // puerto = config_get_string_value(config, "PUERTO_MASTER");
    puerto = config_get_int_value(config, "PUERTO_MASTER");

    socket_conexion = crear_conexion_cliente(ip, puerto);

    log_info(logger, "## Conexión al Master exitosa. IP: %s, Puerto: %d", ip, puerto);

    t_datosHandshake *datos_enviar = malloc(sizeof(t_datosHandshake));

    datos_enviar->nombre_query = argv[2];
    datos_enviar->length_nombre_query = strlen(datos_enviar->nombre_query) + 1; // por las dudas un +1
    datos_enviar->modulo = "QUERY_CONTROL";
    datos_enviar->length_modulo = strlen(datos_enviar->modulo);
    datos_enviar->prioridad = (uint8_t)atoi(argv[3]);

    log_info(logger, "## Solicitud de ejecución de Query: %s, prioridad: %d", 
             datos_enviar->nombre_query, datos_enviar->prioridad);

    t_paquete *paquete = crear_paquete_handshake_query(datos_enviar);

    enviar_paquete(paquete, socket_conexion);
    eliminar_paquete(paquete);

    atender_mensajes_master(socket_conexion);

    free(datos_enviar);

    terminar_programa(socket_conexion, logger, config);

    return 0;
}

t_paquete *crear_paquete_handshake_query(t_datosHandshake *datos_enviar)
{
    t_paquete *paquete = crear_paquete(HANDSHAKE_QUERY);

    agregar_a_paquete(paquete, &datos_enviar->length_modulo, sizeof(uint32_t));
    agregar_a_paquete(paquete, (void *)datos_enviar->modulo, datos_enviar->length_modulo);

    agregar_a_paquete(paquete, &datos_enviar->length_nombre_query, sizeof(uint32_t));
    agregar_a_paquete(paquete, (void *)datos_enviar->nombre_query, datos_enviar->length_nombre_query);

    agregar_a_paquete(paquete, &datos_enviar->prioridad, sizeof(uint8_t));

    return paquete;
}

void atender_mensajes_master(int conexion)
{
    while (1)
    {

        int desplazamiento = 0;
        t_paquete *paquete = recibir_paquete(conexion);

        if (!paquete)
        {
            log_error(logger, "Conexión con el Master perdida. Cerrando hilo de escucha.");
            break;
        }

        switch (paquete->codigo_operacion)
        {
        case LECTURA_QUERY:
            char *file = deserializar_string(paquete->buffer->stream, &desplazamiento);
            char *tag = deserializar_string(paquete->buffer->stream, &desplazamiento);
            char *contenido = deserializar_string(paquete->buffer->stream, &desplazamiento);


            log_info(logger,
                     "## Lectura realizada: File <%s:%s>, contenido: %s",
                     file, tag, contenido);

            free(file);
            free(tag);
            free(contenido);
            eliminar_paquete(paquete);
            break;

        case FIN_QUERY:
            char *motivo_fin_query = deserializar_string(paquete->buffer->stream, &desplazamiento);
            log_info(logger, "## Query Finalizada - %s", motivo_fin_query);

            free(motivo_fin_query);
            eliminar_paquete(paquete);

            terminar_programa(conexion, logger, config);
            exit(EXIT_SUCCESS);
            break;
        default:
            log_warning(logger, "Codigo de operacion desconocido");
            eliminar_paquete(paquete);
            break;
        }
    }
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

t_config *iniciar_config(char *path_config)
{
    // t_config *nuevo_config = config_create("/home/utnso/tp-2025-2c-NootNoot/query_control/ejemplo_query.config");
    t_config *nuevo_config = config_create(path_config);
    return nuevo_config;
}

t_log *iniciar_logger(char *nombre_query)
{

    char *nombre_log = string_from_format("log_query_%s_%d.log", nombre_query, getpid());

    t_log *nuevo_logger = log_create(nombre_log, "QueryControl", 1, LOG_LEVEL_INFO);

    free(nombre_log);

    return nuevo_logger;
}

void terminar_programa(int conexion, t_log *logger, t_config *config)
{
    log_destroy(logger);
    config_destroy(config);
    liberar_conexion(conexion);
}