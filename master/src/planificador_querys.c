#include <planificador_querys.h>

static int siguiente_id = 0;

t_propQuery *crear_query_para_ready(char *nombre, uint8_t prioridad, int fd_conexion) // se podria dejar en otro archivo o en main_master
{
    t_propQuery *query = malloc(sizeof(t_propQuery));
    query->nombre_query = nombre;
    query->length_nombre_query = strlen(query->nombre_query);
    query->program_counter = 0;
    query->id_query = siguiente_id++;
    query->estado_actual = READY;
    query->prioridad = prioridad;
    query->fd_socket = fd_conexion;
    query->temporizador = temporal_create(); // inicia el cronometro

    return query;
}

t_propQuery *buscar_y_remover_query(int query_id)
{
    bool _es_la_query(void *elem)
    {
        t_propQuery *candidato = (t_propQuery *)elem;
        return candidato->id_query == query_id;
    };
    t_propQuery *query_encontrada = NULL;

    // busco en la cola de querys
    pthread_mutex_lock(&mutex_ready);
    query_encontrada = (t_propQuery *)list_remove_by_condition(cola_querys_ready, _es_la_query);
    pthread_mutex_unlock(&mutex_ready);

    if (query_encontrada)
    {
        return query_encontrada;
    }

    // Si no esta en la cola de ready, busco en la de exec
    pthread_mutex_lock(&mutex_exec);
    query_encontrada = (t_propQuery *)list_remove_by_condition(querys_exec, _es_la_query);
    pthread_mutex_unlock(&mutex_exec);

    return query_encontrada;
}

void iniciar_planificador_corto_plazo()
{
    pthread_t hilo_planificador;

    pthread_create(&hilo_planificador, NULL, ciclo_planificador_corto_plazo, NULL);
    pthread_detach(hilo_planificador);

    char *alg = config_get_string_value(config, "ALGORITMO_PLANIFICACION");
    if (tiempo_aging > 0 && strcmp(alg, "PRIORIDADES") == 0)
    {
        pthread_t hilo_aging;
        pthread_create(&hilo_aging, NULL, aplicar_aging_a_cola_ready, NULL);
        pthread_detach(hilo_aging);
    }
}

void *aplicar_aging_a_cola_ready(void *args)
{
    //log_info(logger, "Hilo de Aging iniciado. Intervalo: %d ms", tiempo_aging);

    // Bucle infinito: El hilo revisa periódicamente
    while (1)
    {
        usleep(tiempo_aging * 1000);

        pthread_mutex_lock(&mutex_ready);

        if (!list_is_empty(cola_querys_ready))
        {
            list_iterate(cola_querys_ready, aplicar_aging);

            sem_post(&sem_evento_planificador);
        }

        pthread_mutex_unlock(&mutex_ready);
    }
    return NULL;
}

void aplicar_aging(void *elemento)
{
    t_propQuery *query = (t_propQuery *)elemento;

    if (query->temporizador == NULL)
        return;

    int tiempo_en_ready_ms = temporal_gettime(query->temporizador);

    if (tiempo_en_ready_ms >= tiempo_aging)
    {
        if (query->prioridad > 0)
        {
            uint8_t prioridad_anterior = query->prioridad;
            query->prioridad--;

            log_info(logger, "##%d Cambio de prioridad: %d - %d",
                     query->id_query, prioridad_anterior, query->prioridad);
        }

        temporal_destroy(query->temporizador);
        query->temporizador = temporal_create();
    }
}

void *ciclo_planificador_corto_plazo(void *arg)
{

    log_info(logger, "Inicia el planificador corto plazo!");
    while (1)
    {
        sem_wait(&sem_evento_planificador);

        switch (algoritmo_a_int(config_get_string_value(config, "ALGORITMO_PLANIFICACION")))
        {
        case 1:
            algoritmo_fifo();
            break;
        case 2:
            algoritmo_prioridades();
            break;
        default:
            log_error(logger, "Algoritmo de planificacion no valido.");
            break;
        }
    }
    return NULL;
}

int algoritmo_a_int(char *desdeLaConfig)
{
    if (strcmp(desdeLaConfig, "FIFO") == 0)
        return 1;
    if (strcmp(desdeLaConfig, "PRIORIDADES") == 0)
        return 2;
    else
        return -1;
}

void algoritmo_fifo()
{
    asignar_query_a_worker();
}

void algoritmo_prioridades()
{

    pthread_mutex_lock(&mutex_ready);
    bool hay_pendientes = !list_is_empty(cola_querys_ready);
    pthread_mutex_unlock(&mutex_ready);

    if (!hay_pendientes)
    {
        asignar_query_a_worker();
        return;
    }

    pthread_mutex_lock(&mutex_ready);
    ordernar_segun_prioridad();
    pthread_mutex_unlock(&mutex_ready);

    asignar_query_a_worker();

    pthread_mutex_lock(&mutex_worker);
    bool hay_workers_libres = (contador_workers_libres > 0);
    pthread_mutex_unlock(&mutex_worker);

    if (hay_workers_libres)
        return;

    pthread_mutex_lock(&mutex_ready);
    if (list_is_empty(cola_querys_ready))
    {
        pthread_mutex_unlock(&mutex_ready);
        return;
    }
    t_propQuery *query_candidato = list_get(cola_querys_ready, 0);
    pthread_mutex_unlock(&mutex_ready);

    pthread_mutex_lock(&mutex_exec);
    if (list_is_empty(querys_exec))
    {
        pthread_mutex_unlock(&mutex_exec);
        return;
    }

    list_sort(querys_exec, comparar_prioridad);

    int indice_victima = list_size(querys_exec) - 1;
    t_propQuery *query_en_exec_victima = list_get(querys_exec, indice_victima);
    pthread_mutex_unlock(&mutex_exec);

    if (query_candidato->prioridad >= query_en_exec_victima->prioridad)
    {
        return;
    }

    pthread_mutex_lock(&mutex_worker);
    t_worker_conectada *worker_victima = encontrar_worker_con_query(workers_conectados, query_en_exec_victima->id_query);
    pthread_mutex_unlock(&mutex_worker);

    if (!worker_victima)
    {
        return;
    }

    // --- EJECUCION DEL DESALOJO ---
    log_info(logger, "---INICIANDO DESALOJO---: Candidato %d (Prio %d) desaloja a Víctima %d (Prio %d) en Worker %d (Socket FD: %d)",
             query_candidato->id_query, query_candidato->prioridad,
             query_en_exec_victima->id_query, query_en_exec_victima->prioridad,
             worker_victima->id_worker, worker_victima->socket_cliente);

    pedido_desalojo(worker_victima->socket_cliente);

    log_info(logger, "Esperando confirmación del desalojo...");
    sem_wait(&worker_victima->sem_desalojo);

    pthread_mutex_lock(&mutex_ready);
    list_remove_element(cola_querys_ready, query_candidato);

    if (query_candidato->temporizador)
    {
        temporal_destroy(query_candidato->temporizador);
        query_candidato->temporizador = NULL;
    }
    pthread_mutex_unlock(&mutex_ready);

    pthread_mutex_lock(&mutex_exec);
    list_remove_element(querys_exec, query_en_exec_victima);

    cambiar_estado_query(query_candidato, EXEC);
    list_add(querys_exec, query_candidato);
    pthread_mutex_unlock(&mutex_exec);

    cambiar_estado_query(query_en_exec_victima, READY);

    query_en_exec_victima->temporizador = temporal_create();

    pthread_mutex_lock(&mutex_ready);
    list_add(cola_querys_ready, query_en_exec_victima);
    pthread_mutex_unlock(&mutex_ready);

    worker_victima->query_en_ejecucion = query_candidato;

    log_info(logger, "## Se desaloja la Query %d (%d) del Worker %d Motivo: %s",
             query_en_exec_victima->id_query, query_en_exec_victima->prioridad,
             worker_victima->id_worker, motivo_to_string(PRIORIDAD));

    ejecutar_en_worker(query_candidato, worker_victima);
}

void asignar_query_a_worker()
{
    t_worker_conectada *worker_elegido = NULL;

    pthread_mutex_lock(&mutex_worker);
    for (int i = 0; i < list_size(workers_conectados); i++)
    {
        t_worker_conectada *w = list_get(workers_conectados, i);
        if (w->libre)
        {
            worker_elegido = w;
            worker_elegido->libre = false;
            contador_workers_libres--;
            break;
        }
    }
    pthread_mutex_unlock(&mutex_worker);

    if (worker_elegido == NULL)
    {
        return;
    }

    t_propQuery *query_ejecutar = NULL;

    pthread_mutex_lock(&mutex_ready);
    if (!list_is_empty(cola_querys_ready))
    {
        query_ejecutar = list_remove(cola_querys_ready, 0);
    }
    pthread_mutex_unlock(&mutex_ready);

    if (query_ejecutar != NULL)
    {
        if (query_ejecutar->temporizador != NULL)
        {
            temporal_destroy(query_ejecutar->temporizador);
            query_ejecutar->temporizador = NULL;
        }

        cambiar_estado_query(query_ejecutar, EXEC);

        pthread_mutex_lock(&mutex_exec);
        list_add(querys_exec, query_ejecutar);
        pthread_mutex_unlock(&mutex_exec);

        worker_elegido->query_en_ejecucion = query_ejecutar;

        log_info(logger, "## Se envía la Query %d (%d) al Worker %d",
                 query_ejecutar->id_query, query_ejecutar->prioridad, worker_elegido->id_worker);

        ejecutar_en_worker(query_ejecutar, worker_elegido);
    }
    else
    {
        pthread_mutex_lock(&mutex_worker);
        worker_elegido->libre = true;
        contador_workers_libres++;
        pthread_mutex_unlock(&mutex_worker);
    }
}

char *motivo_to_string(t_motivo_desalojo motivo)
{
    switch (motivo)
    {
    case DESCONEXION:
        return "DESCONEXION";
    case PRIORIDAD:
        return "PRIORIDAD";
    default:
        return "DESCONOCIDO";
    }
}

t_worker_conectada *encontrar_worker_con_query(t_list *lista, int id_query)
{
    for (int i = 0; i < list_size(lista); i++)
    {
        t_worker_conectada *worker = list_get(lista, i);

        if (worker != NULL && worker->query_en_ejecucion != NULL)
        {
            if (worker->query_en_ejecucion->id_query == id_query)
            {
                return worker;
            }
        }
    }
    return NULL;
}

t_worker_conectada *encontrar_worker_por_id(t_list *lista, int id_worker)
{
    for (int i = 0; i < list_size(lista); i++)
    {
        t_worker_conectada *worker = list_get(lista, i);
        if (worker != NULL && worker->id_worker == id_worker)
        {
            return worker;
        }
    }
    return NULL;
}

void pedido_desalojo(int fd_cliente)
{
    int valor_c = 1;
    t_paquete *paquete = crear_paquete(OP_DESALOJO_QUERY);
    agregar_a_paquete(paquete, &valor_c, sizeof(int));
    enviar_paquete(paquete, fd_cliente);
    destruir_paquete(paquete);
}

bool comparar_prioridad(void *a, void *b)
{
    t_propQuery *q1 = (t_propQuery *)a;
    t_propQuery *q2 = (t_propQuery *)b;

    return q1->prioridad < q2->prioridad;
}

void ordernar_segun_prioridad()
{
    list_sort(cola_querys_ready, comparar_prioridad);
}

void ejecutar_en_worker(t_propQuery *query, t_worker_conectada *worker_libre)
{

    // Envio paquete para avisar que hay un query en ejecucion

    t_paquete *paquete = crear_paquete(OP_EXEC_QUERY);

    agregar_a_paquete(paquete, &query->length_nombre_query, sizeof(int));
    agregar_a_paquete(paquete, (void *)query->nombre_query, query->length_nombre_query);
    agregar_a_paquete(paquete, &query->id_query, sizeof(int));
    agregar_a_paquete(paquete, &query->program_counter, sizeof(int));
    enviar_paquete(paquete, worker_libre->socket_cliente);

    eliminar_paquete(paquete);
}

t_propQuery *cambiar_estado_query(t_propQuery *query, t_estado_query nuevo_estado)
{
    query->estado_actual = nuevo_estado;

    return query;
}

t_propQuery *buscar_query_por_id(t_list *lista, int idQ)
{
    for (int i = 0; i < list_size(lista); i++)
    {
        t_propQuery *query = list_get(lista, i);
        if (query->id_query == idQ)
        {
            return query;
        }
    }
    return NULL;
}

t_propQuery *enviar_a_exit(t_propQuery *query)
{
    cambiar_estado_query(query, EXIT);
    list_add(querys_exit, query);

    return query;
}

void destruir_query_en_exit(t_propQuery* query)
{
    pthread_mutex_lock(&mutex_exit);
    list_remove_element(querys_exit, query);
    pthread_mutex_unlock(&mutex_exit);

    destruir_query(query);
}

char *estado_a_string(t_estado_query estado)
{
    switch (estado)
    {
    case READY:
        return "READY";
    case EXEC:
        return "EXEC";
    case EXIT:
        return "EXIT";
    default:
        return "DESCONOCIDO";
    }
}

void paquete_worker(t_worker_conectada *worker, t_paquete *paquete)
{
    t_propQuery *query_asociada = worker->query_en_ejecucion;

    switch (paquete->codigo_operacion)
    {

    case OP_RESPUESTA_DESALOJO:
        int valor_pc;

        if (paquete->buffer->size >= sizeof(int))
        {
            memcpy(&valor_pc, paquete->buffer->stream, sizeof(int));
        }
        else
        {
            //log_info(logger, "Estoy enviando valor default");
            valor_pc = 0;
        }

        log_info(logger, "Respuesta de desalojo recibida del Worker %d. PC: %d", worker->id_worker, valor_pc);

        if (worker->query_en_ejecucion != NULL)
        {
            worker->query_en_ejecucion->program_counter = valor_pc;
        }

        sem_post(&worker->sem_desalojo);
        break;
    case OP_RESULTADO_LECTURA:
        if (query_asociada)
        {
            int desplazamiento = 0;

            // FILE
            char *file = deserializar_string(paquete->buffer->stream, &desplazamiento);

            // TAG
            char *tag = deserializar_string(paquete->buffer->stream, &desplazamiento);

            // CONTENIDO
            char *lectura = deserializar_string(paquete->buffer->stream, &desplazamiento);

            // Log no obligatorio (debug interno)
            /*log_info(logger, "Resultado de READ recibido del Worker %d -> %s:%s = \"%s\"",
                worker->id_worker, file, tag, lectura);*/

            log_info(logger, "## Se envía un mensaje de lectura de la Query %d en el Worker %d al Query Control",
                     query_asociada->id_query, worker->id_worker);

            //log_error(logger, "LECTURA RECIBIDA: %s", lectura);

            t_paquete *paquete_para_qc = crear_paquete(LECTURA_QUERY);

            uint32_t lf = strlen(file) + 1;
            uint32_t lt = strlen(tag) + 1;
            uint32_t lc = strlen(lectura) + 1;

            agregar_a_paquete(paquete_para_qc, &lf, sizeof(uint32_t));
            agregar_a_paquete(paquete_para_qc, file, lf);

            agregar_a_paquete(paquete_para_qc, &lt, sizeof(uint32_t));
            agregar_a_paquete(paquete_para_qc, tag, lt);

            agregar_a_paquete(paquete_para_qc, &lc, sizeof(uint32_t));
            agregar_a_paquete(paquete_para_qc, lectura, lc);

            enviar_paquete(paquete_para_qc, query_asociada->fd_socket);
            eliminar_paquete(paquete_para_qc);

            free(file);
            free(tag);
            free(lectura);
        }
        break;
    case OP_QUERY_END:

        pthread_mutex_lock(&mutex_exec);
        list_remove_element(querys_exec, query_asociada);
        pthread_mutex_unlock(&mutex_exec);

        enviar_a_exit(query_asociada);

        char *motivo = "EXITO";

        bool liberar_motivo = false;

        if (paquete->buffer->size > 0)
        {
            int desplazamiento = 0;
            motivo = deserializar_string(paquete->buffer->stream, &desplazamiento);
            liberar_motivo = true;
        }
        else
        {
            motivo = "EXITO";
        }

        t_paquete *paquete_fin = crear_paquete(FIN_QUERY);
        uint32_t len_motivo = strlen(motivo) + 1;

        agregar_a_paquete(paquete_fin, &len_motivo, sizeof(uint32_t));
        agregar_a_paquete(paquete_fin, motivo, len_motivo);

        enviar_paquete(paquete_fin, query_asociada->fd_socket);
        eliminar_paquete(paquete_fin);

        if (liberar_motivo)
        {
            free(motivo);
        }

        pthread_mutex_lock(&mutex_worker);
        worker->query_en_ejecucion = NULL;
        worker->libre = true;

        contador_workers_libres++;
        pthread_mutex_unlock(&mutex_worker);
        log_info(logger, "## Se terminó la Query %d en el Worker %d", query_asociada->id_query, worker->id_worker);

        close(query_asociada->fd_socket);
        destruir_query_en_exit(query_asociada);
        sem_post(&sem_hay_workers);
        sem_post(&sem_evento_planificador);
        break;
    default:
        log_warning(logger, "Código de operación %d desconocido recibido del Worker %d", paquete->codigo_operacion, worker->id_worker);
        break;
    }
}

void desconexion_worker(t_worker_conectada *worker)
{
    //log_warning(logger, "Iniciando desconexión del Worker %d (Socket %d)", worker->id_worker, worker->socket_cliente);

    pthread_mutex_lock(&mutex_worker);

    // buscamos y removemos de la lista.
    t_worker_conectada *worker_encontrada = encontrar_worker_por_id(workers_conectados, worker->id_worker);

    if (!worker_encontrada)
    {
        log_error(logger, "Error: Worker %d se desconectó, pero no estaba en la lista.", worker->id_worker);
        pthread_mutex_unlock(&mutex_worker);
        return;
    }

    // guardo la query y la sac de la lista
    t_propQuery *query_afectada = worker_encontrada->query_en_ejecucion;
    list_remove_element(workers_conectados, worker_encontrada);
    contador_workers--;

    // decremento porque hay un worker menos
    // sem_wait(&sem_hay_workers);

    pthread_mutex_unlock(&mutex_worker);

    if (query_afectada)
    {

        log_info(logger, "## Se desconecta el Worker %d - Se finaliza la Query %d Cantidad total de Workers: %d", 
                 worker->id_worker, query_afectada->id_query, contador_workers);
        
        //aviso a la query que un worker finalizo
        t_paquete *paquete_fin = crear_paquete(FIN_QUERY);
        char *motivo_fin_query = motivo_to_string(DESCONEXION);
        uint32_t len_motivo_fin_query = strlen(motivo_fin_query) + 1;

        agregar_a_paquete(paquete_fin, &len_motivo_fin_query, sizeof(uint32_t));
        agregar_a_paquete(paquete_fin, (void *)motivo_fin_query, len_motivo_fin_query);

        enviar_paquete(paquete_fin, query_afectada->fd_socket);
        eliminar_paquete(paquete_fin);

        pthread_mutex_lock(&mutex_exec);
        list_remove_element(querys_exec, query_afectada);
        pthread_mutex_unlock(&mutex_exec);

        enviar_a_exit(query_afectada);

        destruir_query_en_exit(query_afectada);
    }
    else
    {
        log_info(logger, "## Se desconecta el Worker %d - Se finaliza la Query %d - Cantidad total de Workers: %d",
			 worker->id_worker, (worker->query_en_ejecucion ? worker->query_en_ejecucion->id_query : -1), contador_workers);
    }
    destruir_worker(worker_encontrada);

    sem_post(&sem_evento_planificador);
}