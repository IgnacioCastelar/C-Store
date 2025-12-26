#include "main_master.h"

t_log *logger;
t_config *config;

int contador_workers_libres = 0;
int contador_workers = 0;
int tiempo_aging;

void liberar_y_salir(int s)
{
	log_info(logger, "Cierre recibida (%d). Terminando Master...", s);
	sem_post(&sem_avanzar);
}

int main(int argc, char *argv[])
{
	if (argc != 2)
	{
		fprintf(stderr, "Falta algun argumento.\n");
		return EXIT_FAILURE;
	}

	char ruta_config[100];
	char *path_config = argv[1];

	logger = log_create("/home/utnso/so-deploy/tp-2025-2c-NootNoot/master/logger_master.log", "master", 1, LOG_LEVEL_INFO);

	sprintf(ruta_config, "../config/%s.config", path_config);
	config = config_create(ruta_config);

	signal(SIGINT, liberar_y_salir);

	int puerto = config_get_int_value(config, "PUERTO_ESCUCHA");

	tiempo_aging = config_get_int_value(config, "TIEMPO_AGING");

	if (tiempo_aging == 0)
	{
		log_info(logger, "Planificador sin Aging (TIEMPO_AGING=0)");
	}

	int socket_server = iniciar_servidor(puerto);
	int *socketParaHilo = malloc(sizeof(int));
	*socketParaHilo = socket_server;

	log_info(logger, "[MAIN_MASTER] Iniciando estructuras necesarias...");

	inicializar_estructuras_master();

	log_info(logger, "[MAIN_MASTER] Iniciando conexiones...");

	pthread_t conexiones;
	pthread_create(&conexiones, NULL, iniciar_conexiones, socketParaHilo);
	pthread_detach(conexiones);

	log_info(logger, "[MAIN_MASTER] Iniciando planificador...");
	iniciar_planificador_corto_plazo();

	sem_post(&sem_inicio_planificacion);

	log_info(logger, "Master iniciado correctamente.");
	sem_wait(&sem_avanzar);

	log_info(logger, "Liberando recursos del Master...");

	destruir_estructuras_master();

	config_destroy(config);
	log_destroy(logger);
	free(socketParaHilo);

	printf("[MAIN_MASTER] Master finalizado con exito.\n");

	return EXIT_SUCCESS;
}

void *iniciar_conexiones(void *socket_server)
{

	int socketServerDeHilo = *(int *)socket_server;

	// int result_ok = 0;
	// int result_error = -1;

	pthread_t hilo_cliente;
	while (1)
	{
		int socket_cliente = esperar_cliente(socketServerDeHilo);

		t_paquete *paquete_handshake = recibir_paquete_completo(socket_cliente);

		if (paquete_handshake == NULL)
		{
			log_error(logger, "Error al recibir el handshake.");
			// send(socket_cliente, &result_error, sizeof(int), 0);
		}

		switch (paquete_handshake->codigo_operacion)
		{
		case HANDSHAKE_WORKER:
			t_handshakeWorker *paquete_handshake_worker = recibir_paquete_handshake_worker(paquete_handshake);
			log_info(logger, "El tipo de cliente recibido es: %s", paquete_handshake_worker->modulo);

			t_worker_conectada *worker = malloc(sizeof(t_worker_conectada));
			worker->id_worker = atoi(paquete_handshake_worker->id_worker);
			worker->libre = true;
			worker->cerrada = false;
			worker->socket_cliente = socket_cliente;
			worker->query_en_ejecucion = NULL;

			sem_init(&worker->sem_desalojo, 0, 0);

			// bloqueo para que otros hilos no afecten la lista global
			pthread_mutex_lock(&mutex_worker);
			list_add(workers_conectados, worker);
			contador_workers_libres++;
			contador_workers++;
			pthread_mutex_unlock(&mutex_worker);

			// Despierta al planificador
			sem_post(&sem_hay_workers);
			sem_post(&sem_evento_planificador);

			log_info(logger, "## Se conecta el Worker %d - Cantidad total de Workers: %d", worker->id_worker, contador_workers);

			pthread_create(&hilo_cliente, NULL, atender_worker, worker);
			pthread_detach(hilo_cliente);
			log_info(logger, "Se crea el hilo para atender workers");

			free(paquete_handshake_worker->modulo);
			free(paquete_handshake_worker->id_worker);
			free(paquete_handshake_worker);

			eliminar_paquete(paquete_handshake);
			break;
		case HANDSHAKE_QUERY:
			t_handshakeQuery *paquete_handshake_query = recibir_paquete_handshake_query(paquete_handshake);
			// log_info(logger, "El tipo de cliente recibido es: %s", paquete_handshake_query->modulo);
			// log_info(logger, "El nombre del archivo recibido es: %s", paquete_handshake_query->nombre_query);
			// log_info(logger, "La prioridad recibida de la query %s es %d", paquete_handshake_query->modulo, paquete_handshake_query->prioridad);

			t_propQuery *query = crear_query_para_ready(paquete_handshake_query->nombre_query, paquete_handshake_query->prioridad, socket_cliente);
			// log_info(logger, "Query -> Nombre: %s | id_Query: %d | Estado actual: %s | Prioridad: %d", query->nombre_query, query->id_query, estado_a_string(query->estado_actual), query->prioridad);

			t_propQuery *query_para_hilo = malloc(sizeof(t_propQuery));
			*query_para_hilo = *query;

			log_info(logger, "## Se conecta un Query Control para ejecutar la Query %s con prioridad %d - Id asignado: %d. Nivel multiprocesamiento %d",
					 query->nombre_query, query->prioridad, query->id_query, contador_workers);

			pthread_mutex_lock(&mutex_ready);
			list_add(cola_querys_ready, query);
			pthread_mutex_unlock(&mutex_ready);

			sem_post(&sem_hay_querys);
			sem_post(&sem_evento_planificador);

			pthread_create(&hilo_cliente, NULL, atender_querys, query_para_hilo);
			pthread_detach(hilo_cliente);

			free(paquete_handshake_query->modulo);
			free(paquete_handshake_query);

			eliminar_paquete(paquete_handshake);
			break;
		default:
			log_error(logger, "Codigo de operacion desconocido...");
			eliminar_paquete(paquete_handshake);
			break;
		}
	}
	return NULL;
}

void *atender_querys(void *query)
{
	t_propQuery *query_de_hilo = (t_propQuery *)query;
	int query_id = query_de_hilo->id_query;
	int query_socket = query_de_hilo->fd_socket;

	free(query_de_hilo);

	log_info(logger, "Se crea el hilo para la query con id: %d", query_id);
	char buffer[64];

	while (1)
	{
		int bytes_recibidos = recv(query_socket, buffer, sizeof(buffer), MSG_WAITALL);

		if (bytes_recibidos == 0)
		{
			log_info(logger, "El Query Control (ID: %d) cerró la conexión voluntariamente.", query_id);
			break; 
		}
		else if (bytes_recibidos < 0)
		{
			log_error(logger, "Error en conexión con Query Control (ID: %d). Cerrando...", query_id);
			break; 
		}
		else
		{
			log_warning(logger, "Query Control (ID: %d) envió %d bytes inesperados. Ignorando...", query_id, bytes_recibidos);
		}
	}

	t_propQuery *query_cancelada = buscar_y_remover_query(query_id);

	if (query_cancelada == NULL)
	{
		log_debug(logger, "Hilo Query %d: Socket cerrado, query no encontrada (probablemente ya finalizó).", query_id);
		return NULL;
	}

	log_info(logger, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
				 query_cancelada->id_query,
				 //estado_a_string(query_cancelada->estado_actual),
				 query_cancelada->prioridad,
				 contador_workers);

	t_estado_query estado_anterior = query_cancelada->estado_actual;

	switch (estado_anterior)
	{
	case READY:
		if (query_cancelada->temporizador != NULL)
		{
			temporal_destroy(query_cancelada->temporizador);
			query_cancelada->temporizador = NULL;
		}
		break;
	case EXEC:

		liberar_worker(query_cancelada->id_query);
		break;
	case EXIT:
		break;
	}

	enviar_a_exit(query_cancelada);
	close(query_cancelada->fd_socket);

	destruir_query_en_exit(query_cancelada);
	log_info(logger, "Recursos de Query %d liberados correctamente.", query_id);

	return NULL;
}

void *atender_worker(void *args)
{
	t_worker_conectada *worker = (t_worker_conectada *)args;
	log_info(logger, "Hilo atendiendo al Worker %d en Socket %d", worker->id_worker, worker->socket_cliente);

	while (1)
	{
		t_paquete *paquete = recibir_paquete_completo(worker->socket_cliente);

		if (!paquete)
		{
			desconexion_worker(worker);
			break;
		}
		else
		{
			paquete_worker(worker, paquete);
			eliminar_paquete(paquete);
		}
	}

	//close(worker->socket_cliente);
	//free(worker);
	return NULL;
}

void mostrar_lista_workers(t_list *lista)
{
	pthread_mutex_lock(&mutex_worker);
	for (int i = 0; i < list_size(lista); i++)
	{
		t_worker_conectada *worker = list_get(lista, i);
		log_info(logger, "Worker[%d] -> ID: %d | Libre: %d | Cerrada: %d | Socket: %d",
				 i, worker->id_worker, worker->libre, worker->cerrada, worker->socket_cliente);
	}
	pthread_mutex_unlock(&mutex_worker);
}

void mostrar_lista_querys(t_list *lista)
{
	pthread_mutex_lock(&mutex_ready);
	for (int i = 0; i < list_size(lista); i++)
	{
		t_propQuery *query = list_get(lista, i);
		log_info(logger, "Query[%d] -> ID: %d | nombre_query: %s | Prioridad: %d | Socket: %d", i, query->id_query, query->nombre_query,
				 query->prioridad, query->fd_socket);
	}
	pthread_mutex_unlock(&mutex_ready);
}

void liberar_worker(int idQ)
{
	pthread_mutex_lock(&mutex_worker);
	t_worker_conectada *worker_encontrada = encontrar_worker_con_query(workers_conectados, idQ);
	if (worker_encontrada)
	{
		log_info(logger, "## Se desaloja la Query %d (%d) del Worker %d Motivo: %s",
				 worker_encontrada->query_en_ejecucion->id_query,
				 worker_encontrada->query_en_ejecucion->prioridad,
				 worker_encontrada->id_worker,
				 motivo_to_string(DESCONEXION));

		// log_info(logger, "Se encontro el worker a liberar con id: %d", worker_encontrada->id_worker);

		pedido_desalojo(worker_encontrada->socket_cliente);
		pthread_mutex_unlock(&mutex_worker);

		// log_info(logger, "Esperando confirmación de desalojo del Worker %d...", worker_encontrada->id_worker);
		sem_wait(&worker_encontrada->sem_desalojo);

		pthread_mutex_lock(&mutex_worker);

		// log_info(logger, "Worker %d confirmado detenido. Liberando recursos.", worker_encontrada->id_worker);

		worker_encontrada->libre = true;
		worker_encontrada->query_en_ejecucion = NULL;
		contador_workers_libres++;

		sem_post(&sem_hay_workers);
		sem_post(&sem_evento_planificador);

		pthread_mutex_unlock(&mutex_worker);
	}
	else
	{
		pthread_mutex_unlock(&mutex_worker);
	}
}