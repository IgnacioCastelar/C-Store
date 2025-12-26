#include "main_master.h"

// ... (Resto de variables globales igual)
t_log *logger;
t_config *config;
int contador_workers_libres = 0;
int contador_workers = 0;
int tiempo_aging;

// ... (liberar_y_salir igual)
void liberar_y_salir(int s)
{
	log_info(logger, "Cierre recibida (%d). Terminando Master...", s);
	sem_post(&sem_avanzar);
}

int main(int argc, char *argv[])
{
	if (argc != 2)
	{
		fprintf(stderr, "Uso: %s <archivo_config>\n", argv[0]);
		return EXIT_FAILURE;
	}

	char ruta_config[256];
	char *nombre_config = argv[1];

	logger = log_create("master.log", "master", 1, LOG_LEVEL_INFO);

	// FIX T-003/T-004: Compatibilidad Docker/Local
	// Si el argumento ya tiene ruta (ej: config/file.config), usarlo directo.
    // Si no, buscar en ./config/
	if (strstr(nombre_config, "/") != NULL) {
		snprintf(ruta_config, sizeof(ruta_config), "%s", nombre_config);
	} else {
		snprintf(ruta_config, sizeof(ruta_config), "./config/%s", nombre_config);
	}

	config = config_create(ruta_config);

	if (config == NULL) {
		log_error(logger, "No se pudo encontrar el archivo de configuracion: %s", ruta_config);
		log_destroy(logger); // Evitar leak si falla inicio
		return EXIT_FAILURE;
	}

    // ... (El resto del main sigue igual hasta el final)
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
	pthread_t hilo_cliente;
	while (1)
	{
		int socket_cliente = esperar_cliente(socketServerDeHilo);
		t_paquete *paquete_handshake = recibir_paquete(socket_cliente);

		if (paquete_handshake == NULL) {
			log_error(logger, "Error al recibir el handshake.");
			close(socket_cliente);
			continue;
		}

		switch (paquete_handshake->codigo_operacion)
		{
		case HANDSHAKE_WORKER:
			log_info(logger, "Se conectó un Worker (Handshake recibido)");
			t_worker_conectada *worker = malloc(sizeof(t_worker_conectada));
			// FIXME: Protocolo real pendiente
			worker->id_worker = contador_workers + 1; 
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

			log_info(logger, "## Se conecta el Worker %d", worker->id_worker);
			pthread_create(&hilo_cliente, NULL, atender_worker, worker);
			pthread_detach(hilo_cliente);
			destruir_paquete(paquete_handshake);
			break;

		case HANDSHAKE_QUERY:
			log_info(logger, "Se conectó un Query Control");
			destruir_paquete(paquete_handshake);
			break;

		default:
			log_error(logger, "OpCode desconocido: %d", paquete_handshake->codigo_operacion);
			destruir_paquete(paquete_handshake);
			break;
		}
	}
	return NULL;
}

void *atender_querys(void *query) { return NULL; }
void *atender_worker(void *args) {
	t_worker_conectada *worker = (t_worker_conectada *)args;
	while (1) {
		t_paquete *paquete = recibir_paquete(worker->socket_cliente);
		if (!paquete) {
			desconexion_worker(worker);
			break;
		} else {
			destruir_paquete(paquete);
		}
	}
	return NULL;
}
void mostrar_lista_workers(t_list *lista) {}
void mostrar_lista_querys(t_list *lista) {}
void liberar_worker(int idQ) {}