#include "main_master.h"
#include <commons/string.h>
#include <commons/collections/dictionary.h>

// Estructura para persistencia básica (Roadmap T-009)
t_dictionary *indice_archivos;
t_log *logger;
t_config *config;
int contador_workers_libres = 0;
int contador_workers = 0;
int tiempo_aging;
int worker_rr_index = 0;

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

	indice_archivos = dictionary_create();
	// TODO: Cargar metadata.dat si existe (Persistencia)

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

	int socket_server = iniciar_servidor(puerto, logger); // Pasamos la variable global logger del Master
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
		int socket_cliente = esperar_cliente(socketServerDeHilo, logger);
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

		// Caso: OP_UPLOAD_REQ (0x10)
		case OP_UPLOAD_REQ: {
		    log_info(logger, "Petición de subida recibida del Gateway (Socket %d)", socket_cliente);

		    // 1. Deserializar Nombre y Tamaño
		    int despl = 0;
		    char* nombre_archivo = deserializar_string(paquete_handshake->buffer->stream, &despl);
		    uint32_t tam_archivo;
		    memcpy(&tam_archivo, paquete_handshake->buffer->stream + despl, sizeof(uint32_t));

		    // 2. Verificar duplicados (Idempotencia básica en memoria)
		    // Nota: Asumimos que 'indice_archivos' fue inicializado en el main.
		    if (dictionary_has_key(indice_archivos, nombre_archivo)) {
		        log_error(logger, "Rechazando subida: El archivo '%s' ya existe.", nombre_archivo);
		        // Enviar Error 101: ERR_FILE_EXISTS
		        t_paquete* err = crear_paquete(OP_ERROR);
		        // Podríamos agregar código de error en el payload si el protocolo lo define
		        enviar_paquete(err, socket_cliente);
		        destruir_paquete(err);
		    } 
		    else {
		        // 3. Asignación de Worker (Algoritmo Round Robin)
        		pthread_mutex_lock(&mutex_worker);
        
		        int cantidad_workers = list_size(workers_conectados);
        
        		if (cantidad_workers == 0) {
		            log_error(logger, "Error crítico: No hay Workers conectados para atender la subida.");
		            // Enviar Error 201: ERR_NO_WORKERS
        		    pthread_mutex_unlock(&mutex_worker);
            		t_paquete* err = crear_paquete(OP_ERROR); 
            		enviar_paquete(err, socket_cliente);
            		destruir_paquete(err);
        		} else {
            		// Aritmética Modular para Round Robin
            		worker_rr_index = worker_rr_index % cantidad_workers;
            
		            t_worker_conectada* worker_elegido = list_get(workers_conectados, worker_rr_index);
            
        		    // Avanzamos el índice para la próxima petición
            		worker_rr_index++;
            
            		pthread_mutex_unlock(&mutex_worker);

            		log_info(logger, "Asignando Worker ID: %d para subir '%s' (Round Robin)", 
                    		 worker_elegido->id_worker, nombre_archivo);

            		// 4. Responder al Gateway (OP_UPLOAD_ACK)
            		// Payload: [IP/Hostname Worker (String)] [Puerto Datos (Int)]
            		t_paquete* ack = crear_paquete(OP_UPLOAD_ACK);
            
            		// FIXME: Asegúrate de que tu struct t_worker_conectada tenga el campo 'hostname' o 'ip' 
            		// guardado desde el handshake. Si no, usa el ID o una lógica de mapeo.
            		// Por ahora, asumimos que worker-X resuelve por DNS en Docker.
            		char* ip_worker_str = string_from_format("worker-%d", worker_elegido->id_worker); 
            		int puerto_datos = 5000; // Según config T-007

		            int len_ip = strlen(ip_worker_str) + 1;
        		    agregar_a_paquete(ack, &len_ip, sizeof(int));
            		agregar_a_paquete(ack, ip_worker_str, len_ip);
            		agregar_a_paquete(ack, &puerto_datos, sizeof(int));

            		enviar_paquete(ack, socket_cliente);
            		destruir_paquete(ack);
            		free(ip_worker_str);
            
            		// Reservar nombre en índice (Estado: SUBIENDO)
            		dictionary_put(indice_archivos, nombre_archivo, "SUBIENDO");
        		}
    		}

    		free(nombre_archivo);
    		destruir_paquete(paquete_handshake);
    		close(socket_cliente); // Gateway es conexión corta aquí
    		break;
		}

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