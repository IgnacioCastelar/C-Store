# C-Store: Sistema de Almacenamiento Distribuido

![Status](https://img.shields.io/badge/STATUS-PRODUCTION_READY-green)
![Language](https://img.shields.io/badge/C-STD_C99-blue)
![Language](https://img.shields.io/badge/PYTHON-3.10-yellow)
![Platform](https://img.shields.io/badge/DOCKER-NATIVE-blue)

**C-Store** es un sistema de almacenamiento distribuido de alto rendimiento, diseñado bajo el paradigma de **Content Addressable Storage (CAS)**. Implementa una arquitectura modular capaz de ingestar, fragmentar y deduplicar archivos binarios en tiempo real.

> **Nota de Origen:** Este proyecto es una evolución profesional del trabajo académico "Master of Files" (Sistemas Operativos, UTN). Ha sido refactorizado para migrar de una simulación de planificación de CPU a un **Sistema de Archivos Distribuido** funcional y resiliente.

---

## Características Clave (Key Features)

* **Deduplicación de Datos (Block Level):** Utiliza hashing MD5 para identificar bloques repetidos. Si dos usuarios suben el mismo archivo (o archivos con partes idénticas), el sistema solo almacena una copia física, optimizando drásticamente el espacio en disco.
* **Arquitectura Distribuida:** Separación de responsabilidades en microservicios (**Gateway**, **Master**, **Workers**, **Storage**) orquestados nativamente con Docker Compose.
* **Protocolo Binario Custom:** Comunicación TCP de bajo nivel (Sockets) con serialización estricta (*Little Endian*) y control de flujo manual, eliminando el overhead de protocolos de texto como HTTP/JSON en el núcleo del sistema.
* **Resiliencia & Self-Healing:**
    * **Smart Retry:** Los nodos *Worker* detectan la indisponibilidad temporal del *Storage* (ej: durante formateo) y reintentan la conexión automáticamente con backoff lineal.
    * **Master Persistence:** El nodo coordinador (*Master*) persiste sus metadatos en disco, garantizando que la tabla de archivos sobreviva a reinicios o caídas del contenedor.
* **Infraestructura como Código (IaC):** Despliegue automatizado mediante `Makefile` y configuración inyectada por variables de entorno, sin archivos `.config` hardcodeados.

---

## Arquitectura

El sistema se compone de 4 módulos principales desplegados en contenedores:

```mermaid
graph TD
    User((Cliente)) -->|HTTP POST| Gateway[API Gateway (Python)]
    Gateway -->|TCP Stream| Worker[Worker Nodes (C)]
    Gateway -.->|Auth & Ubicación| Master[Master Node (C)]
    Worker -->|MD5 Check & Write| Storage[Storage Node (C)]
    Storage -->|Persistencia| Disk[(Disco Virtual)]

```

1. **API Gateway (Python/FastAPI):** Punto de entrada REST. Transforma peticiones HTTP de alto nivel en streams binarios TCP hacia la red interna.
2. **Master (C):** Actúa como "NameNode". Gestiona el índice de archivos (Namespace), autoriza subidas y balancea la carga entre Workers usando algoritmo *Round Robin*.
3. **Worker (C):** Actúa como "DataNode/Processor". Recibe streams de datos, calcula hashes MD5 al vuelo y gestiona la deduplicación transaccional con el Storage.
4. **Storage (C):** Actúa como "BlockDevice". Gestiona el sistema de archivos virtual (Bitmap, Superbloque) y la persistencia física de los bloques binarios (`.bin`).

---

## Puesta en Marcha (Quick Start)

### Prerrequisitos

* Docker & Docker Compose
* Make (Opcional, pero recomendado para automatización)

### 1. Despliegue (Modo Producción)

Este comando levanta todo el clúster, compila los binarios de C dentro de los contenedores y configura la red virtual.

```bash
make up

```

*Verificación: Espere a ver `Application startup complete` en los logs del Gateway.*

### 2. Uso (Subida de Archivos)

El sistema expone una API REST en el puerto `8000` del host.

```bash
# Ejemplo: Subir una imagen llamada 'foto.png'
curl -X POST -F "file=@foto.png" http://localhost:8000/api/upload

```

### 3. Monitoreo

Para ver los logs unificados de todos los nodos en tiempo real:

```bash
make logs

```

### 4. Reinicio y Limpieza (Deep Clean)

Para detener el sistema. Si desea borrar **todos** los datos (formateo profundo) y comenzar desde cero:

```bash
make reset

```

*(Nota: Esto ejecutará un "Deep Clean", destruyendo contenedores, volúmenes de Docker y eliminando físicamente el archivo de persistencia local `metadata.dat`).*

---

## Configuración (.env)

El comportamiento del clúster se controla centralizadamente desde el archivo `.env` en la raíz. No es necesario recompilar para cambiar estos valores.

| Variable | Default | Descripción |
| --- | --- | --- |
| `FRESH_START` | `TRUE` | `TRUE` formatea el disco virtual al iniciar. `FALSE` intenta recuperar datos previos. |
| `BLOCK_SIZE` | `64` | Tamaño del bloque (en bytes) para la deduplicación y almacenamiento. |
| `LOG_LEVEL` | `INFO` | Nivel de detalle de logs (`DEBUG` incluye trazas de paquetes y sockets). |
| `MASTER_PORT` | `4100` | Puerto TCP interno del nodo Master. |

---

## Estado del Proyecto (v1.0)

### Implementado ✅

* [x] Protocolo de comunicación binario propio (Handshake, Opcodes, Payload).
* [x] Pipeline de subida (*Upload*) completo con streaming.
* [x] Persistencia de metadatos en Master (Recuperación ante fallos).
* [x] Deduplicación optimista en Storage (Ahorro de espacio).
* [x] Suite de Tests de Estrés (`make test` para concurrencia).

### Próximos Pasos (Roadmap v1.1)

* [ ] **Download:** Reensamblado de archivos para descarga (`GET /api/download`). Actualmente el sistema funciona como *Cold Storage* (Ingesta y Archivado).
* [ ] **Garbage Collection:** Liberación asíncrona de bloques físicos cuando se borra un archivo lógico.

---

## Desviaciones de Diseño (Specs vs Realidad)

Si vienes de la especificación académica original, notarás diferencias arquitectónicas intencionales aplicadas durante la refactorización profesional:

1. **Eliminación del Planificador de CPU (PCB/Quantum):**
* *Razón:* C-Store es un sistema intensivo en I/O, no en CPU. El módulo "Master" actúa ahora como un orquestador de recursos de almacenamiento, no como un kernel de SO. Se eliminó la lógica de *Multilevel Feedback Queue* en favor de un enfoque orientado a eventos y *High Throughput*.


2. **Persistencia del Master:**
* *Razón:* Un sistema de archivos real no puede permitirse perder la ubicación de sus datos al reiniciarse. Se agregó persistencia en disco (`metadata.dat`), una característica vital para la integridad de datos que excede el alcance académico original.



---

## Autor

Ignacio Castelar Carballo