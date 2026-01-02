from fastapi import FastAPI, UploadFile, HTTPException, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import hashlib
import struct
import os
import sys 
from cstore_client import CStoreClient, OP_UPLOAD_REQ, OP_UPLOAD_ACK, OP_STREAM_DATA, OP_STREAM_FINISH, OP_ERROR, OP_OK

app = FastAPI(title="C-Store Gateway", version="1.0")

# Permitir CORS (útil si el frontend estuviera en otro puerto, aunque aquí servimos todo junto)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MASTER_HOST = os.getenv("MASTER_HOST", "cstore-master")
MASTER_PORT = int(os.getenv("MASTER_PORT", 4100))

# Log helper para forzar salida inmediata (bypass buffer)
def log(msg):
    print(msg, flush=True)

# --- ENDPOINTS DE LA API ---

# [CORRECCION AGENTE 5] SIN 'async'. 
# Al ser una funcion normal, FastAPI la ejecuta en un ThreadPool separado.
@app.post("/api/upload")
def upload_file(file: UploadFile = File(...)):
    log(f"[Gateway] Recibida solicitud de subida: {file.filename}")
    
    # --- PASO 1: HANDSHAKE CON MASTER ---
    master = CStoreClient()
    try:
        master.connect(MASTER_HOST, MASTER_PORT)
        
        # Preparar Payload OP_UPLOAD_REQ: [LenName (4)][Name][Size (4)]
        filename_bytes = file.filename.encode('utf-8')
        
        # Lectura Síncrona del tamaño
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)

        # Empaquetamos: Size nombre (4) + Nombre (N) + Size archivo (4)
        payload = struct.pack('<I', len(filename_bytes)) + filename_bytes + struct.pack('<I', file_size)
        master.send_packet(OP_UPLOAD_REQ, payload)

        # Esperar respuesta del Master
        opcode, response = master.recv_packet()
        master.disconnect()

        if opcode == OP_ERROR:
            raise HTTPException(status_code=400, detail="Master rechazo la subida (¿Archivo duplicado o sistema lleno?)")
        
        if opcode != OP_UPLOAD_ACK:
            raise HTTPException(status_code=500, detail=f"Respuesta inesperada del Master: OpCode {opcode}")

        # Deserializar OP_UPLOAD_ACK: [LenIP (4)][IP (N)][Port (4)][Ticket (4)]
        # Nota: El ticket lo ignoramos por ahora si no se usa en el handshake worker
        offset = 0
        
        len_ip = struct.unpack('<I', response[offset:offset+4])[0]
        offset += 4

        worker_host = response[offset:offset+len_ip].decode('utf-8') # Ojo: Hostname de Docker (ej: worker-1)
        offset += len_ip

        worker_port = struct.unpack('<I', response[offset:offset+4])[0]
        
        log(f"[Gateway] Master asignó Worker: {worker_host}:{worker_port}")

    except Exception as e:
        log(f"[Gateway] Error Master: {e}")
        return JSONResponse(status_code=500, content={"error": f"Fallo al contactar Master: {str(e)}"})

    # --- PASO 2: STREAMING AL WORKER ---
    worker = CStoreClient()
    try:
        # Conectamos al worker asignado
        worker.connect(worker_host, worker_port)
        
        md5 = hashlib.md5()
        chunk_size = 1024 * 1024 # 1MB chunks

        while True:
            # [CORRECCION AGENTE 5] Lectura Síncrona (SIN await)
            chunk = file.file.read(chunk_size)
            if not chunk:
                break
            
            # Enviar chunk crudo: [OP_STREAM_DATA][Size][Bytes]
            worker.send_packet(OP_STREAM_DATA, chunk)
            md5.update(chunk)
        
        # Enviar Hash Final (MD5 Hex Digest)
        # Se asume que el Worker espera el string hexadecimal (32 chars)
        md5_digest = md5.hexdigest().encode('utf-8')
        worker.send_packet(OP_STREAM_FINISH, md5_digest)

        # Esperar confirmación final del Worker (OP_OK o OP_ERROR)
        opcode, _ = worker.recv_packet()
        worker.disconnect()

        if opcode == OP_OK:
            log(f"[Gateway] EXITO: {file.filename} subido a {worker_host}")
            return {"status": "success", "file": file.filename, "node": worker_host}
        else:
            raise HTTPException(status_code=500, detail="El Worker reportó error al finalizar la escritura")

    except Exception as e:
        log(f"[Gateway] Error con Worker: {e}")
        return JSONResponse(status_code=500, content={"error": f"Fallo Worker: {str(e)}"})

# --- SERVIDOR DE ARCHIVOS ESTÁTICOS (FRONTEND) ---
# Esto debe ir AL FINAL para no bloquear las rutas de la API.
# 'html=True' significa que si vas a la raíz "/", buscará "index.html" automáticamente.
app.mount("/", StaticFiles(directory="static", html=True), name="static")