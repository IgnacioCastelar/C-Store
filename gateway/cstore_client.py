import socket
import struct

# Definición de OpCodes según paquete.h
OP_ERROR = 0x00
OP_UPLOAD_REQ = 0x10
OP_UPLOAD_ACK = 0x11
OP_STREAM_DATA = 0x20
OP_STREAM_FINISH = 0x21
OP_OK = 0x02

class CStoreClient:
    def __init__(self):
        self.sock = None

    def connect(self, host, port):
        """Conecta al socket TCP del módulo destino."""
        print(f"[Gateway] Conectando a {host}:{port}...")
        self.sock = socket.create_connection((host, int(port)))

    def disconnect(self):
        if self.sock:
            self.sock.close()

    def _recv_exact(self, n):
        """Lee exactamente n bytes del socket."""
        data = b''
        while len(data) < n:
            packet = self.sock.recv(n - len(data))
            if not packet:
                raise ConnectionError("Socket cerrado remotamente")
            data += packet
        return data

    def send_packet(self, opcode, payload=b''):
        """
        Serializa y envía un paquete: [OP_CODE (1B)] + [SIZE (4B)] + [PAYLOAD]
        """
        # Estándar Little Endian (<), OpCode unsigned char (B), Size unsigned int (I)
        header = struct.pack('<BI', opcode, len(payload))
        self.sock.sendall(header + payload)

    def recv_packet(self):
        """
        Recibe y deserializa un paquete. Retorna (opcode, payload).
        """
        # 1. Leer OpCode (1 Byte)
        op_byte = self._recv_exact(1)
        opcode = struct.unpack('<B', op_byte)[0]

        # 2. Leer Size (4 Bytes)
        size_bytes = self._recv_exact(4)
        size = struct.unpack('<I', size_bytes)[0]

        # 3. Leer Payload
        payload = b''
        if size > 0:
            payload = self._recv_exact(size)

        return opcode, payload