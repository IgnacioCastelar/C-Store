#include "handshake.h"

t_paquete* crear_paquete_handshake(const char* modulo, const char* id) {
    t_paquete* paquete = crear_paquete(HANDSHAKE_WORKER);

    int modulo_len = strlen(modulo) + 1;
    int id_len  = strlen(id) + 1;

    agregar_a_paquete(paquete, &modulo_len, sizeof(int));
    agregar_a_paquete(paquete, (void*)modulo, modulo_len);

    agregar_a_paquete(paquete, &id_len, sizeof(int));
    agregar_a_paquete(paquete, (void*)id, id_len);

    return paquete;
}