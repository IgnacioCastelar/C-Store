#ifndef HANDSHAKE_H
#define HANDSHAKE_H

#include "paquete.h"
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include<stdlib.h>
#include<stdio.h>
#include<signal.h>
#include<unistd.h>
#include<netdb.h>
#include<string.h>
#include<sys/socket.h>
#include<commons/log.h>
#include<commons/config.h>

t_paquete* crear_paquete_handshake(const char* modulo, const char* id);

#endif