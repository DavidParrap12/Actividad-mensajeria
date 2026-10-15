"""CONSUMIDOR: lee los eventos del stream de Redis y los procesa.

Usa un consumer group (los mensajes se reparten entre consumidores) y XACK
para confirmar. Es idempotente: si llega un evento repetido, lo ignora.
"""
import json
import os
import socket

import redis
from redis.exceptions import ResponseError

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM = "ordenes_eventos"
GRUPO = "grupo_ordenes"
CONSUMIDOR = os.getenv("HOSTNAME", socket.gethostname())
VISTOS = "outbox_ids_procesados"  # set para evitar procesar duplicados


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        r.xgroup_create(STREAM, GRUPO, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    print(f"[CONSUMIDOR] {CONSUMIDOR} escuchando el stream '{STREAM}' (grupo '{GRUPO}')...")
    while True:
        respuesta = r.xreadgroup(GRUPO, CONSUMIDOR, {STREAM: ">"}, count=10, block=5000)
        for _, mensajes in respuesta:
            for msg_id, campos in mensajes:
                if r.sadd(VISTOS, campos["outbox_id"]) == 0:
                    print(f"[CONSUMIDOR] Duplicado outbox#{campos['outbox_id']} ignorado")
                else:
                    p = json.loads(campos["payload"])
                    print(
                        f"[CONSUMIDOR] {campos['tipo_evento']} -> Orden #{p['orden_id']}: "
                        f"{p['cliente']} compró {p['cantidad']} x {p['producto']} (${p['total']})"
                    )
                r.xack(STREAM, GRUPO, msg_id)


if __name__ == "__main__":
    main()
