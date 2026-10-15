"""RELAY: lee los eventos pendientes de la tabla outbox y los publica en Redis (Stream).
Después de publicar cada evento lo marca como procesado = TRUE.
Garantía: "al menos una vez" (si el relay se cae entre publicar y confirmar,
el evento se reenvía; por eso el consumidor debe ser idempotente).
"""
import json
import os
import time

import psycopg2
import redis

DB = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5433")),
    dbname=os.getenv("PG_DB", "sistemas_distribuidos"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", "postgres"),
)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM = "ordenes_eventos"
INTERVALO = 2  # segundos entre revisiones cuando no hay nada pendiente


def conectar_pg():
    for _ in range(30):
        try:
            return psycopg2.connect(**DB)
        except psycopg2.OperationalError:
            print("[RELAY] Esperando a Postgres...")
            time.sleep(2)
    raise SystemExit("No se pudo conectar a Postgres")


def main():
    conn = conectar_pg()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print(f"[RELAY] Iniciado. Revisando outbox cada {INTERVALO}s y publicando en el stream '{STREAM}'")

    while True:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, aggregate_id, tipo_evento, payload FROM outbox "
                    "WHERE procesado = FALSE ORDER BY id LIMIT 50 "
                    "FOR UPDATE SKIP LOCKED"
                )
                filas = cur.fetchall()
                for outbox_id, aggregate_id, tipo, payload in filas:
                    r.xadd(
                        STREAM,
                        {
                            "outbox_id": outbox_id,
                            "aggregate_id": aggregate_id,
                            "tipo_evento": tipo,
                            "payload": json.dumps(payload),
                        },
                    )
                    cur.execute("UPDATE outbox SET procesado = TRUE WHERE id = %s", (outbox_id,))
                    print(f"[RELAY] Evento outbox#{outbox_id} ({tipo}, orden {aggregate_id}) publicado en Redis")
        if not filas:
            time.sleep(INTERVALO)


if __name__ == "__main__":
    main()
