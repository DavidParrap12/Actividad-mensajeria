"""PRODUCTOR: crea órdenes y, en la MISMA transacción, el evento en la tabla outbox.
Uso:
    python productor.py        -> crea 3 órdenes
    python productor.py 10     -> crea 10 órdenes
"""
import json
import os
import random
import sys
import time

import psycopg2

DB = dict(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5433")),
    dbname=os.getenv("PG_DB", "sistemas_distribuidos"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", "postgres"),
)

CLIENTES = ["Juan", "María", "Carlos", "Laura", "Andrés", "Camila"]
PRODUCTOS = [("Laptop", 2500000), ("Mouse", 45000), ("Teclado", 120000), ("Monitor", 800000)]


def conectar():
    for _ in range(20):
        try:
            return psycopg2.connect(**DB)
        except psycopg2.OperationalError:
            print("Esperando a Postgres...")
            time.sleep(2)
    raise SystemExit("No se pudo conectar a Postgres")


def crear_orden(conn):
    cliente = random.choice(CLIENTES)
    producto, precio = random.choice(PRODUCTOS)
    cantidad = random.randint(1, 3)
    total = precio * cantidad

    # "with conn" = una sola transacción: o se guardan ambas filas o ninguna
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ordenes (cliente, producto, cantidad, total) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (cliente, producto, cantidad, total),
            )
            orden_id = cur.fetchone()[0]
            payload = {
                "orden_id": orden_id,
                "cliente": cliente,
                "producto": producto,
                "cantidad": cantidad,
                "total": total,
            }
            cur.execute(
                "INSERT INTO outbox (aggregate_id, tipo_evento, payload) "
                "VALUES (%s, %s, %s::jsonb)",
                (orden_id, "OrdenCreada", json.dumps(payload)),
            )
    return payload


def main():
    cantidad = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    conn = conectar()
    for _ in range(cantidad):
        p = crear_orden(conn)
        print(f"[PRODUCTOR] Orden #{p['orden_id']} creada: {p['cliente']} compró {p['cantidad']} x {p['producto']} (${p['total']})")
        time.sleep(0.5)
    conn.close()


if __name__ == "__main__":
    main()
