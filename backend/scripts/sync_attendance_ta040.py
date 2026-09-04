"""Agente local: descarga usuarios/marcadas TA040 y los envia al ERP por HTTPS."""

import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime
from urllib import error, request

from dotenv import load_dotenv


load_dotenv()


def _args():
    parser = argparse.ArgumentParser(description="Sincroniza un reloj 3nStar TA040 con el ERP")
    parser.add_argument("--device-ip", default=os.getenv("ATTENDANCE_DEVICE_IP", "192.168.1.132"))
    parser.add_argument("--device-port", type=int, default=int(os.getenv("ATTENDANCE_DEVICE_PORT", "4370")))
    parser.add_argument("--comm-key", type=int, default=int(os.getenv("ATTENDANCE_DEVICE_COMM_KEY", "0")))
    parser.add_argument("--device-code", default=os.getenv("ATTENDANCE_DEVICE_CODE", "ta040-central"))
    parser.add_argument("--api-url", default=os.getenv("ATTENDANCE_API_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--token", default=os.getenv("ATTENDANCE_SYNC_TOKEN", ""))
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--force-udp", action="store_true")
    parser.add_argument("--verify-ping", action="store_true", help="Exigir ping ICMP antes de conectar")
    parser.add_argument("--watch", action="store_true", help="Mantener sincronizacion continua")
    parser.add_argument("--interval", type=int, default=3, help="Segundos entre lecturas en modo --watch")
    parser.add_argument("--users-interval", type=int, default=300, help="Segundos entre actualizaciones de usuarios")
    parser.add_argument("--reconnect-interval", type=int, default=180, help="Segundos antes de renovar la conexion al reloj")
    parser.add_argument("--watchdog-timeout", type=int, default=45, help="Reiniciar el agente si una sincronizacion se bloquea")
    return parser.parse_args()


def _serialize_user(user):
    return {
        "user_id": str(user.user_id),
        "uid": user.uid,
        "name": user.name or None,
        "card_number": str(user.card) if getattr(user, "card", None) else None,
    }


def _serialize_punch(item):
    timestamp = item.timestamp
    if not isinstance(timestamp, datetime):
        timestamp = datetime.fromisoformat(str(timestamp))
    return {
        "user_id": str(item.user_id),
        "occurred_at": timestamp.isoformat(timespec="seconds"),
        "punch_state": getattr(item, "punch", None),
        "verify_mode": getattr(item, "status", None),
        "work_code": str(item.workcode) if getattr(item, "workcode", None) else None,
    }


def _new_connection(args, zk_class):
    return zk_class(
        args.device_ip,
        port=args.device_port,
        timeout=args.timeout,
        password=args.comm_key,
        force_udp=args.force_udp,
        # El puerto ZK es la prueba real. Algunos TA040 dejan de responder ICMP
        # temporalmente aunque el servicio 4370 siga disponible.
        ommit_ping=not args.verify_ping,
    ).connect()


def _sync_once(args, zk_class, connection=None, include_users=True) -> int:
    conn = connection
    owns_connection = connection is None
    try:
        if conn is None:
            conn = _new_connection(args, zk_class)
        users = (conn.get_users() or []) if include_users else []
        punches = conn.get_attendance() or []
        serial_number = None
        if include_users:
            try:
                serial_number = conn.get_serialnumber()
            except Exception:
                pass
    except Exception as exc:
        print(f"No fue posible leer el reloj {args.device_ip}:{args.device_port}: {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        if owns_connection and conn is not None:
            try:
                conn.disconnect()
            except Exception:
                pass

    payload = {
        "device_code": args.device_code,
        "device_name": "Reloj TA040 Central",
        "model": "3nStar TA040",
        "serial_number": serial_number,
        "local_ip": args.device_ip,
        "local_port": args.device_port,
        "users": [_serialize_user(user) for user in users],
        "punches": [_serialize_punch(item) for item in punches],
    }
    body = json.dumps(payload).encode("utf-8")
    endpoint = args.api_url.rstrip("/") + "/api/attendance/ingest"
    req = request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "X-Attendance-Token": args.token},
    )
    try:
        with request.urlopen(req, timeout=30) as response:
            print(response.read().decode("utf-8"), flush=True)
    except error.HTTPError as exc:
        print(f"ERP rechazo el lote ({exc.code}): {exc.read().decode('utf-8', 'replace')}", file=sys.stderr, flush=True)
        return 1
    except error.URLError as exc:
        print(f"No fue posible conectar con el ERP: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


def _sync_with_watchdog(args, zk_class, connection, include_users) -> int:
    """Termina el proceso si una libreria deja una operacion de red colgada."""
    outcome = {}

    def run():
        try:
            outcome["result"] = _sync_once(
                args,
                zk_class,
                connection=connection,
                include_users=include_users,
            )
        except BaseException as exc:  # La excepcion se propaga en el hilo principal.
            outcome["error"] = exc

    worker = threading.Thread(target=run, name="ta040-sync", daemon=True)
    worker.start()
    worker.join(max(15, args.watchdog_timeout))
    if worker.is_alive():
        print(
            f"Sincronizacion bloqueada por mas de {args.watchdog_timeout}s; reiniciando agente.",
            file=sys.stderr,
            flush=True,
        )
        os._exit(3)
    if "error" in outcome:
        raise outcome["error"]
    return int(outcome.get("result", 1))


def main() -> int:
    args = _args()
    if not args.token:
        print("Falta ATTENDANCE_SYNC_TOKEN", file=sys.stderr, flush=True)
        return 2
    if args.interval < 2:
        print("El intervalo minimo es 2 segundos", file=sys.stderr, flush=True)
        return 2
    try:
        from zk import ZK
    except ImportError:
        print("Falta pyzk. Instale requirements.txt dentro de la venv.", file=sys.stderr, flush=True)
        return 2

    if not args.watch:
        return _sync_once(args, ZK)

    print(f"Sincronizacion en caliente activa cada {args.interval} segundos. Ctrl+C para detener.", flush=True)
    conn = None
    connected_at = 0.0
    last_users_sync = 0.0
    try:
        while True:
            try:
                now = time.monotonic()
                if conn is not None and now - connected_at >= max(60, args.reconnect_interval):
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                    conn = None
                if conn is None:
                    conn = _new_connection(args, ZK)
                    connected_at = time.monotonic()
                    last_users_sync = 0.0
                    print(f"Conexion establecida con {args.device_ip}:{args.device_port}.", flush=True)
                include_users = time.monotonic() - last_users_sync >= max(60, args.users_interval)
                result = _sync_with_watchdog(args, ZK, conn, include_users)
                if not result and include_users:
                    last_users_sync = time.monotonic()
                if result:
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                    conn = None
            except Exception as exc:
                print(f"Conexion con el reloj interrumpida: {exc}. Reintentando...", file=sys.stderr, flush=True)
                if conn is not None:
                    try:
                        conn.disconnect()
                    except Exception:
                        pass
                conn = None
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Sincronizacion detenida.", flush=True)
        return 0
    finally:
        if conn is not None:
            try:
                conn.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
