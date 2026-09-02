"""Agente local: descarga usuarios/marcadas TA040 y los envia al ERP por HTTPS."""

import argparse
import hashlib
import json
import os
import sys
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
    parser.add_argument("--watch", action="store_true", help="Mantener sincronizacion continua")
    parser.add_argument("--interval", type=int, default=5, help="Segundos entre lecturas en modo --watch")
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


def _sync_once(args, zk_class) -> int:
    zk = zk_class(
        args.device_ip,
        port=args.device_port,
        timeout=args.timeout,
        password=args.comm_key,
        force_udp=args.force_udp,
        ommit_ping=False,
    )
    conn = None
    try:
        conn = zk.connect()
        users = conn.get_users() or []
        punches = conn.get_attendance() or []
        serial_number = None
        try:
            serial_number = conn.get_serialnumber()
        except Exception:
            pass
    except Exception as exc:
        print(f"No fue posible leer el reloj {args.device_ip}:{args.device_port}: {exc}", file=sys.stderr)
        return 1
    finally:
        if conn is not None:
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
    payload_digest = hashlib.sha256(body).hexdigest()
    if args.watch and getattr(_sync_once, "last_payload_digest", None) == payload_digest:
        return 0
    endpoint = args.api_url.rstrip("/") + "/api/attendance/ingest"
    req = request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "X-Attendance-Token": args.token},
    )
    try:
        with request.urlopen(req, timeout=30) as response:
            print(response.read().decode("utf-8"))
            _sync_once.last_payload_digest = payload_digest
    except error.HTTPError as exc:
        print(f"ERP rechazo el lote ({exc.code}): {exc.read().decode('utf-8', 'replace')}", file=sys.stderr)
        return 1
    except error.URLError as exc:
        print(f"No fue posible conectar con el ERP: {exc}", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    args = _args()
    if not args.token:
        print("Falta ATTENDANCE_SYNC_TOKEN", file=sys.stderr)
        return 2
    if args.interval < 2:
        print("El intervalo minimo es 2 segundos", file=sys.stderr)
        return 2
    try:
        from zk import ZK
    except ImportError:
        print("Falta pyzk. Instale requirements.txt dentro de la venv.", file=sys.stderr)
        return 2

    if not args.watch:
        return _sync_once(args, ZK)

    print(f"Sincronizacion en caliente activa cada {args.interval} segundos. Ctrl+C para detener.")
    try:
        while True:
            _sync_once(args, ZK)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Sincronizacion detenida.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
