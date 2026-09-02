import asyncio
import hashlib
import hmac
import json
import os
import re
import threading
import unicodedata
from datetime import date, datetime, time, timedelta
from typing import List, Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, Form, Header, HTTPException, Query, Request, WebSocket, status
from fastapi.responses import RedirectResponse
from jose import JWTError, jwt
from pydantic import BaseModel, Field
from sqlalchemy import and_, or_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..config import settings
from ..core.deps import get_db
from ..core.security import ALGORITHM, SECRET_KEY
from ..database import get_session_local
from ..models.attendance import (
    AttendanceDevice,
    AttendanceDeviceUser,
    AttendancePunch,
    AttendancePolicySetting,
    AttendanceSyncRun,
    HRArea,
    HREmployee,
    HRPosition,
)
from ..models.user import Branch, User

router = APIRouter(prefix="/api/attendance", tags=["Attendance synchronization"])
web_router = APIRouter(tags=["Attendance"])


class AttendanceSocketHub:
    def __init__(self):
        self._lock = threading.Lock()
        self._subscribers = {}

    def subscribe(self):
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue()
        key = id(queue)
        with self._lock:
            self._subscribers[key] = (loop, queue)
        return key, queue

    def unsubscribe(self, key):
        with self._lock:
            self._subscribers.pop(key, None)

    def publish(self, payload):
        with self._lock:
            subscribers = list(self._subscribers.values())
        for loop, queue in subscribers:
            try:
                loop.call_soon_threadsafe(queue.put_nowait, payload)
            except RuntimeError:
                pass


attendance_socket_hub = AttendanceSocketHub()


class DeviceUserIn(BaseModel):
    user_id: str = Field(min_length=1, max_length=40)
    uid: Optional[int] = None
    name: Optional[str] = Field(default=None, max_length=160)
    card_number: Optional[str] = Field(default=None, max_length=80)


class PunchIn(BaseModel):
    user_id: str = Field(min_length=1, max_length=40)
    occurred_at: datetime
    punch_state: Optional[int] = None
    verify_mode: Optional[int] = None
    work_code: Optional[str] = Field(default=None, max_length=40)


class AttendanceBatchIn(BaseModel):
    device_code: str = Field(min_length=1, max_length=50)
    device_name: str = Field(default="Reloj biometrico", max_length=120)
    model: Optional[str] = Field(default=None, max_length=80)
    serial_number: Optional[str] = Field(default=None, max_length=100)
    local_ip: Optional[str] = Field(default=None, max_length=64)
    local_port: int = Field(default=4370, ge=1, le=65535)
    users: List[DeviceUserIn] = Field(default_factory=list)
    punches: List[PunchIn] = Field(default_factory=list, max_length=10000)


def _require_sync_token(x_attendance_token: Optional[str] = Header(default=None)) -> None:
    expected = os.getenv("ATTENDANCE_SYNC_TOKEN", "").strip()
    expected_hash = settings.ATTENDANCE_SYNC_TOKEN_SHA256.strip().lower()
    if not expected and not expected_hash:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Token de sincronizacion no configurado",
        )
    supplied = (x_attendance_token or "").strip()
    valid_plain = bool(expected and supplied and hmac.compare_digest(supplied, expected))
    supplied_hash = hashlib.sha256(supplied.encode("utf-8")).hexdigest() if supplied else ""
    valid_hash = bool(expected_hash and supplied_hash and hmac.compare_digest(supplied_hash, expected_hash))
    if not valid_plain and not valid_hash:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token de sincronizacion invalido")


def _event_key(device_code: str, punch: PunchIn) -> str:
    canonical = "|".join(
        [
            device_code.strip().lower(),
            punch.user_id.strip(),
            punch.occurred_at.isoformat(timespec="seconds"),
            str(punch.punch_state if punch.punch_state is not None else ""),
            str(punch.verify_mode if punch.verify_mode is not None else ""),
            punch.work_code or "",
        ]
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@router.get("/health", dependencies=[Depends(_require_sync_token)])
def attendance_health(db: Session = Depends(get_db)):
    return {
        "ok": True,
        "server_time": datetime.utcnow(),
        "devices": db.query(AttendanceDevice).filter(AttendanceDevice.active.is_(True)).count(),
    }


@router.post("/ingest", dependencies=[Depends(_require_sync_token)])
def ingest_attendance_batch(payload: AttendanceBatchIn, db: Session = Depends(get_db)):
    now = datetime.utcnow()
    device_code = payload.device_code.strip().lower()
    device = db.query(AttendanceDevice).filter(AttendanceDevice.code == device_code).first()
    if not device:
        device = AttendanceDevice(code=device_code, name=payload.device_name)
        db.add(device)
        db.flush()

    device.name = payload.device_name
    device.model = payload.model or device.model
    device.serial_number = payload.serial_number or device.serial_number
    device.local_ip = payload.local_ip or device.local_ip
    device.local_port = payload.local_port
    device.last_seen_at = now

    sync_run = AttendanceSyncRun(device_id=device.id, received_count=len(payload.punches))
    db.add(sync_run)
    db.flush()

    links = {
        row.device_user_id: row
        for row in db.query(AttendanceDeviceUser).filter(AttendanceDeviceUser.device_id == device.id).all()
    }
    for user in payload.users:
        user_id = user.user_id.strip()
        link = links.get(user_id)
        if not link:
            link = AttendanceDeviceUser(device_id=device.id, device_user_id=user_id)
            db.add(link)
            links[user_id] = link
        link.device_uid = user.uid
        link.device_name = user.name
        link.card_number = user.card_number
        link.last_seen_at = now

    inserted = 0
    duplicates = 0
    new_events = []
    for punch in payload.punches:
        user_id = punch.user_id.strip()
        link = links.get(user_id)
        if not link:
            link = AttendanceDeviceUser(
                device_id=device.id,
                device_user_id=user_id,
                last_seen_at=now,
            )
            db.add(link)
            links[user_id] = link
        else:
            link.last_seen_at = now

        occurred_at = punch.occurred_at.replace(tzinfo=None)
        event_key = _event_key(device_code, punch)
        statement = (
            pg_insert(AttendancePunch)
            .values(
                source_event_key=event_key,
                device_id=device.id,
                device_user_id=user_id,
                employee_id=link.employee_id,
                occurred_at=occurred_at,
                punch_state=punch.punch_state,
                verify_mode=punch.verify_mode,
                work_code=punch.work_code,
                raw_payload=json.dumps(punch.model_dump(mode="json"), ensure_ascii=False),
                received_at=now,
            )
            .on_conflict_do_nothing(index_elements=["source_event_key"])
            .returning(AttendancePunch.id)
        )
        inserted_id = db.execute(statement).scalar_one_or_none()
        if inserted_id is None:
            duplicates += 1
            continue
        inserted += 1
        new_events.append(
            {
                "id": inserted_id,
                "occurred_at": occurred_at.isoformat(timespec="seconds"),
                "date": occurred_at.strftime("%d/%m/%Y"),
                "time": occurred_at.strftime("%I:%M:%S %p"),
                "device_user_id": user_id,
                "employee": link.employee.full_name if link.employee else "Sin vincular",
                "device": device.name,
                "punch_state": punch.punch_state,
            }
        )

    try:
        device.last_sync_at = now
        sync_run.finished_at = now
        sync_run.status = "OK"
        sync_run.inserted_count = inserted
        sync_run.duplicate_count = duplicates
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Lote duplicado o inconsistente") from exc

    if new_events:
        attendance_socket_hub.publish({"type": "attendance.punches", "events": new_events})

    return {
        "ok": True,
        "device_code": device_code,
        "received": len(payload.punches),
        "inserted": inserted,
        "duplicates": duplicates,
        "device_users": len(links),
    }


def _browser_admin(request: Request, db: Session) -> User:
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as exc:
        raise HTTPException(status_code=303, headers={"Location": "/login"}) from exc
    email = payload.get("sub")
    user = db.query(User).filter(User.email == email).first() if email else None
    if not user or not user.is_active:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    if not any(role.name == "administrador" for role in user.roles):
        raise HTTPException(status_code=403, detail="Acceso denegado")
    return user


@web_router.get("/attendance")
def attendance_page(
    request: Request,
    punch_date: str = Query(""),
    punch_search: str = Query(""),
    db: Session = Depends(get_db),
):
    user = _browser_admin(request, db)
    punch_query = (
        db.query(AttendancePunch)
        .outerjoin(HREmployee, HREmployee.id == AttendancePunch.employee_id)
        .outerjoin(
            AttendanceDeviceUser,
            and_(
                AttendanceDeviceUser.device_id == AttendancePunch.device_id,
                AttendanceDeviceUser.device_user_id == AttendancePunch.device_user_id,
            ),
        )
    )
    selected_punch_date = None
    if punch_date.strip():
        try:
            selected_punch_date = date.fromisoformat(punch_date)
        except ValueError:
            selected_punch_date = None
    if selected_punch_date:
        punch_start = datetime.combine(selected_punch_date, time.min)
        punch_query = punch_query.filter(
            AttendancePunch.occurred_at >= punch_start,
            AttendancePunch.occurred_at < punch_start + timedelta(days=1),
        )
    normalized_punch_search = punch_search.strip()
    if normalized_punch_search:
        pattern = f"%{normalized_punch_search}%"
        punch_query = punch_query.filter(
            or_(
                AttendancePunch.device_user_id.ilike(pattern),
                HREmployee.full_name.ilike(pattern),
                HREmployee.employee_code.ilike(pattern),
                AttendanceDeviceUser.device_name.ilike(pattern),
            )
        )
    punches = punch_query.order_by(AttendancePunch.occurred_at.desc()).limit(100).all()
    return request.app.state.templates.TemplateResponse(
        "attendance.html",
        {
            "request": request,
            "user": user,
            "areas": db.query(HRArea).order_by(HRArea.name).all(),
            "positions": db.query(HRPosition).order_by(HRPosition.name).all(),
            "employees": db.query(HREmployee).order_by(HREmployee.full_name).all(),
            "devices": db.query(AttendanceDevice).order_by(AttendanceDevice.name).all(),
            "device_users": db.query(AttendanceDeviceUser).order_by(AttendanceDeviceUser.device_name).all(),
            "branches": db.query(Branch).filter(Branch.activo.is_(True)).order_by(Branch.name).all(),
            "punches": punches,
            "punch_date": punch_date,
            "punch_search": normalized_punch_search,
        },
    )


def _duration_label(total_minutes: int) -> str:
    total_minutes = max(0, int(total_minutes or 0))
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours:02d}:{minutes:02d}"


def _unique_catalog_code(db: Session, model, name: str) -> str:
    normalized = (
        unicodedata.normalize("NFKD", name.strip().lower())
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    base = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")[:32] or "registro"
    candidate = base
    suffix = 2
    while db.query(model.id).filter(model.code == candidate).first():
        candidate = f"{base[:27]}_{suffix}"
        suffix += 1
    return candidate


@web_router.get("/attendance/control")
def attendance_control_page(
    request: Request,
    date_from: str = Query(""),
    date_to: str = Query(""),
    area_id: str = Query(""),
    search: str = Query(""),
    db: Session = Depends(get_db),
):
    user = _browser_admin(request, db)
    today = date.today()
    try:
        selected_date_from = date.fromisoformat(date_from) if date_from.strip() else today
    except ValueError:
        selected_date_from = today
    try:
        selected_date_to = date.fromisoformat(date_to) if date_to.strip() else selected_date_from
    except ValueError:
        selected_date_to = selected_date_from
    if selected_date_to < selected_date_from:
        selected_date_from, selected_date_to = selected_date_to, selected_date_from
    range_days = (selected_date_to - selected_date_from).days + 1
    range_limited = range_days > 31
    if range_limited:
        selected_date_to = selected_date_from + timedelta(days=30)
        range_days = 31
    report_dates = [selected_date_from + timedelta(days=offset) for offset in range(range_days)]
    start_at = datetime.combine(selected_date_from, time.min)
    end_at = datetime.combine(selected_date_to + timedelta(days=1), time.min)
    policy = db.query(AttendancePolicySetting).order_by(AttendancePolicySetting.id).first()
    if not policy:
        policy = AttendancePolicySetting()
    expected_minutes = max(1, int(policy.expected_daily_minutes or 480))
    break_minutes = max(0, int(policy.break_minutes or 0))
    break_after_minutes = max(0, int(policy.break_after_minutes or 0))
    selected_area_id = int(area_id) if area_id.strip().isdigit() else None

    employee_query = db.query(HREmployee).filter(HREmployee.status == "ACTIVE")
    if selected_area_id:
        employee_query = employee_query.filter(HREmployee.area_id == selected_area_id)
    normalized_search = search.strip()
    if normalized_search:
        employee_query = employee_query.filter(
            HREmployee.full_name.ilike(f"%{normalized_search}%")
            | HREmployee.employee_code.ilike(f"%{normalized_search}%")
        )
    employees = employee_query.order_by(HREmployee.full_name).all()
    employee_ids = [employee.id for employee in employees]

    punches_by_employee_date = {}
    if employee_ids:
        punches = (
            db.query(AttendancePunch)
            .filter(
                AttendancePunch.employee_id.in_(employee_ids),
                AttendancePunch.occurred_at >= start_at,
                AttendancePunch.occurred_at < end_at,
            )
            .order_by(AttendancePunch.occurred_at.asc())
            .all()
        )
        for punch in punches:
            key = (punch.employee_id, punch.occurred_at.date())
            punches_by_employee_date.setdefault(key, []).append(punch)

    groups = {}
    totals = {"employees": len(employees), "employee_days": len(employees) * range_days, "present": 0, "pending": 0, "absent": 0, "overtime_minutes": 0}
    for employee in employees:
        area_name = employee.area.name if employee.area else "Sin area asignada"
        group = groups.setdefault(area_name, {"name": area_name, "rows": []})
        for report_date in report_dates:
            employee_punches = punches_by_employee_date.get((employee.id, report_date), [])
            entry = employee_punches[0].occurred_at if employee_punches else None
            exit_at = employee_punches[-1].occurred_at if len(employee_punches) >= 2 else None
            worked_minutes = 0
            overtime_minutes = 0
            regular_minutes = 0
            overtime_detail = "Sin salida para calcular"
            weekday = report_date.weekday()
            if weekday == 6 and policy.sunday_all_day_overtime:
                overtime_rule = "Domingo: toda la jornada"
            elif weekday == 5:
                overtime_rule = f"Sabado despues de {policy.saturday_overtime_start.strftime('%I:%M %p')}"
            else:
                overtime_rule = f"Lun-Vie despues de {policy.weekday_overtime_start.strftime('%I:%M %p')}"
            if entry and exit_at:
                gross_minutes = max(0, int((exit_at - entry).total_seconds() // 60))
                applied_break = break_minutes if gross_minutes >= break_after_minutes else 0
                worked_minutes = max(0, gross_minutes - applied_break)
                if weekday == 6 and policy.sunday_all_day_overtime:
                    overtime_minutes = worked_minutes
                    overtime_detail = f"{entry.strftime('%I:%M %p')} - {exit_at.strftime('%I:%M %p')} (jornada dominical)"
                elif weekday == 5:
                    overtime_start = datetime.combine(report_date, policy.saturday_overtime_start)
                    effective_start = max(entry, overtime_start)
                    overtime_minutes = max(0, int((exit_at - effective_start).total_seconds() // 60))
                    overtime_detail = (
                        f"{effective_start.strftime('%I:%M %p')} - {exit_at.strftime('%I:%M %p')}"
                        if overtime_minutes else "No alcanzo el inicio de tiempo extra"
                    )
                elif weekday <= 4:
                    overtime_start = datetime.combine(report_date, policy.weekday_overtime_start)
                    effective_start = max(entry, overtime_start)
                    overtime_minutes = max(0, int((exit_at - effective_start).total_seconds() // 60))
                    overtime_detail = (
                        f"{effective_start.strftime('%I:%M %p')} - {exit_at.strftime('%I:%M %p')}"
                        if overtime_minutes else "No alcanzo el inicio de tiempo extra"
                    )
                regular_minutes = max(0, worked_minutes - overtime_minutes)
                totals["present"] += 1
            elif entry:
                totals["pending"] += 1
            else:
                totals["absent"] += 1
            totals["overtime_minutes"] += overtime_minutes

            if not entry:
                status_label, status_class = "Sin marcadas", "secondary"
            elif not exit_at:
                status_label, status_class = "Salida pendiente", "warning"
            elif overtime_minutes > 0:
                status_label, status_class = "Tiempo extra", "success"
            elif worked_minutes < expected_minutes:
                status_label, status_class = "Jornada parcial", "info"
            else:
                status_label, status_class = "Jornada completa", "primary"

            group["rows"].append(
                {
                    "date": report_date,
                    "employee": employee,
                    "entry": entry,
                    "exit": exit_at,
                    "punch_count": len(employee_punches),
                    "worked_label": _duration_label(worked_minutes) if exit_at else "--:--",
                    "regular_label": _duration_label(regular_minutes) if exit_at else "--:--",
                    "overtime_label": _duration_label(overtime_minutes),
                    "overtime_minutes": overtime_minutes,
                    "overtime_rule": overtime_rule,
                    "overtime_detail": overtime_detail,
                    "status_label": status_label,
                    "status_class": status_class,
                }
            )

    unlinked_count = (
        db.query(AttendancePunch)
        .filter(
            AttendancePunch.employee_id.is_(None),
            AttendancePunch.occurred_at >= start_at,
            AttendancePunch.occurred_at < end_at,
        )
        .count()
    )
    return request.app.state.templates.TemplateResponse(
        "attendance_control.html",
        {
            "request": request,
            "user": user,
            "selected_date_from": selected_date_from,
            "selected_date_to": selected_date_to,
            "range_days": range_days,
            "range_limited": range_limited,
            "selected_area_id": selected_area_id,
            "search": normalized_search,
            "areas": db.query(HRArea).filter(HRArea.active.is_(True)).order_by(HRArea.name).all(),
            "groups": sorted(groups.values(), key=lambda item: item["name"]),
            "totals": totals,
            "total_overtime_label": _duration_label(totals["overtime_minutes"]),
            "expected_minutes": expected_minutes,
            "expected_label": _duration_label(expected_minutes),
            "break_minutes": break_minutes,
            "break_after_minutes": break_after_minutes,
            "unlinked_count": unlinked_count,
            "policy": policy,
        },
    )


@web_router.post("/attendance/areas")
def create_area(
    request: Request,
    name: str = Form(..., min_length=1),
    db: Session = Depends(get_db),
):
    _browser_admin(request, db)
    clean_name = name.strip()
    db.add(HRArea(code=_unique_catalog_code(db, HRArea, clean_name), name=clean_name))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse("/attendance?error=Area+duplicada", status_code=303)
    return RedirectResponse("/attendance?ok=Area+creada", status_code=303)


@web_router.post("/attendance/positions")
def create_position(
    request: Request,
    name: str = Form(..., min_length=1),
    db: Session = Depends(get_db),
):
    _browser_admin(request, db)
    clean_name = name.strip()
    db.add(HRPosition(code=_unique_catalog_code(db, HRPosition, clean_name), name=clean_name))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse("/attendance?error=Cargo+duplicado", status_code=303)
    return RedirectResponse("/attendance?ok=Cargo+creado", status_code=303)


@web_router.post("/data/attendance-policy")
def update_attendance_policy(
    request: Request,
    weekday_overtime_start: str = Form(...),
    saturday_overtime_start: str = Form(...),
    sunday_all_day_overtime: Optional[str] = Form(None),
    expected_daily_minutes: int = Form(..., ge=1, le=1440),
    break_minutes: int = Form(..., ge=0, le=480),
    break_after_minutes: int = Form(..., ge=0, le=1440),
    db: Session = Depends(get_db),
):
    user = _browser_admin(request, db)
    try:
        weekday_start = time.fromisoformat(weekday_overtime_start)
        saturday_start = time.fromisoformat(saturday_overtime_start)
    except ValueError:
        return RedirectResponse("/data?policy_error=Horario+invalido", status_code=303)
    policy = db.query(AttendancePolicySetting).order_by(AttendancePolicySetting.id).first()
    if not policy:
        policy = AttendancePolicySetting()
        db.add(policy)
    policy.weekday_overtime_start = weekday_start
    policy.saturday_overtime_start = saturday_start
    policy.sunday_all_day_overtime = sunday_all_day_overtime == "on"
    policy.expected_daily_minutes = expected_daily_minutes
    policy.break_minutes = break_minutes
    policy.break_after_minutes = break_after_minutes
    policy.updated_by = user.email
    policy.updated_at = datetime.utcnow()
    db.commit()
    return RedirectResponse("/data?policy_ok=Politica+de+marcadas+actualizada", status_code=303)


@web_router.post("/attendance/employees")
def create_employee(
    request: Request,
    employee_code: str = Form(..., min_length=1),
    full_name: str = Form(..., min_length=1),
    identification: Optional[str] = Form(None),
    area_id: str = Form(""),
    position_id: str = Form(""),
    branch_id: str = Form(""),
    db: Session = Depends(get_db),
):
    _browser_admin(request, db)
    db.add(
        HREmployee(
            employee_code=employee_code.strip(),
            full_name=full_name.strip(),
            identification=(identification or "").strip() or None,
            area_id=int(area_id) if area_id.strip() else None,
            position_id=int(position_id) if position_id.strip() else None,
            branch_id=int(branch_id) if branch_id.strip() else None,
        )
    )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse("/attendance?error=Empleado+duplicado", status_code=303)
    return RedirectResponse("/attendance?ok=Empleado+creado", status_code=303)


@web_router.post("/attendance/device-users/{link_id}/employee")
def link_device_user(
    link_id: int,
    request: Request,
    employee_id: int = Form(...),
    db: Session = Depends(get_db),
):
    _browser_admin(request, db)
    link = db.query(AttendanceDeviceUser).filter(AttendanceDeviceUser.id == link_id).first()
    employee = db.query(HREmployee).filter(HREmployee.id == employee_id).first()
    if not link or not employee:
        raise HTTPException(status_code=404, detail="Registro no encontrado")
    link.employee_id = employee.id
    db.query(AttendancePunch).filter(
        AttendancePunch.device_id == link.device_id,
        AttendancePunch.device_user_id == link.device_user_id,
    ).update({"employee_id": employee.id}, synchronize_session=False)
    db.commit()
    return RedirectResponse("/attendance?ok=Usuario+vinculado", status_code=303)


@web_router.post("/attendance/sync-now")
def sync_attendance_now(request: Request, db: Session = Depends(get_db)):
    _browser_admin(request, db)
    device_ip = os.getenv("ATTENDANCE_DEVICE_IP", "192.168.1.132").strip()
    device_port = int(os.getenv("ATTENDANCE_DEVICE_PORT", "4370"))
    comm_key = int(os.getenv("ATTENDANCE_DEVICE_COMM_KEY", "0"))
    device_code = os.getenv("ATTENDANCE_DEVICE_CODE", "ta040-central").strip()
    conn = None
    try:
        from zk import ZK

        conn = ZK(
            device_ip,
            port=device_port,
            timeout=10,
            password=comm_key,
            force_udp=False,
            ommit_ping=False,
        ).connect()
        raw_users = conn.get_users() or []
        raw_punches = conn.get_attendance() or []
        try:
            serial_number = conn.get_serialnumber()
        except Exception:
            serial_number = None
    except Exception as exc:
        message = quote_plus(f"No fue posible leer el reloj: {exc}")
        return RedirectResponse(f"/attendance?error={message}", status_code=303)
    finally:
        if conn is not None:
            try:
                conn.disconnect()
            except Exception:
                pass

    payload = AttendanceBatchIn(
        device_code=device_code,
        device_name="Reloj TA040 Central",
        model="3nStar TA040",
        serial_number=serial_number,
        local_ip=device_ip,
        local_port=device_port,
        users=[
            DeviceUserIn(
                user_id=str(user.user_id),
                uid=user.uid,
                name=user.name or None,
                card_number=str(user.card) if getattr(user, "card", None) else None,
            )
            for user in raw_users
        ],
        punches=[
            PunchIn(
                user_id=str(item.user_id),
                occurred_at=item.timestamp,
                punch_state=getattr(item, "punch", None),
                verify_mode=getattr(item, "status", None),
                work_code=str(item.workcode) if getattr(item, "workcode", None) else None,
            )
            for item in raw_punches
        ],
    )
    result = ingest_attendance_batch(payload, db)
    message = quote_plus(
        f"Sincronizacion completa: {result['device_users']} usuarios, "
        f"{result['inserted']} marcadas nuevas y {result['duplicates']} repetidas"
    )
    return RedirectResponse(f"/attendance?ok={message}", status_code=303)


@web_router.websocket("/ws/attendance")
async def attendance_websocket(websocket: WebSocket):
    token = websocket.cookies.get("access_token")
    db = get_session_local()()
    authorized = False
    try:
        if token:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            email = payload.get("sub")
            user = db.query(User).filter(User.email == email).first() if email else None
            authorized = bool(
                user
                and user.is_active
                and any(role.name == "administrador" for role in user.roles)
            )
    except JWTError:
        authorized = False
    finally:
        db.close()
    if not authorized:
        await websocket.close(code=4401)
        return

    await websocket.accept()
    subscriber_id, queue = attendance_socket_hub.subscribe()
    try:
        await websocket.send_json({"type": "attendance.connected"})
        while True:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=25)
                await websocket.send_json(message)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "attendance.ping"})
    except Exception:
        pass
    finally:
        attendance_socket_hub.unsubscribe(subscriber_id)


@web_router.get("/attendance/live-punches")
def attendance_live_punches(request: Request, after_id: int = Query(0, ge=0), db: Session = Depends(get_db)):
    _browser_admin(request, db)
    punches = (
        db.query(AttendancePunch)
        .filter(AttendancePunch.id > after_id)
        .order_by(AttendancePunch.id.asc())
        .limit(100)
        .all()
    )
    return {
        "events": [
            {
                "id": punch.id,
                "occurred_at": punch.occurred_at.isoformat(timespec="seconds"),
                "date": punch.occurred_at.strftime("%d/%m/%Y"),
                "time": punch.occurred_at.strftime("%I:%M:%S %p"),
                "device_user_id": punch.device_user_id,
                "employee": punch.employee.full_name if punch.employee else "Sin vincular",
                "device": punch.device.name,
                "punch_state": punch.punch_state,
            }
            for punch in punches
        ]
    }
