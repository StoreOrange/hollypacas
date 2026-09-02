from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from ..database import Base


class HRArea(Base):
    __tablename__ = "hr_areas"

    id = Column(Integer, primary_key=True)
    code = Column(String(40), unique=True, nullable=False, index=True)
    name = Column(String(120), unique=True, nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    employees = relationship("HREmployee", back_populates="area")


class HRPosition(Base):
    __tablename__ = "hr_positions"

    id = Column(Integer, primary_key=True)
    code = Column(String(40), unique=True, nullable=False, index=True)
    name = Column(String(120), unique=True, nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    employees = relationship("HREmployee", back_populates="position")


class HREmployee(Base):
    __tablename__ = "hr_employees"

    id = Column(Integer, primary_key=True)
    employee_code = Column(String(40), unique=True, nullable=False, index=True)
    full_name = Column(String(160), nullable=False, index=True)
    identification = Column(String(40), unique=True, nullable=True)
    email = Column(String(160), nullable=True)
    phone = Column(String(40), nullable=True)
    hire_date = Column(Date, nullable=True)
    termination_date = Column(Date, nullable=True)
    status = Column(String(20), nullable=False, default="ACTIVE")
    area_id = Column(Integer, ForeignKey("hr_areas.id"), nullable=True)
    position_id = Column(Integer, ForeignKey("hr_positions.id"), nullable=True)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=True)
    payroll_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    area = relationship("HRArea", back_populates="employees")
    position = relationship("HRPosition", back_populates="employees")
    branch = relationship("Branch")
    payroll_user = relationship("User")
    device_links = relationship("AttendanceDeviceUser", back_populates="employee")


class AttendanceDevice(Base):
    __tablename__ = "attendance_devices"

    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(120), nullable=False)
    model = Column(String(80), nullable=True)
    serial_number = Column(String(100), nullable=True)
    local_ip = Column(String(64), nullable=True)
    local_port = Column(Integer, nullable=False, default=4370)
    branch_id = Column(Integer, ForeignKey("branches.id"), nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    last_seen_at = Column(DateTime, nullable=True)
    last_sync_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    branch = relationship("Branch")
    users = relationship("AttendanceDeviceUser", back_populates="device")
    punches = relationship("AttendancePunch", back_populates="device")


class AttendanceDeviceUser(Base):
    __tablename__ = "attendance_device_users"
    __table_args__ = (
        UniqueConstraint("device_id", "device_user_id", name="uq_attendance_device_user"),
    )

    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey("attendance_devices.id", ondelete="CASCADE"), nullable=False)
    device_user_id = Column(String(40), nullable=False)
    device_uid = Column(Integer, nullable=True)
    device_name = Column(String(160), nullable=True)
    card_number = Column(String(80), nullable=True)
    employee_id = Column(Integer, ForeignKey("hr_employees.id"), nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    first_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_seen_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    device = relationship("AttendanceDevice", back_populates="users")
    employee = relationship("HREmployee", back_populates="device_links")


class AttendancePunch(Base):
    __tablename__ = "attendance_punches"
    __table_args__ = (
        UniqueConstraint("source_event_key", name="uq_attendance_source_event_key"),
    )

    id = Column(Integer, primary_key=True)
    source_event_key = Column(String(160), nullable=False, index=True)
    device_id = Column(Integer, ForeignKey("attendance_devices.id"), nullable=False)
    device_user_id = Column(String(40), nullable=False, index=True)
    employee_id = Column(Integer, ForeignKey("hr_employees.id"), nullable=True, index=True)
    occurred_at = Column(DateTime, nullable=False, index=True)
    punch_state = Column(Integer, nullable=True)
    verify_mode = Column(Integer, nullable=True)
    work_code = Column(String(40), nullable=True)
    raw_payload = Column(Text, nullable=True)
    received_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    device = relationship("AttendanceDevice", back_populates="punches")
    employee = relationship("HREmployee")


class AttendanceSyncRun(Base):
    __tablename__ = "attendance_sync_runs"

    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey("attendance_devices.id"), nullable=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="RUNNING")
    received_count = Column(Integer, nullable=False, default=0)
    inserted_count = Column(Integer, nullable=False, default=0)
    duplicate_count = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)

    device = relationship("AttendanceDevice")


class AttendancePolicySetting(Base):
    __tablename__ = "attendance_policy_settings"

    id = Column(Integer, primary_key=True)
    weekday_overtime_start = Column(Time, nullable=False, default=lambda: datetime.strptime("17:00", "%H:%M").time())
    saturday_overtime_start = Column(Time, nullable=False, default=lambda: datetime.strptime("16:00", "%H:%M").time())
    sunday_all_day_overtime = Column(Boolean, nullable=False, default=True)
    expected_daily_minutes = Column(Integer, nullable=False, default=480)
    break_minutes = Column(Integer, nullable=False, default=60)
    break_after_minutes = Column(Integer, nullable=False, default=360)
    updated_by = Column(String(160), nullable=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
