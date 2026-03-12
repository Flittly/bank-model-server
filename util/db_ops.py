"""
Database operations for tasks, banks, cross sections, basic params, and correction lines
重构版：使用bank表、task表、basic_params表、新的cross_sections表
"""

import json
from typing import List, Dict, Any, Optional, Tuple

from util import db


# ========================================
# Bank 相关操作
# ========================================


def create_bank(
    bank_id: str,
    bank_name: str,
    region_code: str,
    geometry: Optional[Dict[str, Any]] = None,
    bank_geometry: Optional[Dict[str, Any]] = None,
    description: Optional[str] = None,
) -> int:
    """
    Create a bank segment
    """
    with db.get_db_cursor() as (conn, cursor):
        geom_json = json.dumps(geometry) if geometry else None

        cursor.execute(
            """
            INSERT INTO banks (
                bank_id, bank_name, region_code,
                start_point, end_point, geom,
                bank_geometry, description
            ) VALUES (
                %s, %s, %s,
                ST_StartPoint(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
                ST_EndPoint(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
                ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
                %s::jsonb, %s
            )
            RETURNING id
            """,
            (
                bank_id,
                bank_name,
                region_code,
                geom_json,
                geom_json,
                geom_json,
                json.dumps(bank_geometry) if bank_geometry else None,
                description,
            ),
        )
        return cursor.fetchone()[0]


def get_banks(region_code: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get all banks
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        query = """
            SELECT 
                b.*,
                ST_AsGeoJSON(b.geom)::jsonb as geometry
            FROM banks b
        """
        params = []

        conditions = []
        if region_code is not None:
            conditions.append("b.region_code = %s")
            params.append(region_code)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY b.id"

        cursor.execute(query, tuple(params))
        return cursor.fetchall()


def get_bank(bank_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single bank by bank_id
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT 
                b.*,
                ST_AsGeoJSON(b.geom)::jsonb as geometry
            FROM banks b
            WHERE b.bank_id = %s
            """,
            (bank_id,),
        )
        result = cursor.fetchone()
        return result if result else None


def update_bank(bank_id: str, **kwargs) -> bool:
    """
    Update a bank
    """
    if not kwargs:
        return False

    set_clause_parts = []
    values = []

    for key, value in kwargs.items():
        if key == "geometry":
            geom_json = json.dumps(value) if value else None
            set_clause_parts.append(
                "start_point = ST_StartPoint(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
            )
            set_clause_parts.append(
                "end_point = ST_EndPoint(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
            )
            set_clause_parts.append("geom = ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)")
            values.extend([geom_json, geom_json, geom_json])
        elif key == "bank_geometry":
            set_clause_parts.append(f"{key} = %s::jsonb")
            values.append(json.dumps(value) if value else None)
        else:
            set_clause_parts.append(f"{key} = %s")
            values.append(value)

    if not set_clause_parts:
        return False

    set_clause = ", ".join(set_clause_parts)
    values.append(bank_id)

    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            f"""
            UPDATE banks
            SET {set_clause}
            WHERE bank_id = %s
            """,
            tuple(values),
        )
        return cursor.rowcount > 0


def delete_bank(bank_id: str) -> bool:
    """
    Delete a bank
    """
    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            """
            DELETE FROM banks
            WHERE bank_id = %s
            RETURNING id
            """,
            (bank_id,),
        )
        return cursor.fetchone() is not None


# ========================================
# Task 相关操作
# ========================================


def create_task(
    task_id: str,
    task_name: str,
    bank_ids: Optional[List[str]] = None,
    description: Optional[str] = None,
) -> int:
    """
    Create a new task
    """
    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            """
            INSERT INTO tasks (task_id, task_name, bank_ids, description)
            VALUES (%s, %s, %s::jsonb, %s)
            RETURNING id
            """,
            (
                task_id,
                task_name,
                json.dumps(bank_ids) if bank_ids else None,
                description,
            ),
        )
        return cursor.fetchone()[0]


def get_tasks() -> List[Dict[str, Any]]:
    """
    Get all tasks
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT id, task_id, task_name, bank_ids, description, created_at, updated_at
            FROM tasks
            ORDER BY id DESC
            """
        )
        return cursor.fetchall()


def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single task by task_id
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT id, task_id, task_name, bank_ids, description, created_at, updated_at
            FROM tasks
            WHERE task_id = %s
            """,
            (task_id,),
        )
        result = cursor.fetchone()
        return result if result else None


def delete_task(task_id: str) -> bool:
    """
    Delete a task and all related data (cascade)
    """
    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            """
            DELETE FROM tasks
            WHERE task_id = %s
            RETURNING id
            """,
            (task_id,),
        )
        return cursor.fetchone() is not None


def update_task_status(
    task_id: str,
    status: str,
    run_started_at: Optional[str] = None,
    run_completed_at: Optional[str] = None,
    error_message: Optional[str] = None,
    clear_run_started_at: bool = False,
    clear_run_completed_at: bool = False,
    clear_error_message: bool = False,
) -> bool:
    """
    Update task status and related fields
    """
    with db.get_db_cursor() as (conn, cursor):
        set_clause_parts = ["status = %s"]
        values = [status]

        if clear_run_started_at:
            set_clause_parts.append("run_started_at = NULL")
        elif run_started_at is not None:
            set_clause_parts.append("run_started_at = %s")
            values.append(run_started_at)
        if clear_run_completed_at:
            set_clause_parts.append("run_completed_at = NULL")
        elif run_completed_at is not None:
            set_clause_parts.append("run_completed_at = %s")
            values.append(run_completed_at)
        if clear_error_message:
            set_clause_parts.append("error_message = NULL")
        elif error_message is not None:
            set_clause_parts.append("error_message = %s")
            values.append(error_message)

        values.append(task_id)
        set_clause = ", ".join(set_clause_parts)

        cursor.execute(
            f"""
            UPDATE tasks
            SET {set_clause}
            WHERE task_id = %s
            """,
            tuple(values),
        )
        return cursor.rowcount > 0


def delete_risk_results(task_id: str) -> int:
    """
    Delete all persisted risk results for a task.
    """
    with db.get_db_cursor() as (conn, cursor):
        cursor.execute("DELETE FROM bank_risk_results WHERE task_id = %s", (task_id,))
        return cursor.rowcount


# ========================================
# Basic Params 相关操作
# ========================================


def create_basic_param(
    param_id: str,
    param_name: str,
    segment: Optional[str] = None,
    current_timepoint: Optional[str] = None,
    set_name: Optional[str] = None,
    water_qs: Optional[str] = None,
    tidal_level: Optional[str] = None,
    bench_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    hs: Optional[float] = None,
    hc: Optional[float] = None,
    protection_level: Optional[str] = None,
    control_level: Optional[str] = None,
    comparison_timepoint: Optional[str] = None,
    risk_thresholds: Optional[Dict[str, Any]] = None,
    weights: Optional[Dict[str, Any]] = None,
    other_params: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Create basic parameters for model calculation
    """
    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            """
            INSERT INTO basic_params (
                param_id, param_name,
                segment, current_timepoint, set_name, water_qs, tidal_level,
                bench_id, ref_id,
                hs, hc, protection_level, control_level,
                comparison_timepoint,
                risk_thresholds, weights, other_params
            ) VALUES (
                %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            RETURNING id
            """,
            (
                param_id,
                param_name,
                segment,
                current_timepoint,
                set_name,
                water_qs,
                tidal_level,
                bench_id,
                ref_id,
                hs,
                hc,
                protection_level,
                control_level,
                comparison_timepoint,
                json.dumps(risk_thresholds) if risk_thresholds else None,
                json.dumps(weights) if weights else None,
                json.dumps(other_params) if other_params else None,
            ),
        )
        return cursor.fetchone()[0]


def get_basic_params() -> List[Dict[str, Any]]:
    """
    Get all basic parameters
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT * FROM basic_params
            ORDER BY id DESC
            """
        )
        return cursor.fetchall()


def get_basic_param(param_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single basic param by param_id
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT * FROM basic_params
            WHERE param_id = %s
            """,
            (param_id,),
        )
        result = cursor.fetchone()
        return result if result else None


def get_basic_param_by_id(id: int) -> Optional[Dict[str, Any]]:
    """
    Get a single basic param by database id
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT * FROM basic_params
            WHERE id = %s
            """,
            (id,),
        )
        result = cursor.fetchone()
        return result if result else None


def update_basic_param(param_id: str, **kwargs) -> bool:
    """
    Update basic parameters
    """
    if not kwargs:
        return False

    set_clause = ", ".join([f"{key} = %s" for key in kwargs.keys()])
    values = list(kwargs.values()) + [param_id]

    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            f"""
            UPDATE basic_params
            SET {set_clause}
            WHERE param_id = %s
            """,
            tuple(values),
        )
        return cursor.rowcount > 0


# ========================================
# Cross Section 相关操作（重构版）
# ========================================


def create_cross_section(
    task_id_db: int,
    section_id: str,
    section_name: str,
    bank_id: str,
    region_code: str,
    segment_index: Optional[int] = None,
    geometry: Optional[Dict[str, Any]] = None,
    section_geometry: Optional[Dict[str, Any]] = None,
    distance: Optional[float] = None,
    basic_param_id: Optional[int] = None,
    # Section独立的参数字段
    param_name: Optional[str] = None,
    segment: Optional[str] = None,
    current_timepoint: Optional[str] = None,
    set_name: Optional[str] = None,
    water_qs: Optional[str] = None,
    tidal_level: Optional[str] = None,
    bench_id: Optional[str] = None,
    ref_id: Optional[str] = None,
    hs: Optional[float] = None,
    hc: Optional[float] = None,
    protection_level: Optional[str] = None,
    control_level: Optional[str] = None,
    comparison_timepoint: Optional[str] = None,
    risk_thresholds: Optional[Dict[str, Any]] = None,
    weights: Optional[Dict[str, Any]] = None,
    other_params: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Create a cross section in database (重构版 - 每个section独立存储参数)
    """
    with db.get_db_cursor() as (conn, cursor):
        geom_json = json.dumps(geometry) if geometry else None

        cursor.execute(
            """
            INSERT INTO cross_sections (
                task_id, section_id, section_name, bank_id, region_code,
                segment_index,
                start_point, end_point, geom,
                section_geometry,
                distance,
                basic_param_id,
                param_name, segment, current_timepoint, set_name, water_qs, tidal_level,
                bench_id, ref_id,
                hs, hc, protection_level, control_level,
                comparison_timepoint,
                risk_thresholds, weights, other_params
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s,
                ST_StartPoint(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
                ST_EndPoint(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
                ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
                %s::jsonb,
                %s,
                %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            RETURNING id
            """,
            (
                task_id_db,
                section_id,
                section_name,
                bank_id,
                region_code,
                segment_index,
                geom_json,
                geom_json,
                geom_json,
                json.dumps(section_geometry) if section_geometry else None,
                distance,
                basic_param_id,
                param_name,
                segment,
                current_timepoint,
                set_name,
                water_qs,
                tidal_level,
                bench_id,
                ref_id,
                hs,
                hc,
                protection_level,
                control_level,
                comparison_timepoint,
                json.dumps(risk_thresholds) if risk_thresholds else None,
                json.dumps(weights) if weights else None,
                json.dumps(other_params) if other_params else None,
            ),
        )
        return cursor.fetchone()[0]


def get_cross_sections(
    task_id_db: Optional[int] = None,
    bank_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get all cross sections (重构版 - 每个section独立存储参数)
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        query = """
            SELECT 
                cs.*,
                t.task_id,
                t.task_name,
                ST_AsGeoJSON(cs.geom)::jsonb as geometry
            FROM cross_sections cs
            JOIN tasks t ON cs.task_id = t.id
        """
        params = []

        conditions = []
        if task_id_db is not None:
            conditions.append("cs.task_id = %s")
            params.append(task_id_db)
        if bank_id is not None:
            conditions.append("cs.bank_id = %s")
            params.append(bank_id)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY cs.id"

        cursor.execute(query, tuple(params))
        return cursor.fetchall()


def get_cross_section(section_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single cross section by section_id (重构版 - 每个section独立存储参数)
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT 
                cs.*,
                t.task_id,
                t.task_name,
                ST_AsGeoJSON(cs.geom)::jsonb as geometry
            FROM cross_sections cs
            JOIN tasks t ON cs.task_id = t.id
            WHERE cs.section_id = %s
            """,
            (section_id,),
        )
        result = cursor.fetchone()
        return result if result else None


def delete_cross_section(section_id: str) -> bool:
    """
    Delete a cross section
    """
    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            """
            DELETE FROM cross_sections
            WHERE section_id = %s
            RETURNING id
            """,
            (section_id,),
        )
        return cursor.fetchone() is not None


def update_cross_section(section_id: str, **kwargs) -> bool:
    """
    Update a cross section (重构版 - 支持更新section独立的参数字段)
    """
    if not kwargs:
        return False

    set_clause_parts = []
    values = []

    for key, value in kwargs.items():
        if key == "geometry":
            geom_json = json.dumps(value) if value else None
            set_clause_parts.append(
                "start_point = ST_StartPoint(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
            )
            set_clause_parts.append(
                "end_point = ST_EndPoint(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
            )
            set_clause_parts.append("geom = ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)")
            values.extend([geom_json, geom_json, geom_json])
        elif key == "section_geometry":
            set_clause_parts.append(f"{key} = %s::jsonb")
            values.append(json.dumps(value) if value else None)
        elif key in ["risk_thresholds", "weights", "other_params"]:
            set_clause_parts.append(f"{key} = %s::jsonb")
            values.append(json.dumps(value) if value else None)
        else:
            set_clause_parts.append(f"{key} = %s")
            values.append(value)

    if not set_clause_parts:
        return False

    set_clause = ", ".join(set_clause_parts)
    values.append(section_id)

    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            f"""
            UPDATE cross_sections
            SET {set_clause}
            WHERE section_id = %s
            """,
            tuple(values),
        )
        return cursor.rowcount > 0


# ========================================
# 综合查询操作
# ========================================


def get_full_task_data(task_id: str) -> Optional[Dict[str, Any]]:
    """
    Get all data for a task (task + sections + basic_params)
    """
    task = get_task(task_id)
    if not task:
        return None

    task_id_db = task["id"]

    return {
        "task": task,
        "sections": get_cross_sections(task_id_db=task_id_db),
    }


def clear_task_data(task_id: str) -> Optional[Dict[str, int]]:
    """
    Clear all data for a task (but keep the task itself)
    """
    task = get_task(task_id)
    if not task:
        return None

    task_id_db = task["id"]

    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            "DELETE FROM bank_risk_results WHERE task_id = %s", (task_id_db,)
        )
        results_deleted = cursor.rowcount

        cursor.execute("DELETE FROM cross_sections WHERE task_id = %s", (task_id_db,))
        sections_deleted = cursor.rowcount

        # Note: basic_params are not deleted as they might be reused

        return {
            "sections": sections_deleted,
            "results": results_deleted,
        }


def create_risk_result(
    task_id: str,
    section_id: str,
    section_name: str,
    region_code: str,
    bank_id: str,
    risk_level: int,
    indicators: Dict[str, Any],
    geometry: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Create a risk result record
    """
    with db.get_db_cursor() as (conn, cursor):
        geom_json = json.dumps(geometry) if geometry else None

        cursor.execute(
            """
            INSERT INTO bank_risk_results (
                task_id, section_id, section_name, region_code, bank_id,
                risk_level, indicators, geom
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s::jsonb, 
                ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)
            )
            RETURNING id
            """,
            (
                task_id,
                section_id,
                section_name,
                region_code,
                bank_id,
                risk_level,
                json.dumps(indicators),
                geom_json,
            ),
        )
        return cursor.fetchone()[0]


def get_sections_by_task(task_id: str) -> List[Dict[str, Any]]:
    """
    Get all sections for a specific task
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        # First get the internal task id
        cursor.execute("SELECT id FROM tasks WHERE task_id = %s", (task_id,))
        task = cursor.fetchone()
        if not task:
            return []

        internal_task_id = task["id"]

        # Then get sections
        cursor.execute(
            """
            SELECT 
                s.*,
                ST_AsGeoJSON(s.geom)::jsonb as geometry
            FROM cross_sections s
            WHERE s.task_id = %s
            """,
            (internal_task_id,),
        )
        return cursor.fetchall()


def get_bank_risk_results(
    task_id: Optional[str] = None,
    section_id: Optional[str] = None,
    bank_id: Optional[str] = None,
    region_code: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get bank risk results
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        query = """
            SELECT 
                brr.*,
                ST_AsGeoJSON(brr.geom)::jsonb as geometry
            FROM bank_risk_results brr
        """
        params = []

        conditions = []
        if task_id is not None:
            conditions.append("brr.task_id = %s")
            params.append(task_id)
        if section_id is not None:
            conditions.append("brr.section_id = %s")
            params.append(section_id)
        if bank_id is not None:
            conditions.append("brr.bank_id = %s")
            params.append(bank_id)
        if region_code is not None:
            conditions.append("brr.region_code = %s")
            params.append(region_code)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY brr.id"

        cursor.execute(query, tuple(params))
        return cursor.fetchall()


def get_bank_risk_result(section_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single bank risk result by section_id
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT 
                brr.*,
                ST_AsGeoJSON(brr.geom)::jsonb as geometry
            FROM bank_risk_results brr
            WHERE brr.section_id = %s
            ORDER BY brr.run_time DESC, brr.id DESC
            LIMIT 1
            """,
            (section_id,),
        )
        result = cursor.fetchone()
        return result if result else None


# ========================================
# Hydrodynamic 相关操作
# ========================================


def create_hydrodynamic_point(
    point_id: str,
    region_code: str,
    set_name: str,
    water_qs: str,
    tidal_level: str,
    x: float,
    y: float,
) -> int:
    """
    Create a hydrodynamic point
    """
    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            """
            INSERT INTO hydrodynamic_points (
                point_id, region_code, set_name, water_qs, tidal_level, x, y, geom
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            )
            RETURNING id
            """,
            (point_id, region_code, set_name, water_qs, tidal_level, x, y, x, y),
        )
        return cursor.fetchone()[0]


def get_hydrodynamic_point(point_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single hydrodynamic point by point_id
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT 
                hp.*,
                ST_AsGeoJSON(hp.geom)::jsonb as geometry
            FROM hydrodynamic_points hp
            WHERE hp.point_id = %s
            """,
            (point_id,),
        )
        result = cursor.fetchone()
        return result if result else None


def get_hydrodynamic_points(
    region_code: Optional[str] = None,
    set_name: Optional[str] = None,
    water_qs: Optional[str] = None,
    tidal_level: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get hydrodynamic points with optional filters
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        query = """
            SELECT 
                hp.*,
                ST_AsGeoJSON(hp.geom)::jsonb as geometry
            FROM hydrodynamic_points hp
        """
        params = []

        conditions = []
        if region_code is not None:
            conditions.append("hp.region_code = %s")
            params.append(region_code)
        if set_name is not None:
            conditions.append("hp.set_name = %s")
            params.append(set_name)
        if water_qs is not None:
            conditions.append("hp.water_qs = %s")
            params.append(water_qs)
        if tidal_level is not None:
            conditions.append("hp.tidal_level = %s")
            params.append(tidal_level)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY hp.id"

        cursor.execute(query, tuple(params))
        return cursor.fetchall()


def create_hydrodynamic_data(
    point_id_db: int,
    time_step: int,
    h: Optional[float] = None,
    p: Optional[float] = None,
    u: Optional[float] = None,
    v: Optional[float] = None,
) -> int:
    """
    Create hydrodynamic data for a point at a specific time step
    """
    with db.get_db_cursor() as (conn, cursor):
        cursor.execute(
            """
            INSERT INTO hydrodynamic_data (point_id, time_step, h, p, u, v)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (point_id_db, time_step, h, p, u, v),
        )
        return cursor.fetchone()[0]


def get_hydrodynamic_data(
    point_id_db: Optional[int] = None,
    time_step: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Get hydrodynamic data with optional filters
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        query = "SELECT * FROM hydrodynamic_data"
        params = []

        conditions = []
        if point_id_db is not None:
            conditions.append("point_id = %s")
            params.append(point_id_db)
        if time_step is not None:
            conditions.append("time_step = %s")
            params.append(time_step)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY point_id, time_step"

        cursor.execute(query, tuple(params))
        return cursor.fetchall()


def get_hydrodynamic_data_by_point_id(point_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a point and all its time series data by point_id
    """
    point = get_hydrodynamic_point(point_id)
    if not point:
        return None

    data = get_hydrodynamic_data(point_id_db=point["id"])
    return {"point": point, "data": data}


def delete_hydrodynamic_points(
    region_code: Optional[str] = None,
    set_name: Optional[str] = None,
    water_qs: Optional[str] = None,
    tidal_level: Optional[str] = None,
) -> int:
    """
    Delete hydrodynamic points and related data (cascade)
    """
    with db.get_db_cursor() as (conn, cursor):
        query = "DELETE FROM hydrodynamic_points"
        params = []

        conditions = []
        if region_code is not None:
            conditions.append("region_code = %s")
            params.append(region_code)
        if set_name is not None:
            conditions.append("set_name = %s")
            params.append(set_name)
        if water_qs is not None:
            conditions.append("water_qs = %s")
            params.append(water_qs)
        if tidal_level is not None:
            conditions.append("tidal_level = %s")
            params.append(tidal_level)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        cursor.execute(query, tuple(params))
        return cursor.rowcount


def bulk_create_hydrodynamic_points(points: list) -> list:
    """
    批量创建水动力点，返回插入后的id列表
    points格式: [{'point_id': ..., 'region_code': ..., 'set_name': ..., 'water_qs': ..., 'tidal_level': ..., 'x': ..., 'y': ...}, ...]
    注意：为了简化，每次只插入一个点并返回其id
    """
    inserted_ids = []
    for p in points:
        try:
            point_id_db = create_hydrodynamic_point(
                point_id=p["point_id"],
                region_code=p["region_code"],
                set_name=p["set_name"],
                water_qs=p["water_qs"],
                tidal_level=p["tidal_level"],
                x=p["x"],
                y=p["y"],
            )
            inserted_ids.append(point_id_db)
        except Exception:
            continue
    return inserted_ids


def bulk_create_hydrodynamic_data(data_list: list) -> int:
    """
    批量创建水动力数据记录
    data_list格式: [{'point_id_db': ..., 'time_step': ..., 'h': ..., 'p': ..., 'u': ..., 'v': ...}, ...]
    """
    if not data_list:
        return 0

    with db.get_db_cursor() as (conn, cursor):
        cursor.executemany(
            """
            INSERT INTO hydrodynamic_data (point_id, time_step, h, p, u, v)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (d["point_id_db"], d["time_step"], d["h"], d["p"], d["u"], d["v"])
                for d in data_list
            ],
        )
        return cursor.rowcount
