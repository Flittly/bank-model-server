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
    user_id: Optional[int] = None,
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
        # 获取 admin 用户的 ID 作为默认归属
        if user_id is None:
            cursor.execute("SELECT id FROM users WHERE username = 'admin'")
            row = cursor.fetchone()
            user_id = row[0] if row else 1

        cursor.execute(
            """
            INSERT INTO basic_params (
                param_id, param_name, user_id,
                segment, current_timepoint, set_name, water_qs, tidal_level,
                bench_id, ref_id,
                hs, hc, protection_level, control_level,
                comparison_timepoint,
                risk_thresholds, weights, other_params
            ) VALUES (
                %s, %s, %s,
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
                user_id,
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


def get_cross_sections(
    task_id: Optional[str] = None,
    bank_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Get all cross sections (重构版 - 每个section独立存储参数)
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        query = """
            SELECT 
                cs.*,
                t.task_name,
                ST_AsGeoJSON(cs.geom)::jsonb as geometry
            FROM cross_sections cs
            JOIN tasks t ON cs.task_id = t.task_id
        """
        params = []

        conditions = []
        if task_id is not None:
            conditions.append("cs.task_id = %s")
            params.append(task_id)
        if bank_id is not None:
            conditions.append("cs.bank_id = %s")
            params.append(bank_id)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY cs.id"

        cursor.execute(query, tuple(params))
        return cursor.fetchall()


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


def get_hydrodynamic_points(
    region_code: Optional[str] = None,
    set_name: Optional[str] = None,
    water_qs: Optional[str] = None,
    tidal_level: Optional[str] = None,
    temp: Optional[bool] = None,
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
        if temp is not None:
            conditions.append("hp.temp = %s")
            params.append(temp)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY hp.id"

        cursor.execute(query, tuple(params))
        return cursor.fetchall()


def get_available_hydrodynamic_nodes(
    region_code: str,
    set_name: str,
    tidal_level: str,
) -> List[int]:
    """
    Get available non-temp hydrodynamic water_qs nodes for a given tidal level.
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT DISTINCT water_qs
            FROM hydrodynamic_points
            WHERE region_code = %s
              AND set_name = %s
              AND tidal_level = %s
              AND temp = FALSE
            """,
            (region_code, set_name, tidal_level),
        )
        rows = cursor.fetchall()
        return sorted((int(row["water_qs"]) for row in rows))


def get_nearest_hydrodynamic_point(
    region_code: str,
    set_name: str,
    water_qs: str,
    tidal_level: str,
    x: float,
    y: float,
) -> Optional[Dict[str, Any]]:
    """
    Get the nearest non-temp hydrodynamic point for a condition.
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT
                hp.*,
                ST_AsGeoJSON(hp.geom)::jsonb as geometry,
                ((hp.x - %s) * (hp.x - %s) + (hp.y - %s) * (hp.y - %s)) AS distance_sq
            FROM hydrodynamic_points hp
            WHERE hp.region_code = %s
              AND hp.set_name = %s
              AND hp.water_qs = %s
              AND hp.tidal_level = %s
              AND hp.temp = FALSE
            ORDER BY distance_sq ASC, hp.id ASC
            LIMIT 1
            """,
            (x, x, y, y, region_code, set_name, water_qs, tidal_level),
        )
        result = cursor.fetchone()
        return result if result else None


def get_hydrodynamic_series(point_id_db: int) -> List[Dict[str, Any]]:
    """
    Get full hydrodynamic time series for a point.
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT time_step, h, p, u, v
            FROM hydrodynamic_data
            WHERE point_id = %s
            ORDER BY time_step
            """,
            (point_id_db,),
        )
        return cursor.fetchall()


def get_tiff_bounds(tiff_key: str) -> Optional[Dict[str, Any]]:
    """
    从数据库获取 TIFF 边界信息

    Args:
        tiff_key: TIFF 文件的资源键

    Returns:
        边界信息字典，如果不存在返回 None
    """
    with db.get_db_cursor(dict_cursor=True) as (conn, cursor):
        cursor.execute(
            """
            SELECT tiff_key, region_code, year, timepoint,
                   min_x, min_y, max_x, max_y,
                   ST_AsText(geom) as geom_wkt
            FROM tiff_bounds
            WHERE tiff_key = %s
            """,
            (tiff_key,),
        )
        row = cursor.fetchone()
        if row:
            return {
                "tiff_key": row["tiff_key"],
                "region_code": row["region_code"],
                "year": row["year"],
                "timepoint": row["timepoint"],
                "min_x": float(row["min_x"]),
                "min_y": float(row["min_y"]),
                "max_x": float(row["max_x"]),
                "max_y": float(row["max_y"]),
                "geom_wkt": row["geom_wkt"],
            }
        return None


def save_tiff_bounds(
    tiff_key: str,
    region_code: Optional[str],
    year: Optional[str],
    timepoint: Optional[str],
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
    srid: int = 3857,
    user_id: Optional[int] = None,
) -> int:
    """
    保存 TIFF 边界信息到数据库

    Args:
        tiff_key: TIFF 文件的资源键
        region_code: 区域代码
        year: 年份
        timepoint: 时间点
        min_x: 最小 X 坐标
        min_y: 最小 Y 坐标
        max_x: 最大 X 坐标
        max_y: 最大 Y 坐标
        srid: 原始坐标系的 SRID

    Returns:
        插入记录的 ID
    """
    # 构建 WKT 多边形
    geom_wkt = f"POLYGON(({min_x} {min_y}, {max_x} {min_y}, {max_x} {max_y}, {min_x} {max_y}, {min_x} {min_y}))"

    with db.get_db_cursor() as (conn, cursor):
        if user_id is None:
            cursor.execute("SELECT id FROM users WHERE username = 'admin'")
            row = cursor.fetchone()
            user_id = row[0] if row else 1

        cursor.execute(
            """
            INSERT INTO tiff_bounds (tiff_key, region_code, year, timepoint,
                                     min_x, min_y, max_x, max_y, geom, user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 
                    ST_Transform(ST_SetSRID(ST_GeomFromText(%s), %s), 4326), %s)
            ON CONFLICT (tiff_key) DO UPDATE SET
                region_code = EXCLUDED.region_code,
                year = EXCLUDED.year,
                timepoint = EXCLUDED.timepoint,
                min_x = EXCLUDED.min_x,
                min_y = EXCLUDED.min_y,
                max_x = EXCLUDED.max_x,
                max_y = EXCLUDED.max_y,
                geom = EXCLUDED.geom,
                user_id = EXCLUDED.user_id,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            """,
            (
                tiff_key,
                region_code,
                year,
                timepoint,
                min_x,
                min_y,
                max_x,
                max_y,
                geom_wkt,
                srid,
                user_id,
            ),
        )
        result = cursor.fetchone()
        return result[0] if result else None


