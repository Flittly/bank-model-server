-- ========================================
-- Bank workflow schema (PostgreSQL + PostGIS) - 重构版
-- ========================================

CREATE EXTENSION IF NOT EXISTS postgis;

-- ========================================
-- 1. 岸段表 (banks)
-- 存储岸段的几何信息和基本属性
-- ========================================
CREATE TABLE IF NOT EXISTS banks (
    id SERIAL PRIMARY KEY,
    bank_id VARCHAR(100) UNIQUE NOT NULL,
    bank_name VARCHAR(100) NOT NULL,
    region_code VARCHAR(50) NOT NULL,
    
    -- 几何数据
    start_point GEOMETRY(Point, 4326),
    end_point GEOMETRY(Point, 4326),
    geom GEOMETRY(LineString, 4326),
    
    -- 岸段几何（用于前端展示）
    bank_geometry JSONB,
    
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_banks_id ON banks(bank_id);
CREATE INDEX IF NOT EXISTS idx_banks_region ON banks(region_code);
CREATE INDEX IF NOT EXISTS idx_banks_geom ON banks USING GIST(geom);

CREATE OR REPLACE FUNCTION update_banks_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS trigger_banks_updated_at ON banks;
CREATE TRIGGER trigger_banks_updated_at
BEFORE UPDATE ON banks
FOR EACH ROW EXECUTE FUNCTION update_banks_updated_at();

-- ========================================
-- 2. 任务表 (tasks)
-- 存储每次运行的任务信息
-- ========================================
CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(100) UNIQUE NOT NULL,
    task_name VARCHAR(100) NOT NULL,
    bank_ids JSONB,
    description TEXT,
    
    -- 模型运行状态
    status VARCHAR(20) DEFAULT 'pending',
    run_started_at TIMESTAMP,
    run_completed_at TIMESTAMP,
    error_message TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tasks_id ON tasks(task_id);

CREATE OR REPLACE FUNCTION update_tasks_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS trigger_tasks_updated_at ON tasks;
CREATE TRIGGER trigger_tasks_updated_at
BEFORE UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION update_tasks_updated_at();

-- ========================================
-- 2. 基础参数表 (basic_params)
-- 存储模型计算所需的所有基础参数
-- ========================================
CREATE TABLE IF NOT EXISTS basic_params (
    id SERIAL PRIMARY KEY,
    param_id VARCHAR(100) UNIQUE NOT NULL,
    param_name VARCHAR(100) NOT NULL,
    
    -- 基础参数
    segment VARCHAR(50),
    current_timepoint VARCHAR(20),
    set_name VARCHAR(50),
    water_qs VARCHAR(20),
    tidal_level VARCHAR(20),
    
    -- DEM参数
    bench_id VARCHAR(100),
    ref_id VARCHAR(100),
    
    -- 水深参数
    hs NUMERIC,
    hc NUMERIC,
    
    -- 防护控制参数
    protection_level VARCHAR(20),
    control_level VARCHAR(20),
    
    -- 对比时间点
    comparison_timepoint VARCHAR(20),
    
    -- 风险阈值
    risk_thresholds JSONB,
    
    -- 权重参数
    weights JSONB,
    
    -- 其他参数
    other_params JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_basic_params_id ON basic_params(param_id);

CREATE OR REPLACE FUNCTION update_basic_params_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS trigger_basic_params_updated_at ON basic_params;
CREATE TRIGGER trigger_basic_params_updated_at
BEFORE UPDATE ON basic_params
FOR EACH ROW EXECUTE FUNCTION update_basic_params_updated_at();

-- ========================================
-- 3. 断面表 (cross_sections) - 重构版
-- 存储断面信息，关联任务、岸段和基础参数
-- ========================================
CREATE TABLE IF NOT EXISTS cross_sections (
    id SERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL,
    section_id VARCHAR(100) UNIQUE NOT NULL,
    section_name VARCHAR(100) NOT NULL,
    bank_id VARCHAR(100) NOT NULL,
    region_code VARCHAR(50) NOT NULL,
    segment_index INTEGER,
    
    -- 几何数据
    start_point GEOMETRY(Point, 4326),
    end_point GEOMETRY(Point, 4326),
    geom GEOMETRY(LineString, 4326),
    
    -- 断面几何（用于前端展示）
    section_geometry JSONB,
    
    -- 距离参数（用于前端显示）
    distance NUMERIC,
    
    -- 关联的基础参数ID（用于参考，不依赖）
    basic_param_id INTEGER,
    
    -- 基础参数（每个section独立存储）
    param_name VARCHAR(100),
    segment VARCHAR(50),
    current_timepoint VARCHAR(20),
    set_name VARCHAR(50),
    water_qs VARCHAR(20),
    tidal_level VARCHAR(20),
    
    -- DEM参数
    bench_id VARCHAR(100),
    ref_id VARCHAR(100),
    
    -- 水深参数
    hs NUMERIC,
    hc NUMERIC,
    
    -- 防护控制参数
    protection_level VARCHAR(20),
    control_level VARCHAR(20),
    
    -- 对比时间点
    comparison_timepoint VARCHAR(20),
    
    -- 风险阈值
    risk_thresholds JSONB,
    
    -- 权重参数
    weights JSONB,
    
    -- 其他参数
    other_params JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    FOREIGN KEY (basic_param_id) REFERENCES basic_params(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_sections_task ON cross_sections(task_id);
CREATE INDEX IF NOT EXISTS idx_sections_bank ON cross_sections(bank_id);
CREATE INDEX IF NOT EXISTS idx_sections_region ON cross_sections(region_code);
CREATE INDEX IF NOT EXISTS idx_sections_geom ON cross_sections USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_sections_id ON cross_sections(section_id);

CREATE OR REPLACE FUNCTION update_cross_sections_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS trigger_cross_sections_updated_at ON cross_sections;
CREATE TRIGGER trigger_cross_sections_updated_at
BEFORE UPDATE ON cross_sections
FOR EACH ROW EXECUTE FUNCTION update_cross_sections_updated_at();



-- ========================================
-- 5. 风险等级结果表 (bank_risk_results)
-- ========================================
CREATE TABLE IF NOT EXISTS bank_risk_results (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(100) NOT NULL,
    section_id VARCHAR(100) NOT NULL,
    section_name VARCHAR(100),
    region_code VARCHAR(50),
    bank_id VARCHAR(100),
    run_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    risk_level INTEGER,
    indicators JSONB,
    geom GEOMETRY(LineString, 4326),
    
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (section_id) REFERENCES cross_sections(section_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_bank_results_section ON bank_risk_results(section_id);
CREATE INDEX IF NOT EXISTS idx_bank_results_task ON bank_risk_results(task_id);
CREATE INDEX IF NOT EXISTS idx_bank_results_region ON bank_risk_results(region_code);
CREATE INDEX IF NOT EXISTS idx_bank_results_geom ON bank_risk_results USING GIST(geom);

-- ========================================
-- 6. 水动力点表 (hydrodynamic_points)
-- 存储水动力数据的坐标点信息
-- ========================================
CREATE TABLE IF NOT EXISTS hydrodynamic_points (
    id SERIAL PRIMARY KEY,
    point_id VARCHAR(100) UNIQUE NOT NULL,
    region_code VARCHAR(50) NOT NULL,
    set_name VARCHAR(50) NOT NULL,
    water_qs VARCHAR(20) NOT NULL,
    tidal_level VARCHAR(20) NOT NULL,
    
    -- 坐标数据
    x NUMERIC NOT NULL,
    y NUMERIC NOT NULL,
    geom GEOMETRY(Point, 4326),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hydro_points_id ON hydrodynamic_points(point_id);
CREATE INDEX IF NOT EXISTS idx_hydro_points_region ON hydrodynamic_points(region_code);
CREATE INDEX IF NOT EXISTS idx_hydro_points_set ON hydrodynamic_points(set_name);
CREATE INDEX IF NOT EXISTS idx_hydro_points_water ON hydrodynamic_points(water_qs);
CREATE INDEX IF NOT EXISTS idx_hydro_points_tidal ON hydrodynamic_points(tidal_level);
CREATE INDEX IF NOT EXISTS idx_hydro_points_geom ON hydrodynamic_points USING GIST(geom);

CREATE OR REPLACE FUNCTION update_hydrodynamic_points_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS trigger_hydrodynamic_points_updated_at ON hydrodynamic_points;
CREATE TRIGGER trigger_hydrodynamic_points_updated_at
BEFORE UPDATE ON hydrodynamic_points
FOR EACH ROW EXECUTE FUNCTION update_hydrodynamic_points_updated_at();

-- ========================================
-- 7. 水动力数据表 (hydrodynamic_data)
-- 存储每个坐标点在不同时段的水动力数据
-- ========================================
CREATE TABLE IF NOT EXISTS hydrodynamic_data (
    id SERIAL PRIMARY KEY,
    point_id INTEGER NOT NULL,
    time_step INTEGER NOT NULL,
    
    -- 水动力参数
    h NUMERIC,
    p NUMERIC,
    u NUMERIC,
    v NUMERIC,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (point_id) REFERENCES hydrodynamic_points(id) ON DELETE CASCADE,
    UNIQUE(point_id, time_step)
);

CREATE INDEX IF NOT EXISTS idx_hydro_data_point ON hydrodynamic_data(point_id);
CREATE INDEX IF NOT EXISTS idx_hydro_data_timestep ON hydrodynamic_data(time_step);
