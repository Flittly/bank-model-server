# 数据库表结构设计说明

## 概述

本文档描述了岸段、断面、修正线、风险等级结果四个核心表的设计，以及它们之间的关系。

- **banks (岸段表)**: 存储岸段的几何信息
- **tasks (任务表)**: 存储任务信息，管理断面和风险结果
- **basic_params (基础参数表)**: 存储参数模板，供断面复用
- **cross_sections (断面表)**: 存储断面信息，每个断面独立存储完整的模型参数
- **bank_risk_results (风险等级结果表)**: 存储按照断面计算的风险等级结果

## 表关系图

```
┌─────────────────┐         ┌─────────────────┐
│      banks      │         │     tasks       │
│    (岸段表)      │         │    (任务表)     │
└─────────────────┘         └────────┬────────┘
         │                           │
         │ 1:N                       │ 1:N
         ▼                           ▼
┌─────────────────┐         ┌─────────────────┐
│ cross_sections  │◀────────│  basic_params   │
│   (横断面表)    │         │  (基础参数表)   │
└─────────────────┘         └─────────────────┘
         │
         │ 1:N
         ▼
┌─────────────────┐
│bank_risk_results│
│  (风险结果表)   │
└─────────────────┘
```

## 表结构详情

### 1. banks (岸段表)

**说明**: 存储岸段的几何信息和基本属性。

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | SERIAL | 主键 | PRIMARY KEY |
| bank_id | VARCHAR(100) | 岸段ID | UNIQUE, NOT NULL |
| bank_name | VARCHAR(100) | 岸段名称 | NOT NULL |
| region_code | VARCHAR(50) | 区域代码 | NOT NULL |
| start_point | GEOMETRY(Point, 4326) | 起点坐标 | - |
| end_point | GEOMETRY(Point, 4326) | 终点坐标 | - |
| geom | GEOMETRY(LineString, 4326) | 岸段几何线 | - |
| bank_geometry | JSONB | 岸段几何（用于前端展示） | - |
| description | TEXT | 描述 | - |
| created_at | TIMESTAMP | 创建时间 | DEFAULT CURRENT_TIMESTAMP |
| updated_at | TIMESTAMP | 更新时间 | DEFAULT CURRENT_TIMESTAMP |

**索引**:
- idx_banks_id: bank_id
- idx_banks_region: region_code
- idx_banks_geom: geom (GIST)

---

### 2. tasks (任务表)

**说明**: 存储每次运行的任务信息，是重构后的核心表。

```sql
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
```

#### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | VARCHAR(100) | 任务唯一标识 |
| `task_name` | VARCHAR(100) | 任务名称 |
| `bank_ids` | JSONB | 岸段ID列表（JSON数组） |
| `description` | TEXT | 任务描述 |
| `status` | VARCHAR(20) | 任务运行状态（pending/running/completed/error） |
| `run_started_at` | TIMESTAMP | 运行开始时间 |
| `run_completed_at` | TIMESTAMP | 运行完成时间 |
| `error_message` | TEXT | 错误信息（status为error时使用） |

> 💡 **Tip**: 
> - `bank_ids` 用 JSONB 存储岸段ID列表
> - **新增状态字段**：可以跟踪整个任务的运行状态，前端可以查询任务的整体进度
> - **状态值**：pending（等待）、running（运行中）、completed（完成）、error（错误）

---

### 3. basic_params (基础参数表)

**说明**: 存储模型计算所需的所有基础参数，可以被多个断面复用。

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | SERIAL | 主键 | PRIMARY KEY |
| param_id | VARCHAR(100) | 参数唯一ID | UNIQUE, NOT NULL |
| param_name | VARCHAR(100) | 参数名称 | NOT NULL |
| segment | VARCHAR(50) | 河段编码 | - |
| current_timepoint | VARCHAR(20) | 当前时间点 | - |
| set_name | VARCHAR(50) | 数据集名称 | - |
| water_qs | VARCHAR(20) | 流量 | - |
| tidal_level | VARCHAR(20) | 潮位 | - |
| bench_id | VARCHAR(100) | 基准DEM ID | - |
| ref_id | VARCHAR(100) | 参考DEM ID | - |
| hs | NUMERIC | 水深参数1 | - |
| hc | NUMERIC | 水深参数2 | - |
| protection_level | VARCHAR(20) | 防护等级 | - |
| control_level | VARCHAR(20) | 控制等级 | - |
| comparison_timepoint | VARCHAR(20) | 对比时间点 | - |
| risk_thresholds | JSONB | 风险阈值 | - |
| weights | JSONB | 权重参数 | - |
| other_params | JSONB | 其他参数 | - |
| created_at | TIMESTAMP | 创建时间 | DEFAULT CURRENT_TIMESTAMP |
| updated_at | TIMESTAMP | 更新时间 | DEFAULT CURRENT_TIMESTAMP |

**索引**:
- idx_basic_params_id: param_id

---

### 4. cross_sections (横断面表 - 重构版)

**说明**: 存储垂直于岸段的断面信息，关联岸段并继承属性参数。断面是模型主要计算的对象。**每个断面独立存储完整的模型运行参数！**

**重要说明**: 
- `multipleIndicators` 模型计算出的坡度(Sa)、高差(Zb)、近岸冲刷(Ln)、抗冲击流速(Ky)、水位波动(Zd)等**不需要存储在数据库中**，由模型实时计算并存储在模型实例的响应中

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | SERIAL | 主键 | PRIMARY KEY |
| task_id | INTEGER | 外键 → tasks.id | NOT NULL |
| section_id | VARCHAR(100) | 断面唯一ID | UNIQUE, NOT NULL |
| section_name | VARCHAR(100) | 断面名称 | NOT NULL |
| bank_id | VARCHAR(100) | 岸段ID | NOT NULL |
| region_code | VARCHAR(50) | 区域代码 | NOT NULL |
| segment_index | INTEGER | 岸段序号 | - |
| start_point | GEOMETRY(Point, 4326) | 起点坐标（岸上） | - |
| end_point | GEOMETRY(Point, 4326) | 终点坐标（河里） | - |
| geom | GEOMETRY(LineString, 4326) | 断面几何线 | - |
| section_geometry | JSONB | 断面几何（用于前端展示） | - |
| distance | NUMERIC | 距离参数（用于前端显示） | - |
| basic_param_id | INTEGER | 外键 → basic_params.id（可选，仅用于参考） | - |

-- 基础参数（每个section独立存储）
| param_name | VARCHAR(100) | 参数名称 | - |
| segment | VARCHAR(50) | 河段编码 | - |
| current_timepoint | VARCHAR(20) | 当前时间点 | - |
| set_name | VARCHAR(50) | 数据集名称 | - |
| water_qs | VARCHAR(20) | 流量 | - |
| tidal_level | VARCHAR(20) | 潮位 | - |
| bench_id | VARCHAR(100) | 基准DEM ID | - |
| ref_id | VARCHAR(100) | 参考DEM ID | - |
| hs | NUMERIC | 砂土层厚度（米） | - |
| hc | NUMERIC | 土层总覆盖厚度（米） | - |
| protection_level | VARCHAR(20) | 岸坡防护等级（继承自岸段，可被修正线修改） | - |
| control_level | VARCHAR(20) | 荷载控制等级（继承自岸段，可被修正线修改） | - |
| comparison_timepoint | VARCHAR(20) | 对比时间点 | - |
| risk_thresholds | JSONB | 风险阈值配置 | - |
| weights | JSONB | 权重配置 | - |
| other_params | JSONB | 其他参数 | - |

| created_at | TIMESTAMP | 创建时间 | DEFAULT CURRENT_TIMESTAMP |
| updated_at | TIMESTAMP | 更新时间 | DEFAULT CURRENT_TIMESTAMP |

**索引**:
- idx_sections_task: task_id
- idx_sections_bank: bank_id
- idx_sections_region: region_code
- idx_sections_geom: geom (GIST)
- idx_sections_id: section_id

**外键关系**:
- task_id → tasks(id) (ON DELETE CASCADE)
- basic_param_id → basic_params(id) (ON DELETE SET NULL)

> 💡 **Tip**: 
> - 注意 `task_id` 是外键，关联到 `tasks(id)`。这建立了**任务-断面**的一对多关系，一个任务可以有多个横断面。
> - `bank_id` 关联到 `banks.bank_id`，建立**岸段-断面**的一对多关系
> - `basic_param_id` 可选地关联到基础参数表，用于参考和参数复制
> - **核心变化：每个断面独立存储完整的模型运行参数！**
> - 创建断面时，如果提供了 `basic_param_id`，后端会自动从 `basic_params` 表复制参数到 `cross_sections` 表
> - 修改断面的参数时，直接修改 `cross_sections` 表中的字段，**不会影响 `basic_params` 表**

---

### 5. bank_risk_results (风险结果表)

**说明**: 存储按照断面计算的风险等级结果。

| 字段名 | 类型 | 说明 | 约束 |
|--------|------|------|------|
| id | SERIAL | 主键 | PRIMARY KEY |
| task_id | VARCHAR(100) | 外键 → tasks.task_id | NOT NULL |
| section_id | VARCHAR(100) | 外键 → cross_sections.section_id | NOT NULL |
| section_name | VARCHAR(100) | 断面名称 | - |
| region_code | VARCHAR(50) | 区域代码 | - |
| bank_id | VARCHAR(100) | 岸段ID | - |
| run_time | TIMESTAMP | 运行时间 | DEFAULT CURRENT_TIMESTAMP |
| risk_level | INTEGER | 风险等级 | - |
| indicators | JSONB | 子指标详情 (JSON) | - |
| geom | GEOMETRY(LineString, 4326) | 空间几何 (复制自断面) | - |

**索引**:
- idx_bank_results_section: section_id
- idx_bank_results_task: task_id
- idx_bank_results_region: region_code
- idx_bank_results_geom: geom (GIST)

**外键关系**:
- task_id → tasks(task_id) (ON DELETE CASCADE)
- section_id → cross_sections(section_id) (ON DELETE CASCADE)

---

## 使用场景

### 场景1: 创建岸段数据

```sql
INSERT INTO banks (
    bank_id, bank_name, region_code,
    start_point, end_point, geom,
    bank_geometry, description
) VALUES (
    'BANK_001', '岸段001', 'Mzs',
    ST_SetSRID(ST_MakePoint(121.5, 31.2), 4326),
    ST_SetSRID(ST_MakePoint(121.6, 31.3), 4326),
    ST_SetSRID(ST_MakeLine(ST_MakePoint(121.5, 31.2), ST_MakePoint(121.6, 31.3)), 4326),
    '{"type": "LineString", ...}',
    '这是一个示例岸段'
);
```

---

### 场景2: 创建基础参数模板

```sql
INSERT INTO basic_params (
    param_id, param_name,
    segment, current_timepoint, set_name,
    water_qs, tidal_level,
    bench_id, ref_id,
    hs, hc,
    protection_level, control_level,
    comparison_timepoint,
    risk_thresholds, weights
) VALUES (
    'PARAM_001', '默认参数模板',
    'Mzs', '2024-01-15', 'standard',
    '45000', 'zc',
    'dem_2024.tif', 'dem_2020.tif',
    0.5, 2.0,
    'systemic', 'strict',
    '2020-01-15',
    '{"Dsed": [0.3, 0.5, 0.7], ...}',
    '{"wRE": [0.3, 0.4, 0.3], ...}'
);
```

---

### 场景3: 创建断面（从参数模板复制参数）

```sql
-- 方法1: 直接插入，从basic_params复制参数
-- 注意：这在代码中自动处理，不是直接SQL操作
```

---

### 场景4: 查询岸段及其关联的断面

```sql
SELECT 
    b.bank_id,
    b.bank_name,
    COUNT(cs.id) as section_count
FROM banks b
LEFT JOIN cross_sections cs ON b.bank_id = cs.bank_id
GROUP BY b.id, b.bank_id, b.bank_name;
```

---

### 场景5: 保存风险等级结果

```sql
-- 模型计算完成后，将断面级别的风险等级存入结果表
INSERT INTO bank_risk_results (
    task_id, section_id, section_name,
    region_code, bank_id,
    risk_level, indicators, geom
) SELECT 
    1, cs.id, cs.section_name,
    cs.region_code, cs.bank_id,
    3, 
    '{"slope_risk": 2, "flow_risk": 3, "erosion_risk": 4}'::jsonb,
    cs.geom
FROM cross_sections cs
WHERE cs.section_id = 'SEC_001';
```

---

### 场景6: 查询断面的风险等级结果

```sql
SELECT 
    cs.section_id,
    cs.section_name,
    b.bank_name,
    br.risk_level,
    br.indicators,
    br.run_time
FROM bank_risk_results br
JOIN cross_sections cs ON br.section_id = cs.id
JOIN banks b ON cs.bank_id = b.bank_id
WHERE br.task_id = 1
ORDER BY br.risk_level DESC;
```

---

## 数据流程

1. **初始化阶段**:
   - 前端绘制岸段 → 插入 banks
   - 前端创建参数模板 → 插入 basic_params

2. **任务创建阶段**:
   - 前端创建任务 → 插入 tasks
   - 前端绘制断面 → 插入 cross_sections（从basic_params复制参数）

3. **参数修改阶段**:
   - 前端修改参数 → 直接更新 cross_sections（不影响basic_params）

4. **模型运行阶段**:
   - 调用 POST /bank/tasks/{task_id}/run
   - 从 cross_sections 获取所有断面参数
   - 将断面数据传递给模型进行运算
   - 结果存入 bank_risk_results
   - 自动更新 tasks 状态

---

## 执行SQL

使用以下命令执行表创建脚本：

```bash
# 方法1: 使用psql命令行
psql -h localhost -p 5432 -U postgres -d bank_risk_db -f database_schema.sql

# 方法2: 使用Python脚本（需要先安装依赖）
python util/db_init.py

# 方法3: 在数据库管理工具中直接执行
# 打开 database_schema.sql 文件，复制SQL语句在PgAdmin或DBeaver中执行
```

---

## 注意事项

1. **几何坐标系**: 所有几何数据使用 WGS84 (SRID 4326)
2. **外键约束**: 
   - 断面表的外键设置了 ON DELETE CASCADE，删除任务会自动删除关联的断面
   - 风险结果表的外键设置了 ON DELETE CASCADE，删除断面会自动删除关联的风险结果
3. **索引优化**: 为空间查询和常用查询字段创建了索引
4. **JSONB使用**: other_params、risk_thresholds、weights 使用 JSONB 类型，支持高效的JSON查询
5. **时间戳**: 自动记录创建和更新时间，便于追踪数据变更
6. **风险等级**: 风险等级是按照断面计算和存储的，每个断面对应一个风险等级
7. **参数独立存储**:
   - 每个section都独立存储完整的模型参数
   - 创建section时可以从basic_params复制参数
   - 修改section的参数只影响该section，不影响basic_params表
8. **任务状态跟踪**:
   - tasks表新增status字段，跟踪整个任务的运行状态
   - 状态值：pending（等待）、running（运行中）、completed（完成）、error（错误）
   - 记录运行开始和完成时间
   - 记录错误信息
9. **模型计算参数说明**:
   - **multipleIndicators 模型**: 复杂多指标风险评估模型
   - 坡度(Sa)、高差(Zb)、近岸冲刷(Ln)、抗冲击流速(Ky)、水位波动(Zd)等参数**由模型实时计算得出**
   - 这些计算参数**不需要存储在数据库中**，由模型实时计算并存储在模型实例的响应中
10. **前端提供的参数**:
    - DEM TIFF 文件（存储在 resource/tiff/ 目录）
    - 土壤参数（hs、hc）
    - 地质工程参数（protection_level、control_level）
    - 地形参数（通过选择 DEM 文件）
    - 水文参数（segment、current_timepoint、set_name、water_qs、tidal_level）
    - 断面几何参数（通过前端绘制）
    - 风险阈值和指标权重（risk_thresholds、weights）

---

## 后续扩展

如需添加新字段或表，请参考现有结构并保持一致性：
- 使用 SERIAL 作为主键
- 使用 GEOMETRY 类型存储空间数据
- 为外键添加适当的约束
- 为查询频繁的字段创建索引
- 添加必要的注释说明字段用途
