import os
import weakref

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover

    def load_dotenv(*args, **kwargs):
        return False


load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


APP_PORT = int(os.getenv("APP_PORT", "8088"))
APP_DEBUG = _env_flag("APP_DEBUG", True)

# API Version
API_VERSION = "/v0"

# API for Model Case
API_MC_DELETE = API_VERSION + "/mc"
API_MC_ERROR = API_VERSION + "/mc/error"
API_MC_STATUS = API_VERSION + "/mc/status"
API_MC_RESULT = API_VERSION + "/mc/result"
API_MC_PRE_ERROR_CASES = API_VERSION + "/mc/pre-error-cases"

API_MCS_DELETE = API_VERSION + "/mcs"
API_MCS_TIME = API_VERSION + "/mcs/time"
API_MCS_STATUS = API_VERSION + "/mcs/status"
API_MCS_SERIALIZATION = API_VERSION + "/mcs/serialization"

# API for File System
API_FS_DISK_USAGE = API_VERSION + "/fs/usage"
API_FS_RESULT_ZIP = API_VERSION + "/fs/result/zip"
API_FS_RESULT_FILE = API_VERSION + "/fs/result/file"
API_FS_RESOURCE_FILE = API_VERSION + "/fs/resource/file"
API_FS_RESOURCE_ZIP = API_VERSION + "/fs/resource/zip"
API_FS_RESOURCE_HYDRODYNAMIC_LIST = API_VERSION + "/fs/resource/hydrodynamic/list"

API_FS_RESOURCE_DELETE = API_VERSION + "/fs/resource"
API_FS_RESOURCE_ADF = API_VERSION + "/fs/resource/adf"
API_FS_RESOURCE_TIFF = API_VERSION + "/fs/resource/tiff"

API_FS_RESOURCE_JSON = API_VERSION + "/fs/resource/json"
API_FS_RESOURCE_GEOJSON = API_VERSION + "/fs/resource/geojson"
API_FS_RESOURCE_SHP = API_VERSION + "/fs/resource/shapefile"
API_FS_RESOURCE_HYDRODYNAMIC = API_VERSION + "/fs/resource/hydrodynamic"

# API for Model Runner
API_MR = API_VERSION + "/<category>/<model_name>"

# API for Reverbed Evolution
API_RE_REGION_FLUSH = API_VERSION + "/re/region-flush"
API_RE_SECTION_VIEW = API_VERSION + "/re/section-view"
API_RE_RIVER_VOLUME = API_VERSION + "/re/river-volume"
API_RE_FLUSH_CONTOUR = API_VERSION + "/re/region-contour"
API_RE_SECTION_CONTRAST = API_VERSION + "/re/section-contrast"

# API for Numerical Model
API_NM_HYDRODYNAMIC = API_VERSION + "/nm/hydrodynamic"
API_NM_REAL_HYDRODYNAMIC = API_VERSION + "/nm/real-hydrodynamic"
API_NM_FLOW_FIELD_VELOCITY = API_VERSION + "/nm/flow-field-velocity"

# API for Multi-Indicator Analysis
API_MI_RISK_LEVEL = API_VERSION + "/mi/risk-level"
API_MI_SLOPE_RATE = API_VERSION + "/mi/slope-rate"
API_MI_LOAD_CONTROL = API_VERSION + "/mi/load-control"
API_MI_NEARSHORE_FLUSH = API_VERSION + "/mi/nearshore-flush"
API_MI_FLOW_EQUIVALENT = API_VERSION + "/mi/flow-equivalent"
API_MI_SOIL_COMPOSITION = API_VERSION + "/mi/soil-composition"
API_MI_SLOPE_PROTECTION = API_VERSION + "/mi/slope-protection"
API_MI_ANTI_IMPACT_SPEED = API_VERSION + "/mi/anti-impact-speed"
API_MI_HEIGHT_DIFFERENCE = API_VERSION + "/mi/height-difference"
API_MI_WATER_LEVEL_FLUCTUATION = API_VERSION + "/mi/water-level-fluctuation"
API_EM_BSTEM = API_VERSION + "/em/bstem"

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "bank_risk_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "123456")

# Status Flag
STATUS_UNLOCK = 0b1
STATUS_LOCK = 0b10
STATUS_RUNNING = 0b100
STATUS_COMPLETE = 0b1000
STATUS_NONE = 0b10000
STATUS_ERROR = 0b100000

# Directory Setting
DIR_ROOT = os.path.dirname(os.path.abspath(__file__))
DIR_MODEL_CASE = os.path.join(DIR_ROOT, "case")
DIR_MODEL = os.path.join(DIR_ROOT, "model")
DIR_TRIGGER_RESOURCE = os.path.join(DIR_ROOT, "modelResource")
DIR_RESOURCE = os.path.join(DIR_ROOT, "resource")
DIR_RESOURCE_CACHE = os.path.join(DIR_RESOURCE, "cache")
DIR_STORAGE_LOG = os.path.join(DIR_ROOT, "resource", "storage", "log.txt")
DIR_RESOURCE_SHP = os.path.join(DIR_ROOT, "resource", "shp")
DIR_RESOURCE_ADF = os.path.join(DIR_ROOT, "resource", "adf")
DIR_RESOURCE_JSON = os.path.join(DIR_ROOT, "resource", "json")
DIR_RESOURCE_TIFF = os.path.join(DIR_ROOT, "resource", "tiff")
DIR_RESOURCE_MODEL = os.path.join(DIR_ROOT, "resource", "model")
DIR_RESOURCE_GEOJSON = os.path.join(DIR_ROOT, "resource", "geojson")
DIR_RESOURCE_HYDRODYNAMIC = os.path.join(DIR_ROOT, "resource", "hydrodynamic")
DIR_RESOURCE_EROSIONMODEL = os.path.join(DIR_ROOT, "resource", "erosionModel")

DIR_RESOURCE_PQ_TEMPLATE = os.path.join(
    DIR_ROOT, "resource", "json", "Mzs", "2023", "standard", "PQ", "pq.json"
)
DIR_RESOURCE_RISKLEVEL_THRESHOLD_TEMPLATE = os.path.join(
    DIR_ROOT,
    "resource",
    "json",
    "Mzs",
    "2023",
    "standard",
    "RiskLevel",
    "template.json",
)

DIR_GLOBALE_FILE_LOCKER = os.path.join(DIR_MODEL_CASE, "lock")


MODEL_REGISTRY = {
    API_EM_BSTEM: "erosionModel/erosionModel.py",
    API_NM_HYDRODYNAMIC: "numericalModel/hydrodynamic.py",
    API_NM_REAL_HYDRODYNAMIC: "numericalModel/realHydrodynamic.py",
    API_NM_FLOW_FIELD_VELOCITY: "numericalModel/flowFieldVelocity.py",
    API_RE_RIVER_VOLUME: "riverbedEvolution/riverVolume.py",
    API_RE_REGION_FLUSH: "riverbedEvolution/regionFlush.py",
    API_RE_SECTION_VIEW: "riverbedEvolution/sectionView.py",
    API_RE_FLUSH_CONTOUR: "riverbedEvolution/flushContour.py",
    API_RE_SECTION_CONTRAST: "riverbedEvolution/sectionContrast.py",
    API_MI_SLOPE_RATE: "multipleIndicators/slopeRate.py",
    API_MI_RISK_LEVEL: "multipleIndicators/riskLevel.py",
    API_MI_LOAD_CONTROL: "multipleIndicators/loadControl.py",
    API_MI_NEARSHORE_FLUSH: "multipleIndicators/nearshoreFlush.py",
    API_MI_FLOW_EQUIVALENT: "multipleIndicators/flowEquivalent.py",
    API_MI_SLOPE_PROTECTION: "multipleIndicators/slopeProtection.py",
    API_MI_SOIL_COMPOSITION: "multipleIndicators/soilComposition.py",
    API_MI_ANTI_IMPACT_SPEED: "multipleIndicators/antiImpactSpeed.py",
    API_MI_HEIGHT_DIFFERENCE: "multipleIndicators/heightDifference.py",
    API_MI_WATER_LEVEL_FLUCTUATION: "multipleIndicators/waterLevelFluctuation.py",
}

APP_ALLOWED_EXTENSIONS = {"zip"}

APP_IGNORE_THINGS = [".DS_Store", "__MACOSX"]

CAL_CORE_NUM = 5
MAX_RUNNING_MODEL_CASE_NUM = 20
MODEL_SERVICE_POLL_INTERVAL = 1
MODEL_SERVICE_DEFAULT_TIMEOUT = 600

# Kafka 配置
KAFKA_ENABLED = _env_flag("KAFKA_ENABLED", False)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(
    ","
)
KAFKA_TASK_TOPIC = os.getenv("KAFKA_TASK_TOPIC", "bank.model.task")  # 与 Java 端一致
KAFKA_RESULT_TOPIC = os.getenv("KAFKA_RESULT_TOPIC", "bank.model.result.v1")
KAFKA_WORKER_GROUP = os.getenv("KAFKA_WORKER_GROUP", "bank-model-workers")
KAFKA_WORKER_ID = os.getenv("KAFKA_WORKER_ID", "python-worker-01")
# Worker 负责的岸段 ID（逗号分隔，支持多个岸段）
# 例如：BANK_001,BANK_002
# 如果为空，则不指定岸段，由 Kafka 分配
KAFKA_ASSIGNED_BANKS = (
    os.getenv("KAFKA_ASSIGNED_BANKS", "").split(",")
    if os.getenv("KAFKA_ASSIGNED_BANKS")
    else []
)

# RustFS 配置
RUSTFS_ENABLED = _env_flag("RUSTFS_ENABLED", True)
RUSTFS_ENDPOINT = os.getenv("RUSTFS_ENDPOINT", "http://127.0.0.1:9000")
RUSTFS_BUCKET = os.getenv("RUSTFS_BUCKET", "yangtze-bank-warning-system")
RUSTFS_ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin")
RUSTFS_SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY", "rustfsadmin")
RUSTFS_REGION = os.getenv("RUSTFS_REGION", "us-east-1")
RUSTFS_SECURE = _env_flag("RUSTFS_SECURE", False)

# 地形数据缓存目录
DIR_TERRAIN_CACHE = os.path.join(DIR_ROOT, "cache", "terrain")
