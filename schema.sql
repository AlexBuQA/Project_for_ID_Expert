-- =============================================================================
--  Универсальная платформа Телематики — схема базы данных (PostgreSQL 15+ / TimescaleDB)
--  Разработчик: Бужор Александра.  Заказчик: «ID Expert» (Априорные решения машин).
--
--  Принципы:
--   * Универсальность: любые типы техники описываются справочниками, без ALTER TABLE.
--   * Расширяемость: вариативные атрибуты вынесены в JSONB.
--   * Масштабируемость: metrics/events партиционируются по времени; hot/warm/cold слои.
--   * Скорость: таблица vehicle_state кэширует последнее состояние для карты на 1000+ ТС.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";        -- геозоны и пространственные запросы
-- CREATE EXTENSION IF NOT EXISTS timescaledb;    -- включается при использовании TimescaleDB

-- -----------------------------------------------------------------------------
-- 1. СПРАВОЧНИКИ (реестр)
-- -----------------------------------------------------------------------------

CREATE TABLE vehicle_type (
    id            SMALLSERIAL PRIMARY KEY,
    code          TEXT        NOT NULL UNIQUE,          -- forklift_ev, tractor_ice, robot_agv
    name          TEXT        NOT NULL,
    category      TEXT        NOT NULL CHECK (category IN ('tractor','forklift','cart','robot')),
    powertrain    TEXT        NOT NULL CHECK (powertrain IN ('ice','ev','hybrid','none')),
    is_autonomous BOOLEAN     NOT NULL DEFAULT FALSE,
    attributes    JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE sensor_type (
    id          SERIAL PRIMARY KEY,
    code        TEXT   NOT NULL UNIQUE,                 -- engine.rpm, battery.soc, gps.location
    name        TEXT   NOT NULL,
    unit        TEXT,
    value_type  TEXT   NOT NULL CHECK (value_type IN ('numeric','boolean','text','json','geo')),
    min_value   DOUBLE PRECISION,
    max_value   DOUBLE PRECISION,
    category    TEXT   NOT NULL CHECK (category IN ('common','ice','ev','avts','specific')),
    description TEXT
);

CREATE TABLE event_type (
    id               SERIAL PRIMARY KEY,
    code             TEXT NOT NULL UNIQUE,              -- overspeed, geofence_exit, overheat
    name             TEXT NOT NULL,
    category         TEXT NOT NULL,                     -- driving, technical, security, operational
    default_severity TEXT NOT NULL CHECK (default_severity IN ('info','warning','critical')),
    description      TEXT
);

-- -----------------------------------------------------------------------------
-- 2. РЕЕСТР ТС И ДАТЧИКОВ
-- -----------------------------------------------------------------------------

CREATE TABLE vehicle (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_id         TEXT NOT NULL UNIQUE,           -- FRK-014 (бизнес-идентификатор)
    vehicle_type_id     SMALLINT NOT NULL REFERENCES vehicle_type(id),
    model               TEXT,
    serial_number       TEXT,
    registration_number TEXT,
    status              TEXT NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active','idle','offline','maintenance','decommissioned')),
    commissioned_at     TIMESTAMPTZ,
    attributes          JSONB NOT NULL DEFAULT '{}'::jsonb,  -- site, owner_dept, capacity …
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_vehicle_type   ON vehicle (vehicle_type_id);
CREATE INDEX ix_vehicle_status ON vehicle (status);
CREATE INDEX ix_vehicle_attrs  ON vehicle USING GIN (attributes);   -- поиск по JSONB

-- Какие датчики реально установлены на конкретном ТС (для валидации и калибровки)
CREATE TABLE vehicle_sensor (
    id             BIGSERIAL PRIMARY KEY,
    vehicle_id     UUID NOT NULL REFERENCES vehicle(id) ON DELETE CASCADE,
    sensor_type_id INT  NOT NULL REFERENCES sensor_type(id),
    hardware_id    TEXT,
    is_active      BOOLEAN NOT NULL DEFAULT TRUE,
    calibration    JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (vehicle_id, sensor_type_id)
);

-- -----------------------------------------------------------------------------
-- 3. ТЕЛЕМЕТРИЯ (metrics) — «длинная» таблица временных рядов
--    Одна строка = одно измерение одного датчика.
--    Добавление нового параметра = новая запись в sensor_type, БЕЗ миграции схемы.
--    Партиционирование по времени (декларативное). При TimescaleDB — hypertable.
-- -----------------------------------------------------------------------------

CREATE TABLE metric (
    time           TIMESTAMPTZ      NOT NULL,
    vehicle_id     UUID             NOT NULL,
    sensor_type_id INT              NOT NULL,
    value_num      DOUBLE PRECISION,
    value_bool     BOOLEAN,
    value_text     TEXT,
    value_json     JSONB,                               -- для geo/составных значений
    quality        SMALLINT         NOT NULL DEFAULT 1, -- 1 ок, 0 подозрит., -1 ошибка
    received_at    TIMESTAMPTZ      NOT NULL DEFAULT now()
) PARTITION BY RANGE (time);

-- Составной индекс под основной паттерн запроса: «датчик X по ТС Y за период»
CREATE INDEX ix_metric_vehicle_sensor_time
    ON metric (vehicle_id, sensor_type_id, time DESC);
-- BRIN — компактный индекс по времени для сканов больших диапазонов
CREATE INDEX ix_metric_time_brin ON metric USING BRIN (time);

-- Пример помесячных секций (в проде создаются автоматически: pg_partman / TimescaleDB)
CREATE TABLE metric_2026_06 PARTITION OF metric
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE metric_2026_07 PARTITION OF metric
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

-- --- Вариант TimescaleDB (вместо ручных секций) ------------------------------
-- SELECT create_hypertable('metric','time', chunk_time_interval => INTERVAL '1 day');
-- ALTER TABLE metric SET (timescaledb.compress,
--                         timescaledb.compress_segmentby = 'vehicle_id, sensor_type_id');
-- SELECT add_compression_policy('metric', INTERVAL '7 days');   -- warm-слой
-- SELECT add_retention_policy  ('metric', INTERVAL '90 days');  -- выгрузка в cold (S3)
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- 4. СОБЫТИЯ (events) — партиционирование по времени
-- -----------------------------------------------------------------------------

CREATE TABLE event (
    id            UUID        NOT NULL DEFAULT uuid_generate_v4(),
    time          TIMESTAMPTZ NOT NULL,
    vehicle_id    UUID        NOT NULL REFERENCES vehicle(id),
    event_type_id INT         NOT NULL REFERENCES event_type(id),
    severity      TEXT        NOT NULL CHECK (severity IN ('info','warning','critical')),
    geo_lat       DOUBLE PRECISION,
    geo_lon       DOUBLE PRECISION,
    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,   -- value, threshold, geofence_id …
    acknowledged  BOOLEAN     NOT NULL DEFAULT FALSE,
    ack_by        TEXT,
    ack_at        TIMESTAMPTZ,
    PRIMARY KEY (id, time)
) PARTITION BY RANGE (time);

CREATE INDEX ix_event_vehicle_time ON event (vehicle_id, time DESC);
CREATE INDEX ix_event_type_time    ON event (event_type_id, time DESC);
CREATE INDEX ix_event_open         ON event (acknowledged) WHERE acknowledged = FALSE;
CREATE INDEX ix_event_payload      ON event USING GIN (payload);

CREATE TABLE event_2026_06 PARTITION OF event
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE event_2026_07 PARTITION OF event
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

-- -----------------------------------------------------------------------------
-- 5. ТЕКУЩЕЕ СОСТОЯНИЕ ПАРКА (last-known snapshot) — для карты мониторинга
--    Обновляется stream-процессором на каждый пакет. Читается картой (1000+ ТС).
--    Дублируется в Redis для sub-миллисекундного доступа.
-- -----------------------------------------------------------------------------

CREATE TABLE vehicle_state (
    vehicle_id    UUID PRIMARY KEY REFERENCES vehicle(id) ON DELETE CASCADE,
    last_seen     TIMESTAMPTZ,
    status        TEXT,
    last_lat      DOUBLE PRECISION,
    last_lon      DOUBLE PRECISION,
    last_speed    DOUBLE PRECISION,
    active_alarms INT NOT NULL DEFAULT 0,
    last_metrics  JSONB NOT NULL DEFAULT '{}'::jsonb,   -- срез ключевых значений
    geom          GEOGRAPHY(Point, 4326)                -- для bbox/радиус-запросов
);
CREATE INDEX ix_vehicle_state_geom   ON vehicle_state USING GIST (geom);
CREATE INDEX ix_vehicle_state_status ON vehicle_state (status);

-- -----------------------------------------------------------------------------
-- 6. МИССИИ ВАТС
-- -----------------------------------------------------------------------------

CREATE TABLE mission (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vehicle_id    UUID NOT NULL REFERENCES vehicle(id),
    code          TEXT,
    status        TEXT NOT NULL DEFAULT 'planned'
                    CHECK (status IN ('planned','active','paused','completed','aborted','failed')),
    priority      SMALLINT NOT NULL DEFAULT 5,
    planned_route JSONB NOT NULL DEFAULT '[]'::jsonb,
    actual_route  JSONB,
    progress_pct  NUMERIC(5,2) NOT NULL DEFAULT 0,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    result        JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_mission_vehicle ON mission (vehicle_id, created_at DESC);
CREATE INDEX ix_mission_status  ON mission (status);

-- -----------------------------------------------------------------------------
-- 7. ГЕОЗОНЫ И ПРАВИЛА АЛЕРТОВ
-- -----------------------------------------------------------------------------

CREATE TABLE geofence (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('inclusion','exclusion')),
    geom       GEOGRAPHY(Polygon, 4326) NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active  BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX ix_geofence_geom ON geofence USING GIST (geom);

CREATE TABLE alert_rule (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name       TEXT NOT NULL,
    applies_to JSONB NOT NULL DEFAULT '{}'::jsonb,       -- {vehicle_type, site, …}
    condition  JSONB NOT NULL,                           -- {sensor, operator, threshold, for_seconds}
    severity   TEXT NOT NULL CHECK (severity IN ('info','warning','critical')),
    actions    JSONB NOT NULL DEFAULT '[]'::jsonb,       -- ["push","email","webhook"]
    is_active  BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- 8. ПОЛЬЗОВАТЕЛИ, РОЛИ, RBAC
-- -----------------------------------------------------------------------------

CREATE TABLE app_role (
    id   SMALLSERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,                            -- dispatcher, analyst, tech, admin …
    name TEXT NOT NULL
);

CREATE TABLE app_user (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    login         TEXT NOT NULL UNIQUE,
    full_name     TEXT,
    email         TEXT,
    password_hash TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','blocked')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE user_role (
    user_id UUID     NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
    role_id SMALLINT NOT NULL REFERENCES app_role(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

-- -----------------------------------------------------------------------------
-- 9. НЕПРЕРЫВНЫЕ АГРЕГАТЫ (пример «тёплого» слоя — почасовая сводка)
--    Для быстрых отчётов и графиков без сканирования сырых metrics.
-- -----------------------------------------------------------------------------

CREATE TABLE metric_hourly (
    bucket         TIMESTAMPTZ NOT NULL,
    vehicle_id     UUID        NOT NULL,
    sensor_type_id INT         NOT NULL,
    avg_value      DOUBLE PRECISION,
    min_value      DOUBLE PRECISION,
    max_value      DOUBLE PRECISION,
    sample_count   INT,
    PRIMARY KEY (bucket, vehicle_id, sensor_type_id)
);
-- В TimescaleDB реализуется как continuous aggregate (materialized view) с авто-обновлением.

-- =============================================================================
--  ПРИМЕРЫ ЗАПОЛНЕНИЯ СПРАВОЧНИКОВ
-- =============================================================================
INSERT INTO vehicle_type (code, name, category, powertrain, is_autonomous) VALUES
  ('tractor_ice', 'Трактор дизельный',       'tractor',  'ice', FALSE),
  ('forklift_ev', 'Погрузчик электрический', 'forklift', 'ev',  FALSE),
  ('cart',        'Тележка',                 'cart',     'none',FALSE),
  ('robot_agv',   'Робот (ВАТС)',            'robot',    'ev',  TRUE);

INSERT INTO sensor_type (code, name, unit, value_type, min_value, max_value, category) VALUES
  ('gps.location',    'Геопозиция',                NULL,  'json',   NULL, NULL, 'common'),
  ('speed',           'Скорость',                  'км/ч','numeric',0,    120,  'common'),
  ('odometer',        'Пробег',                    'км',  'numeric',0,    NULL, 'common'),
  ('engine.rpm',      'Обороты двигателя',         'об/мин','numeric',0,  4000, 'ice'),
  ('fuel.level',      'Уровень топлива',           '%',   'numeric',0,    100,  'ice'),
  ('coolant.temp',    'Температура ОЖ',            '°C',  'numeric',-40,  130,  'ice'),
  ('battery.soc',     'Заряд тяговой батареи',     '%',   'numeric',0,    100,  'ev'),
  ('battery.temp',    'Температура батареи',       '°C',  'numeric',-40,  120,  'ev'),
  ('battery.voltage', 'Напряжение батареи',        'В',   'numeric',0,    1000, 'ev'),
  ('autonomy.mode',   'Режим автономности',        NULL,  'text',   NULL, NULL, 'avts'),
  ('mission.progress','Прогресс миссии',           '%',   'numeric',0,    100,  'avts'),
  ('estop.state',     'Состояние аварийной кнопки',NULL,  'boolean',NULL, NULL, 'avts'),
  ('lift.height',     'Высота подъёма вил',        'м',   'numeric',0,    7,    'specific'),
  ('lift.load_weight','Вес груза на вилах',        'кг',  'numeric',0,    5000, 'specific');

INSERT INTO event_type (code, name, category, default_severity) VALUES
  ('overspeed',        'Превышение скорости',   'driving',     'warning'),
  ('geofence_exit',    'Выход из геозоны',      'security',    'warning'),
  ('harsh_braking',    'Резкое торможение',     'driving',     'info'),
  ('excessive_idling', 'Простой сверх нормы',   'operational', 'info'),
  ('low_fuel',         'Низкий уровень топлива','technical',   'warning'),
  ('overheat',         'Перегрев',              'technical',   'critical'),
  ('estop_triggered',  'Аварийный останов ВАТС','security',    'critical'),
  ('operator_intervention','Вмешательство оператора','operational','info'),
  ('connection_lost',  'Потеря связи',          'technical',   'warning');

INSERT INTO app_role (code, name) VALUES
  ('dispatcher','Диспетчер'), ('analyst','Аналитик парка'),
  ('technician','Технический специалист'), ('manager','Руководитель'),
  ('admin','Администратор'), ('avts_operator','Оператор ВАТС');
