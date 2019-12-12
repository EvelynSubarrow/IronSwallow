CREATE TABLE darwin_schedules(
    uid                   VARCHAR(7) NOT NULL,
    rid                   CHAR(15)   NOT NULL,
    ssd                   DATE       NOT NULL,
    signalling_id         CHAR(4)    NOT NULL,
    status                CHAR(1)    NOT NULL,
    category              CHAR(2)    NOT NULL,
    operator              CHAR(2)    NOT NULL,
    is_active             BOOL       NOT NULL DEFAULT FALSE,
    is_charter            BOOL       NOT NULL DEFAULT FALSE,
    is_deleted            BOOL       NOT NULL DEFAULT FALSE,
    is_passenger          BOOL       NOT NULL DEFAULT FALSE,

    UNIQUE (uid, ssd),
    UNIQUE (rid),
    PRIMARY KEY (rid)
);

CREATE INDEX idx_sched_uid on darwin_schedules(uid);
CREATE INDEX idx_sched_ssd on darwin_schedules(ssd);
CREATE INDEX idx_sched_rid on darwin_schedules(rid);

CREATE TABLE darwin_schedule_locations(
    rid                   CHAR(15)    UNIQUE NOT NULL REFERENCES darwin_schedules(rid) ON DELETE CASCADE,
    index                 SMALLINT,
    type                  VARCHAR(4)  NOT NULL,
    tiploc                VARCHAR(7)  NOT NULL,
    action                VARCHAR(12) NOT NULL,

    pta                   TIMESTAMP DEFAULT NULL,
    wta                   TIMESTAMP DEFAULT NULL,
    wtp                   TIMESTAMP DEFAULT NULL,
    ptd                   TIMESTAMP DEFAULT NULL,
    wtd                   TIMESTAMP DEFAULT NULL,

    -- live data
    cancelled             BOOL NOT NULL DEFAULT FALSE,
    rdelay                SMALLINT NOT NULL DEFAULT 0,
    live_arrival          JSON DEFAULT '{}',
    live_departure        JSON DEFAULT '{}',
    platform              JSON DEFAULT '{}',

    UNIQUE(rid, tiploc, wta, wtd, wtp)
);

CREATE INDEX idx_sched_location_tiploc on darwin_schedule_locations(tiploc);
CREATE INDEX idx_sched_location_wta on darwin_schedule_locations(wta);
CREATE INDEX idx_sched_location_wtd on darwin_schedule_locations(wtd);
CREATE INDEX idx_sched_location_wtp on darwin_schedule_locations(wtp);
