CREATE TABLE darwin_schedules(
    uid                   VARCHAR(7) NOT NULL,
    rid                   CHAR(15)   NOT NULL,
    rsid                  CHAR(8),
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
    rid                   CHAR(15)    NOT NULL REFERENCES darwin_schedules(rid) ON DELETE CASCADE,
    index                 SMALLINT,
    type                  VARCHAR(4)  NOT NULL,
    tiploc                VARCHAR(7)  NOT NULL,
    activity              VARCHAR(12) NOT NULL,

    pta                   TIMESTAMP DEFAULT NULL,
    wta                   TIMESTAMP DEFAULT NULL,
    wtp                   TIMESTAMP DEFAULT NULL,
    ptd                   TIMESTAMP DEFAULT NULL,
    wtd                   TIMESTAMP DEFAULT NULL,

    -- liveish data
    cancelled             BOOL NOT NULL DEFAULT FALSE,
    rdelay                SMALLINT NOT NULL DEFAULT 0,

    UNIQUE(rid, tiploc, wta, wtd, wtp)
);

CREATE INDEX idx_sched_location_tiploc on darwin_schedule_locations(tiploc);
CREATE INDEX idx_sched_location_wta on darwin_schedule_locations(wta);
CREATE INDEX idx_sched_location_wtd on darwin_schedule_locations(wtd);
CREATE INDEX idx_sched_location_wtp on darwin_schedule_locations(wtp);

CREATE TABLE last_received_sequence (
    id SMALLINT NOT NULL UNIQUE,
    sequence INTEGER NOT NULL,
    time_acquired TIMESTAMP NOT NULL
);

CREATE TABLE darwin_schedule_status (
    rid                   CHAR(15) NOT NULL,
    tiploc                VARCHAR(7),

    location_index        SMALLINT,

    ta                    TIMESTAMP DEFAULT NULL,
    tp                    TIMESTAMP DEFAULT NULL,
    td                    TIMESTAMP DEFAULT NULL,

    ta_source             VARCHAR DEFAULT NULL,
    tp_source             VARCHAR DEFAULT NULL,
    td_source             VARCHAR DEFAULT NULL,

    ta_type               VARCHAR(1) DEFAULT NULL,
    tp_type               VARCHAR(1) DEFAULT NULL,
    td_type               VARCHAR(1) DEFAULT NULL,

    ta_delayed            BOOL,
    tp_delayed            BOOL,
    td_delayed            BOOL,

    plat                  VARCHAR DEFAULT NULL,
    plat_suppressed       BOOL,
    plat_cis_suppressed   BOOL,
    plat_confirmed        BOOL,
    plat_source           VARCHAR,

    UNIQUE(rid, tiploc, location_index)
);

CREATE INDEX idx_sched_status_ta on darwin_schedule_status(ta);
CREATE INDEX idx_sched_status_td on darwin_schedule_status(td);
CREATE INDEX idx_sched_status_tp on darwin_schedule_status(tp);

CREATE INDEX idx_sched_status_tiploc on darwin_schedule_status(tiploc);
CREATE INDEX idx_sched_status_index on darwin_schedule_status(location_index);

CREATE OR REPLACE FUNCTION purge_status() RETURNS trigger AS $$
    BEGIN
        DELETE FROM darwin_schedule_status WHERE darwin_schedule_status.rid=OLD.rid;
        RETURN OLD;
    END;
    $$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_schedule_delete BEFORE DELETE ON darwin_schedules FOR EACH ROW
    EXECUTE PROCEDURE purge_status();





