CREATE TABLE darwin_schedules(
    uid                   CHAR(7)  NOT NULL,
    rid                   CHAR(15) NOT NULL,
    ssd                   DATE     NOT NULL,
    signalling_id         CHAR(4)  NOT NULL,
    status                CHAR(1)  NOT NULL,
    category              CHAR(2)  NOT NULL,
    operator              CHAR(2)  NOT NULL,
    is_active             BOOL     NOT NULL DEFAULT FALSE,
    is_charter            BOOL     NOT NULL DEFAULT FALSE,
    is_deleted            BOOL     NOT NULL DEFAULT FALSE,
    is_passenger          BOOL     NOT NULL DEFAULT FALSE,

    UNIQUE (uid, ssd),
    UNIQUE (rid),
    PRIMARY KEY (rid)
);

CREATE INDEX idx_sched_uid on darwin_schedules(uid);
CREATE INDEX idx_sched_ssd on darwin_schedules(ssd);
CREATE INDEX idx_sched_rid on darwin_schedules(rid);
