-- Table: public.execonfig

-- DROP TABLE public.execonfig;

CREATE TABLE public.execonfig
(
    rowid integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    "SeriesID" integer,
    "AreaID" integer,
    "LastModified" timestamp without time zone,
    "StartTime" timestamp without time zone,
    "EndTime" timestamp without time zone,
    "LastRunDate" timestamp without time zone,
    "LastRunStatus" varchar(10),
    "Status" varchar(10),
    "isActive" integer
)

TABLESPACE pg_default;

ALTER TABLE public.execonfig
    OWNER to postgres;