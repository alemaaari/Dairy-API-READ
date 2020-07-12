-- Table: public.value_weekly

-- DROP TABLE public.value_weekly;

CREATE TABLE public.value_weekly
(
    "RecordID" integer NOT NULL,
    "ProductID" integer,
    "SeriesID" integer,
    "AreaID" integer,
    "ReportPeriod" timestamp without time zone,
    "WeekEnding" timestamp without time zone,
    "High" double precision,
    "Low" double precision,
    "Value" double precision,
    "CollectionDate" timestamp without time zone,
    "SourceID" integer,
    "LastModified" timestamp without time zone NOT NULL,
    "Active" integer,
    CONSTRAINT value_weekly_pkey PRIMARY KEY ("RecordID"),
    CONSTRAINT "value_weekly_RecordID_SeriesID_AreaID_ReportPeriod_key" UNIQUE ("RecordID", "SeriesID", "AreaID", "ReportPeriod")
)

TABLESPACE pg_default;

ALTER TABLE public.value_weekly
    OWNER to postgres;