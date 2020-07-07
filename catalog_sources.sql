-- Table: public.catalog_sources

DROP TABLE public.catalog_sources;

CREATE TABLE public.catalog_sources
(
    "SourceID" integer,
    "ReportID" integer,
    "SourceName" varchar(400) ,
    "LastModified" timestamp without time zone ,
    "Active" boolean
)

TABLESPACE pg_default;

ALTER TABLE public.catalog_sources
    OWNER to postgres;