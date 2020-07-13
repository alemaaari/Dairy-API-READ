-- Table: public.catalog_series

DROP TABLE public.catalog_series;

CREATE TABLE public.catalog_series
(
    "SeriesID" integer,
    "SeriesName" varchar(400),
    "SeriesNameTranslated" varchar(400),
    "SeriesNameCode" varchar(400),
    "UnitID" integer,
    "Scale" integer,
    "LastModified" timestamp without time zone,
    "Active" boolean
)

TABLESPACE pg_default;

ALTER TABLE public.catalog_series
    OWNER to postgres;