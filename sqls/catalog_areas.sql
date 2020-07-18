-- Table: public.catalog_areas

DROP TABLE IF EXISTS public.catalog_areas;

CREATE TABLE public.catalog_areas
(
    "AreaID" integer,
    "AreaName" varchar(400),
    "AreaNameTranslated" varchar(900),
    "AreaNameCode" varchar(900),
    "AreaDescription" varchar(8000),
    "LastModified" timestamp without time zone,
    "Active" boolean
)

TABLESPACE pg_default;

ALTER TABLE public.catalog_areas
    OWNER to postgres;