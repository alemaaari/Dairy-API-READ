DROP TABLE IF EXISTS public.apiconfig;

CREATE TABLE public.apiconfig
(	id integer,
 	tablename text,
    username text COLLATE pg_catalog."default",
    pwd text COLLATE pg_catalog."default",
 	endpoint text
)

TABLESPACE pg_default;

ALTER TABLE public.apiconfig
    OWNER to postgres;
