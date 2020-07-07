-- Table: public.authentication

-- DROP TABLE public.authentication;

CREATE TABLE public.authentication
(
    username text COLLATE pg_catalog."default",
    pwd text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE public.authentication
    OWNER to postgres;