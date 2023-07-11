CREATE DATABASE sitt;

\c sitt

CREATE EXTENSION postgis;

CREATE SCHEMA topology;

CREATE TABLE topology.rechubs (
    id integer NOT NULL,
    geom public.geometry(PointZ,4326),
    rechubid text,
    overnight text default 'n'
);

CREATE SEQUENCE topology.rec_hubs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE topology.rec_hubs_id_seq OWNED BY topology.rechubs.id;


CREATE TABLE topology.recroads (
    id integer NOT NULL,
    geom public.geometry(LineStringZ,4326),
    recroadid text,
    hubaid text,
    hubbid text
);

CREATE SEQUENCE topology.recroads_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE topology.recroads_id_seq OWNED BY topology.recroads.id;

ALTER TABLE ONLY topology.rechubs ALTER COLUMN id SET DEFAULT nextval('topology.rec_hubs_id_seq'::regclass);
ALTER TABLE ONLY topology.recroads ALTER COLUMN id SET DEFAULT nextval('topology.recroads_id_seq'::regclass);

ALTER TABLE ONLY topology.rechubs
    ADD CONSTRAINT rec_hubs_pkey PRIMARY KEY (id);
ALTER TABLE ONLY topology.recroads
    ADD CONSTRAINT recroads_pkey PRIMARY KEY (id);
CREATE INDEX sidx_rec_hubs_geom ON topology.rechubs USING gist (geom);
CREATE INDEX sidx_recroads_geom ON topology.recroads USING gist (geom);