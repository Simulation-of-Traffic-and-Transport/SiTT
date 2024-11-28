CREATE DATABASE sitt;

\c sitt

CREATE EXTENSION postgis;
CREATE EXTENSION postgis_sfcgal;

CREATE SCHEMA topology;

CREATE TABLE topology.rechubs (
    id integer NOT NULL,
    geom public.geometry(PointZ,4326),
    rechubid text,
    overnight text default 'n'
);

alter table topology.rechubs
    add constraint rechubs_pk
        primary key (id);

create index sidx_rec_hubs_geom
    on topology.rechubs using gist (geom);

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

alter table topology.recroads
    add constraint recroads_pk
        primary key (id);

create index sidx_rec_roads_geom
    on topology.recroads using gist (geom);

CREATE SEQUENCE topology.recroads_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE topology.recroads_id_seq OWNED BY topology.recroads.id;

create table topology.recrivers
(
    id                integer,
    geom              geometry(LineString, 4326),
    recroadid         text,
    hubaid            text,
    hubbid            text,
    direction         text,
    dimensions        text,
    explanationfileid text
);

alter table topology.recrivers
    add constraint recrivers_pk
        primary key (id);

create index sidx_rec_rivers_geom
    on topology.recrivers using gist (geom);

CREATE SEQUENCE topology.rec_rivers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE topology.rec_rivers_id_seq OWNED BY topology.recrivers.id;

ALTER TABLE ONLY topology.rechubs ALTER COLUMN id SET DEFAULT nextval('topology.rec_hubs_id_seq'::regclass);
ALTER TABLE ONLY topology.recroads ALTER COLUMN id SET DEFAULT nextval('topology.recroads_id_seq'::regclass);
ALTER TABLE ONLY topology.recrivers ALTER COLUMN id SET DEFAULT nextval('topology.rec_rivers_id_seq'::regclass);

ALTER TABLE ONLY topology.rechubs
    ADD CONSTRAINT rec_hubs_pkey PRIMARY KEY (id);
ALTER TABLE ONLY topology.recroads
    ADD CONSTRAINT recroads_pkey PRIMARY KEY (id);
CREATE INDEX sidx_rec_hubs_geom ON topology.rechubs USING gist (geom);
CREATE INDEX sidx_recroads_geom ON topology.recroads USING gist (geom);