CREATE EXTENSION IF NOT EXISTS postgis;

-- schema creation
CREATE SCHEMA sitt;

COMMENT ON SCHEMA sitt IS 'Schema for Si.T.T. simulation';

-- tables
create table sitt.hubs
(
    id        text                   not null
        constraint hubs_pk
            primary key,
    geom      geography(PointZ, 4326) not null,
    overnight boolean default false  not null,
    harbor    boolean default false  not null,
    market    boolean default false  not null
);

create index hubs_geom_index
    on sitt.hubs using gist (geom);

create index hubs_harbor_index
    on sitt.hubs (harbor);



create table sitt.roads
(
    id        text not null
        constraint roads_pk
            primary key,
    geom geography(LineStringZ,4326) not null,
    hub_id_a  text not null
        constraint roads_hubs_a_id_fk
            references sitt.hubs,
    hub_id_b  text not null
        constraint roads_hubs_b_id_fk
            references sitt.hubs,
    roughness double precision default 1
);



create index roads_geom_index
    on sitt.roads using gist (geom);

create index roads_hub_id_a_index
    on sitt.roads (hub_id_a);

create index roads_hub_id_b_index
    on sitt.roads (hub_id_b);



create table sitt.water_bodies
(
    id       integer not null
        constraint water_bodies_pk
            primary key,
    geom     geometry,
    is_river boolean
);

alter table sitt.water_bodies
    owner to postgres;

create index water_bodies_geom_index
    on sitt.water_bodies using gist (geom);


create table sitt.water_parts
(
    id            serial,
    geom          geometry(Polygon, 4326),
    water_body_id integer
        constraint water_parts_water_bodies_id_fk
            references sitt.water_bodies,
    is_river      boolean default false
);

create index water_parts_geom_index
    on sitt.water_parts using gist (geom);


create table sitt.water_depths
(
    id            serial
        constraint water_depths_pk
            primary key,
    geom          geometry(Point, 4326),
    water_body_id integer,
    depth_m       double precision not null default 0
);

create index water_depths_geom_index
    on sitt.water_depths using gist (geom);

create index water_depths_water_body_id_index
    on sitt.water_depths (water_body_id);



create table sitt.water_lines
(
    id   integer,
    geom geometry
);

alter table sitt.water_lines
    owner to postgres;

create index sidx_water_lines_geom
    on sitt.water_lines using gist (geom);


CREATE TYPE sitt.edge_type AS ENUM ('road', 'river', 'lake');

create table sitt.edges
(
    id       text                       not null
        constraint edges_pk
            primary key,
    geom     geography(GeometryZ, 4326) not null,
    hub_id_a text                       not null
        constraint roads_hubs_a_id_fk
            references sitt.hubs,
    hub_id_b text                       not null
        constraint roads_hubs_b_id_fk
            references sitt.hubs,
    type     sitt.edge_type,
    cost_a_b double precision default 1 not null,
    cost_b_a double precision default 1 not null,
    data     jsonb
);

alter table sitt.edges
    owner to postgres;

create index edges_hub_id_a_index
    on sitt.edges (hub_id_a);

create index edges_hub_id_b_index
    on sitt.edges (hub_id_b);


