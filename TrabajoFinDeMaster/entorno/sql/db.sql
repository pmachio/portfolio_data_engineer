--
-- PostgreSQL database dump
--

-- Dumped from database version 11.1 (Debian 11.1-1.pgdg90+1)
-- Dumped by pg_dump version 11.1 (Debian 11.1-1.pgdg90+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_with_oids = false;

--Setup database
DROP DATABASE IF EXISTS tracking;
CREATE DATABASE tracking;
\c tracking;

\echo 'Creando la base de datos '
CREATE TABLE public.journeys (
    id uuid NOT NULL,
	  device_id int8 NOT NULL,
    start_timestamp timestamp NOT NULL,
    start_location_address text NOT NULL,
    start_location_latitude float NOT NULL,
    start_location_longitude float NOT NULL,
    end_timestamp timestamp  NULL,
    end_location_address text NULL,
    end_location_latitude float NULL,
    end_location_longitude float NULL,
    distance int8 NULL,
    consumption int8 NULL,
	  label text NULL
);  
comment on column journeys.id is 'Identificador del trayecto'; 
comment on column journeys.device_id is 'Identificador del dispositivo que realiza el trayecto';
comment on column journeys.start_timestamp is 'Fecha de inicio del trayecto';
comment on column journeys.start_location_address is 'Dirección de la localización del inicio del trayecto';
comment on column journeys.start_location_latitude is 'Latitud de la localización del inicio del trayecto';
comment on column journeys.start_location_longitude is 'Longitud de la localización del inicio de trayecto';
comment on column journeys.end_timestamp is 'Fecha de fin de trayecto. Solo se establece si está terminado el trayecto';
comment on column journeys.end_location_address is 'Dirección de la localización del final de trayecto. Solo se establece si está terminado el trayecto';
comment on column journeys.end_location_latitude is 'Latitud de la localización del final de trayecto. Solo se establece si está terminado el trayecto';
comment on column journeys.end_location_longitude is 'Longitud de la localización del final de trayecto. Solo se establece si está terminado el trayecto';
comment on column journeys.distance is 'Distancia recorrida en metros. Solo se establece si está terminado el trayecto';
comment on column journeys.consumption is 'Consumo realizado en trayecto en litros. Solo se establece si está terminado el trayecto';
comment on column journeys.label is 'Etiqueta asociada al trayecto que permite identificar trayectos con el mismo inicio y fin (location_latitude, location_longitude)';

CREATE TABLE public.frames (
	id int8 NOT NULL,
	device_id int8 NOT NULL,
	created timestamp NOT NULL,
	received timestamp NOT NULL,
	location_created timestamp NULL,
	location_address text NULL,
	location_latitude numeric(11, 7) NULL,
	location_longitude numeric(11, 7) NULL,
	location_altitude float8 NULL,
	location_speed float4 NULL,
	location_valid bool NULL,
	location_course float4 NULL,
	ignition bool NULL
    --Pendiente meter las entradas de sondas de combustible, 
);
comment on column frames.id is 'Indetificador único del frame';
comment on column frames.device_id is 'Identificador del dispositivo';
comment on column frames.created is 'Fecha de creción de la trama';
comment on column frames.received is 'Fecha de recepción de la trama en el sistema';
comment on column frames.location_created is 'Fecha de creación de la localización del dispositivo';
comment on column frames.location_address is 'Dirección de la localización del dispositivo';
comment on column frames.location_latitude is 'Latitud de la localización del dispositivo';
comment on column frames.location_longitude is 'Longitud de la localización del dispositivo';
comment on column frames.location_altitude is 'Altitud de la localización del dispositivo';
comment on column frames.location_speed is 'Velocidad de la localización del dispositivo';
comment on column frames.location_valid is 'Si o no valida la localización del dispositivo';
comment on column frames.location_course is 'Dirección en grados de la localización del dispositivo';
comment on column frames.ignition is 'Si la trama contiene el estado de la llave de contacto del vehículo';

CREATE TABLE public.events (
	id uuid NOT NULL,
	device_id int8 NOT NULL,
	created timestamp NOT NULL,
	type_id int8 NOT NULL,
	location_address text NULL,
    location_latitude numeric(11, 7) NULL,
	location_longitude numeric(11, 7) NULL,
	value text NULL
);
comment on column events.id is 'Indetificador único del evento';
comment on column events.device_id is 'Identificador del dispositivo';
comment on column events.created is 'Fecha de creción de la del evento';
comment on column events.type_id is 'Identificador del tipo de evento';
comment on column events.location_address is 'Dirección de la localización del dispositivo que generó el evento';
comment on column events.location_latitude is 'Latitud de la localización del dispositivo que generó el evento';
comment on column events.location_longitude is 'Longitud de la localización del dispositivo que generó el evento';
comment on column events.value is 'Información extra sobre el evento';
