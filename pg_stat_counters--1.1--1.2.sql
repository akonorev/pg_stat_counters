/* pg_stat_counters/pg_stat_counters--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_stat_counters UPDATE TO '1.2'" to load this file. \quit

CREATE FUNCTION _fullversion()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

