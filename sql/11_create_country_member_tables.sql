
-- ============================================================
-- SQL 10: CREATE COUNTRY-SPECIFIC MEMBER TABLES
-- ============================================================
-- Creates country-specific current-state projection tables.
-- Each table inherits the structure of the central CORE table.
--
-- All database, schema and table names are resolved from:
-- config/source_config.json
-- ============================================================

CREATE TABLE IF NOT EXISTS __MEMBER_INDIA_TABLE__
LIKE __MEMBER_CORE_TABLE__;

CREATE TABLE IF NOT EXISTS __MEMBER_USA_TABLE__
LIKE __MEMBER_CORE_TABLE__;

CREATE TABLE IF NOT EXISTS __MEMBER_PHILIPPINES_TABLE__
LIKE __MEMBER_CORE_TABLE__;

CREATE TABLE IF NOT EXISTS __MEMBER_CANADA_TABLE__
LIKE __MEMBER_CORE_TABLE__;

CREATE TABLE IF NOT EXISTS __MEMBER_AUSTRALIA_TABLE__
LIKE __MEMBER_CORE_TABLE__;
