import Databases from "./Databases";
import DatabaseStorageUsageHistory from "./DatabaseStorageUsageHistory";
import LoadHistory from "./LoadHistory";
import LoginHistory from "./LoginHistory";
import QueryHistory from "./QueryHistory";
import StorageUsage from "./StorageUsage";
import Tables from "./Tables";
import WarehouseMeteringHistory from "./WarehouseMeteringHistory";

export const TABLE_NAMES = [
  "DATABASE_STORAGE_USAGE_HISTORY",
  "DATABASES",
  "LOAD_HISTORY",
  "LOGIN_HISTORY",
  "QUERY_HISTORY",
  "STORAGE_USAGE",
  "TABLES",
  "WAREHOUSE_METERING_HISTORY"
];

export const TABLE_MAP = {
  DATABASE_STORAGE_USAGE_HISTORY: DatabaseStorageUsageHistory,
  DATABASES: Databases,
  LOAD_HISTORY: LoadHistory,
  LOGIN_HISTORY: LoginHistory,
  QUERY_HISTORY: QueryHistory,
  STORAGE_USAGE: StorageUsage,
  TABLES: Tables,
  WAREHOUSE_METERING_HISTORY: WarehouseMeteringHistory
};

export {
  Databases,
  DatabaseStorageUsageHistory,
  LoadHistory,
  LoginHistory,
  QueryHistory,
  StorageUsage,
  Tables,
  WarehouseMeteringHistory
};
