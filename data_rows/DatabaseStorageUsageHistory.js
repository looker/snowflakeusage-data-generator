import DataRow from "./DataRow";
import TYPES from "./Types";
import { randomFromArray } from "../helpers";

class InitializeError extends Error {}

/**
 * DATABASE_STORAGE_USAGE_HISTORY View
 *
 * https://docs.snowflake.net/manuals/sql-reference/account-usage/database_storage_usage_history.html
 */
export default class DatabaseStorageUsageHistory extends DataRow {
  constructor(databaseID, databaseName, date) {
    super();
    if (!this.constructor._sizes) {
      throw new InitializeError(
        `Must call ${this.constructor.name}.initialize() first`
      );
    }
    this.USAGE_DATE = date.toISOString();
    this.DATABASE_ID = databaseID;
    this.DATABASE_NAME = databaseName;
    this._setAverageDatabaseBytes();
    this._setAverageFailsafeBytes();
  }

  _setAverageDatabaseBytes() {
    let size = this.constructor._sizes[this.DATABASE_NAME];
    if (Math.random() < 0.1) {
      const changeDegree = Math.random() * (0.2 - 0.05) + 0.05;
      const change = Math.round(size * changeDegree);
      if (Math.random() < 0.3) {
        size -= change;
      } else {
        size += change;
      }
    }
    this.constructor._sizes[this.DATABASE_NAME] = size;
    this.AVERAGE_DATABASE_BYTES = size;
  }

  _setAverageFailsafeBytes() {
    this.AVERAGE_FAILSAFE_BYTES = 0;
    if (Math.random() < 0.1) {
      this.AVERAGE_FAILSAFE_BYTES = Math.round(
        Math.random() * this.AVERAGE_DATABASE_BYTES
      );
    }
  }

  static initialize(dbs) {
    if (this._sizes) {
      throw new InitializeError(
        `Must call ${this.name}.initialize() only once`
      );
    }
    const seedSizes = [1.1e15, 1.2e15, 1.3e15, 1.4e15];
    const sizes = {};
    for (const db of dbs) {
      const seed = randomFromArray(seedSizes);
      sizes[db.DATABASE_NAME] = Math.round(Math.random() * seed);
    }
    this._sizes = sizes;
  }

  static types() {
    return [
      {
        name: "USAGE_DATE",
        type: TYPES.timestamp
      },
      {
        name: "DATABASE_ID",
        type: TYPES.integer
      },
      {
        name: "DATABASE_NAME",
        type: TYPES.string
      },
      {
        name: "AVERAGE_DATABASE_BYTES",
        type: TYPES.integer
      },
      {
        name: "AVERAGE_FAILSAFE_BYTES",
        type: TYPES.integer
      }
    ];
  }
}
