import DataRow from "./DataRow";
import TYPES from "./Types";

/**
 * STORAGE_USAGE View
 *
 * https://docs.snowflake.net/manuals/sql-reference/account-usage/storage_usage.html#storage-usage-view
 */
export default class StorageUsage extends DataRow {
  constructor(dbStorageUsageHistories, date) {
    super();
    this.USAGE_DATE = date.toISOString();
    this.STORAGE_BYTES = dbStorageUsageHistories.reduce(
      (sum, current) => sum + current.AVERAGE_DATABASE_BYTES,
      0
    );
    this.STAGE_BYTES = Math.round(Math.random() * this.STORAGE_BYTES);
    this.FAILSAFE_BYTES = 0;
    if (Math.random() < 0.1) {
      this.FAILSAFE_BYTES = Math.round(this.STORAGE_BYTES / 10);
    }
  }

  static types() {
    return [
      {
        name: "USAGE_DATE",
        type: TYPES.timestamp
      },
      {
        name: "STORAGE_BYTES",
        type: TYPES.integer
      },
      {
        name: "STAGE_BYTES",
        type: TYPES.integer
      },
      {
        name: "FAILSAFE_BYTES",
        type: TYPES.integer
      }
    ];
  }
}
