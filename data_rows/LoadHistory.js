import DataRow from "./DataRow";
import { TYPES } from "./Types";

/**
 * LOAD_HISTORY View
 *
 * https://docs.snowflake.net/manuals/sql-reference/account-usage/load_history.html
 */
export class LoadHistory extends DataRow {
  constructor(tableID, date) {
    super();
    this.TABLE_ID = tableID;
    this.LAST_LOAD_TIME = date.toISOString();
    this.ROW_COUNT = Math.round(Math.random() * 100);
    this.ERROR_COUNT = Math.random() < 0.1 ? Math.round(Math.random() * 10) : 0;
  }

  static oddsNew() {
    return 0.2;
  }

  static types() {
    return [
      {
        name: "TABLE_ID",
        type: TYPES.integer
      },
      {
        name: "LAST_LOAD_TIME",
        type: TYPES.timestamp
      },
      {
        name: "ROW_COUNT",
        type: TYPES.integer
      },
      {
        name: "ERROR_COUNT",
        type: TYPES.integer
      }
    ];
  }
}
