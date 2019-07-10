import uuid4 from "uuid/v4";

import DataRow from "./DataRow";
import LoginHistory from "./LoginHistory";
import TYPES from "./Types";
import * as helpers from "../helpers";

/**
 * QUERY_HISTORY View
 *
 * https://docs.snowflake.net/manuals/sql-reference/account-usage/query_history.html
 */
export default class QueryHistory extends DataRow {
  constructor(databaseName, date) {
    super();
    this.QUERY_ID = uuid4();
    this.setQueryText();
    this.DATABASE_NAME = databaseName;
    const queryType = helpers.randomFromArray(this._getQueryTypes());
    this.QUERY_TYPE = queryType.type;
    const user = helpers.randomFromArray(LoginHistory.getUsers());
    this.USER_NAME = user.name;
    [this.WAREHOUSE_NAME, this.WAREHOUSE_SIZE] = helpers.randomFromArray([
      ["BIG_WH", "X-Large"],
      ["SMALL_WH", "Small"]
    ]);
    this.setExecutionStatus();
    this.START_TIME = date.toISOString();
    this.COMPILATION_TIME = Math.round(Math.random() * 1000);
    this.setExecutionTime(user.querySpeed, queryType.querySpeed);
    this.QUEUED_REPAIR_TIME = 0;
    this.QUEUED_OVERLOAD_TIME =
      Math.random() < 0.2 ? Math.round(Math.random() * 1000) : 0;
    this.setTransactionBlockedtime();
  }

  setQueryText() {
    const queries = ["SHOW USERS", "SHOW WAREHOUSES", "SELECT foo FROM bar"];
    this.QUERY_TEXT = helpers.randomFromArray(queries);
  }

  _getQueryTypes() {
    return [
      { type: "WITH", querySpeed: 1.2 },
      { type: "REPLACE", querySpeed: 0.5 },
      { type: "SHOW", querySpeed: 0.2 },
      { type: "CREATE", querySpeed: 0.1 },
      { type: "COPY", querySpeed: 1.5 },
      { type: "SELECT", querySpeed: 1.9 },
      { type: "UNKNOWN", querySpeed: 2.2 }
    ];
  }

  setExecutionStatus() {
    const statuses = [
      { weight: 0.9, name: "SUCCESS" },
      { weight: 0.31, name: "RUNNING" },
      { weight: 0.28, name: "QUEUED" },
      { weight: 0.24, name: "BLOCKED" },
      { weight: 0.19, name: "RESUMING_WAREHOUSE" },
      { weight: 0.15, name: "FAILED_WITH_ERROR" },
      { weight: 0.1, name: "FAILED_WITH_INCIDENT" }
    ];
    this.EXECUTION_STATUS = helpers.randomFromArrayByWeight(statuses);
  }

  setExecutionTime(userQuerySpeed, queryTypeSpeed) {
    const warehouseQuerySpeed = {
      BIG_WH: 0.5,
      SMALL_WH: 1.5
    }[this.WAREHOUSE_NAME];
    const factor = warehouseQuerySpeed * userQuerySpeed * queryTypeSpeed * 1000;
    this.EXECUTION_TIME = Math.round(Math.random() * factor);
  }

  setTransactionBlockedtime() {
    this.TRANSACTION_BLOCKED_TIME = 0;
    if (Math.random() < 0.05) {
      this.TRANSACTION_BLOCKED_TIME = Math.round(Math.random() * 1000);
    }
  }

  static oddsNew() {
    return 0.3;
  }

  static types() {
    return [
      {
        name: "QUERY_ID",
        type: TYPES.string
      },
      {
        name: "QUERY_TEXT",
        type: TYPES.string
      },
      {
        name: "DATABASE_NAME",
        type: TYPES.string
      },
      {
        name: "QUERY_TYPE",
        type: TYPES.string
      },
      {
        name: "USER_NAME",
        type: TYPES.string
      },
      {
        name: "WAREHOUSE_NAME",
        type: TYPES.string
      },
      {
        name: "WAREHOUSE_SIZE",
        type: TYPES.string
      },
      {
        name: "EXECUTION_STATUS",
        type: TYPES.string
      },
      {
        name: "START_TIME",
        type: TYPES.timestamp
      },
      {
        name: "COMPILATION_TIME",
        type: TYPES.integer
      },
      {
        name: "EXECUTION_TIME",
        type: TYPES.integer
      },
      {
        name: "QUEUED_REPAIR_TIME",
        type: TYPES.integer
      },
      {
        name: "QUEUED_OVERLOAD_TIME",
        type: TYPES.integer
      },
      {
        name: "TRANSACTION_BLOCKED_TIME",
        type: TYPES.integer
      }
    ];
  }
}
