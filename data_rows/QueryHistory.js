import uuid4 from "uuid/v4";

import DataRow from "./DataRow";
import LoginHistory from "./LoginHistory";
import TYPES from "./Types";
import * as helpers from "../helpers";

const ODDS_NEW = 0.3;

/**
 * QUERY_HISTORY View
 *
 * https://docs.snowflake.net/manuals/sql-reference/account-usage/query_history.html
 */
export default class QueryHistory extends DataRow {
  constructor(databaseName, date) {
    super();
    this.QUERY_ID = uuid4();
    this.DATABASE_NAME = databaseName;
    const queryInfo = helpers.randomFromArrayByWeight(
      this._getQueryInfo(),
      true
    );
    this.QUERY_TEXT = queryInfo.text;
    this.QUERY_TYPE = queryInfo.type;
    const user = helpers.randomFromArray(LoginHistory.getUsers());
    this.USER_NAME = user.name;
    [this.WAREHOUSE_NAME, this.WAREHOUSE_SIZE] = helpers.randomFromArray([
      ["BIG_WH", "X-Large"],
      ["SMALL_WH", "Small"]
    ]);
    this._setExecutionStatus();
    this.START_TIME = date.toISOString();
    this.COMPILATION_TIME = Math.round(Math.random() * 1000);
    this._setExecutionTime(user.querySpeed, queryInfo.querySpeed);
    this.QUEUED_REPAIR_TIME = 0;
    this.QUEUED_OVERLOAD_TIME =
      Math.random() < 0.2 ? Math.round(Math.random() * 1000) : 0;
    this._setTransactionBlockedtime();
  }

  _getQueryInfo() {
    return [
      {
        weight: 0.02,
        type: "UNKNOWN",
        querySpeed: 2.2,
        text: "What's all this then?"
      },
      {
        weight: 0.04,
        type: "CREATE",
        querySpeed: 0.1,
        text: "CREATE DATABASE"
      },
      { weight: 0.06, type: "COPY", querySpeed: 1.5, text: "COPY TABLE" },
      { weight: 0.08, type: "SHOW", querySpeed: 0.2, text: "SHOW USERS" },
      {
        weight: 0.1,
        type: "REPLACE",
        querySpeed: 0.5,
        text: "REPLACE INTO fooTable"
      },
      {
        weight: 0.2,
        type: "WITH",
        querySpeed: 1.2,
        text: "WITH fooTable (bar)"
      },
      {
        weight: 0.5,
        type: "SELECT",
        querySpeed: 1.9,
        text: "SELECT * FROM fooTable"
      }
    ];
  }

  _setExecutionStatus() {
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

  _setExecutionTime(userQuerySpeed, queryTypeSpeed) {
    const warehouseQuerySpeed = {
      BIG_WH: 0.5,
      SMALL_WH: 1.5
    }[this.WAREHOUSE_NAME];
    const factor = warehouseQuerySpeed * userQuerySpeed * queryTypeSpeed * 1000;
    this.EXECUTION_TIME = Math.round(Math.random() * factor);
  }

  _setTransactionBlockedtime() {
    this.TRANSACTION_BLOCKED_TIME = 0;
    if (Math.random() < 0.05) {
      this.TRANSACTION_BLOCKED_TIME = Math.round(Math.random() * 1000);
    }
  }

  static oddsNew() {
    return ODDS_NEW;
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
