import DataRow from "./DataRow";
import TYPES from "./Types";
import * as helpers from "../helpers";

/**
 * LOGIN_HISTORY View
 *
 * https://docs.snowflake.net/manuals/sql-reference/account-usage/login_history.html
 */
export default class LoginHistory extends DataRow {
  constructor(date) {
    super();
    this.EVENT_ID = helpers.getID();
    this.EVENT_TIMESTAMP = date.toISOString();
    const user = helpers.randomFromArray(this.constructor.getUsers());
    this.USER_NAME = user.name;
    this.REPORTED_CLIENT_TYPE = user.driver;
    this.IS_SUCCESS = Math.random() < 0.9 ? "YES" : "NO";
  }

  static getUsers() {
    return [
      { name: "WEB_CLIENT", driver: "JAVASCRIPT_DRIVER", querySpeed: 1.2 },
      { name: "BOB", driver: "OTHER", querySpeed: 2.0 },
      { name: "BI_APP", driver: "JDBC_DRIVER", querySpeed: 0.8 },
      { name: "JANE", driver: "SNOWFLAKE_UI", querySpeed: 1.0 }
    ];
  }

  static types() {
    return [
      {
        name: "EVENT_ID",
        type: TYPES.string
      },
      {
        name: "EVENT_TIMESTAMP",
        type: TYPES.timestamp
      },
      {
        name: "USER_NAME",
        type: TYPES.string
      },
      {
        name: "REPORTED_CLIENT_TYPE",
        type: TYPES.string
      },
      {
        name: "IS_SUCCESS",
        type: TYPES.string
      }
    ];
  }
}
