import DataRow from "./DataRow";
import { TYPES } from "./Types";

/**
 * DATABASES View
 *
 * https://docs.snowflake.net/manuals/sql-reference/account-usage/databases.html
 */
export class Databases extends DataRow {
  constructor(name, id) {
    super();
    this.DATABASE_NAME = name;
    this.DATABASE_ID = id;
  }

  static generate() {
    const names = [
      "squiggly_database",
      "jims_database",
      "jacksonbase",
      "prod",
      "staging_db",
      "dev1"
    ];
    return names.map((name, idx) => new this(name, idx + 1));
  }

  static types() {
    return [
      {
        name: "DATABASE_NAME",
        type: TYPES.string
      },
      {
        name: "DATABASE_ID",
        type: TYPES.integer
      }
    ];
  }
}
