import snowflake from "snowflake-sdk";

const DATABASE = "SNOWFLAKE_GENERATED";
const SCHEMA = "ACCOUNT_USAGE";

export default class Snowflake {
  constructor() {
    this.connection = null;
  }

  connect() {
    let connection = snowflake.createConnection({
      account: "uk87135",
      username: "data_apps_load_user",
      role: "data_apps_load_role",
      database: DATABASE,
      password: process.env.SNOWFLAKE_PASSWORD,
      warehouse: "COMPUTE_WH",
      region: "us-east-1",
      schema: SCHEMA
    });
    return new Promise((resolve, reject) => {
      connection.connect((err, conn) => {
        if (err) {
          console.error("Unable to connect: " + err.message);
          reject(err);
        } else {
          console.log("Successfully connected as id: " + connection.getId());
          resolve(conn);
        }
      });
    });
  }

  async execute(sqlText) {
    if (!this.connection) {
      this.connection = await this.connect();
    }
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText,
        complete: function(err, stmt, rows) {
          if (err) {
            reject(err);
            console.error(
              `Failed to execute statement due to the following error: ` +
                err.message
            );
          } else {
            resolve(rows);
            console.log("Successfully executed statement");
          }
        }
      });
    });
  }

  async createSchema(name) {
    const useDbText = `use ${DATABASE}`;
    const sqlText = `create schema if not exists ${name};`;
    const useSchema = `use schema ${SCHEMA}`;
    return this.execute(useDbText)
      .then(() => this.execute(sqlText))
      .then(() => this.execute(useSchema));
  }

  async createTable(name, columns) {
    const sqlText = `create table if not exists ${name} ${columns}`;
    return this.execute(sqlText);
  }

  async dropTable(name) {
    const sqlText = `drop table if exists ${name};`;
    return this.execute(sqlText);
  }

  async createStage(stageName, bucketName) {
    const sqlText = `create or replace stage
        ${stageName} url='s3://${bucketName}'
      credentials=(
        aws_key_id='${process.env.AWS_ACCESS_KEY_ID}'
        aws_secret_key='${process.env.AWS_SECRET_ACCESS_KEY}'
      )
      file_format = (type=csv skip_header=1);`;
    return this.execute(sqlText);
  }

  async copyInto(tablename, stagename, s3Filepath) {
    const sqlText = `copy into ${tablename} from @${stagename}/${s3Filepath};`;
    return this.execute(sqlText);
  }

  async doAllGrants(role) {
    console.log("doing all grants");
    const grants = [
      `grant usage on database ${DATABASE} to role ${role}`,
      `grant usage on schema ${SCHEMA} to role ${role}`,
      `grant select on all tables in schema ${SCHEMA} to role ${role}`
    ];
    return Promise.all(
      grants.map(grant => {
        return this.execute(grant);
      })
    );
  }
}
