import fs from "fs";
import path from "path";

import s3 from "./services/s3";
import Snowflake from "./services/snowflake";
import { Writer } from "./data_rows/DataRow";
import * as models from "./data_rows";
import * as helpers from "./helpers";

const dbs = [];
const tables = [];
const loadHistories = [];
const queryHistories = [];
const storageUsages = [];
const dbStorageUsageHistories = [];
const loginHistories = [];
const warehouseMeteringHistories = [];

/**
 * Create one-time data and kickoff daily data generation
 */
async function generateData(endDate, daysToGen) {
  const startDate = new Date(
    new Date(endDate).setDate(endDate.getDate() - daysToGen)
  );
  startDate.setHours(0, 0, 0, 0);

  dbs.push(...models.Databases.generate());
  models.DatabaseStorageUsageHistory.initialize(dbs);
  tables.push(...models.Tables.generate());

  for (
    let processingDay = new Date(startDate);
    processingDay < endDate;
    processingDay.setDate(processingDay.getDate() + 1)
  ) {
    generateDailyData(processingDay);
  }
  await writeCSVFiles();
}

/**
 * Create daily data and kickoff hourly data generation
 */
function generateDailyData(processingDay) {
  console.log(
    `${new Date().toISOString()} Generating data for: ` +
      `${processingDay.toISOString()}`
  );
  const todaysDBStorageHistories = [];
  for (const db of dbs) {
    const dsuh = new models.DatabaseStorageUsageHistory(
      db.DATABASE_ID,
      db.DATABASE_NAME,
      processingDay
    );
    dbStorageUsageHistories.push(dsuh);
    todaysDBStorageHistories.push(dsuh);
  }
  storageUsages.push(
    new models.StorageUsage(todaysDBStorageHistories, processingDay)
  );

  const endHour = new Date(processingDay).setDate(processingDay.getDate() + 1);
  for (
    let processingHour = new Date(processingDay);
    processingHour < endHour;
    processingHour.setHours(processingHour.getHours() + 1)
  ) {
    generateHourlyData(processingHour);
  }
}

/**
 * Create hourly data and kickoff per-minute data generation
 */
function generateHourlyData(processingHour) {
  warehouseMeteringHistories.push(
    new models.WarehouseMeteringHistory(processingHour)
  );
  if (models.LoadHistory.rollDice()) {
    const tableID = helpers.randomFromArray(tables).ID;
    loadHistories.push(new models.LoadHistory(tableID, processingHour));
  }
  loginHistories.push(new models.LoginHistory(processingHour));

  const endMinute = new Date(processingHour).setHours(
    processingHour.getHours() + 1
  );
  for (
    let processingMinute = new Date(processingHour);
    processingMinute < endMinute;
    processingMinute.setMinutes(processingMinute.getMinutes() + 1)
  ) {
    generatePerMinuteData(processingMinute, queryHistories);
  }
}

function generatePerMinuteData(processingMinute, queryHistories) {
  if (models.QueryHistory.rollDice()) {
    const dbName = helpers.randomFromArray(dbs).DATABASE_NAME;
    queryHistories.push(new models.QueryHistory(dbName, processingMinute));
  }
}

async function writeCSVFiles() {
  const filewriter = new Writer();
  for (const db of dbs) {
    await filewriter.write("./out/DATABASES.csv", db);
  }
  for (const table of tables) {
    await filewriter.write("./out/TABLES.csv", table);
  }
  for (const loadHistory of loadHistories) {
    await filewriter.write("./out/LOAD_HISTORY.csv", loadHistory);
  }
  for (const loginHistory of loginHistories) {
    await filewriter.write("./out/LOGIN_HISTORY.csv", loginHistory);
  }
  for (const queryHistory of queryHistories) {
    await filewriter.write("./out/QUERY_HISTORY.csv", queryHistory);
  }
  for (const storageUsage of storageUsages) {
    await filewriter.write("./out/STORAGE_USAGE.csv", storageUsage);
  }
  for (const dbStorageUsageHistory of dbStorageUsageHistories) {
    await filewriter.write(
      "./out/DATABASE_STORAGE_USAGE_HISTORY.csv",
      dbStorageUsageHistory
    );
  }
  for (const warehouseMeteringHistory of warehouseMeteringHistories) {
    await filewriter.write(
      "./out/WAREHOUSE_METERING_HISTORY.csv",
      warehouseMeteringHistory
    );
  }
}

async function deleteLocalFiles() {
  const directory = path.join(__dirname, "./out");
  const files = fs.readdirSync(directory);
  for (const file of files) {
    const fullPath = path.join(directory, file);
    console.log("deleting: " + fullPath);
    fs.unlinkSync(fullPath);
  }
}

async function uploadSessionDataSnowflake(snowflakeService) {
  const BUCKET_NAME = "looker-applications-demo-loading-data";
  const STAGE_NAME = "lookerApplicationsDemoStage";
  await snowflakeService.createStage(STAGE_NAME, BUCKET_NAME);
  const s3service = new s3();

  const csvFiles = fs.readdirSync("./out");
  for (const csvFile of csvFiles) {
    console.log("uploading to s3: " + csvFile);
    const tablename = csvFile.split(".")[0];
    await s3service.putFile(`./out/${csvFile}`, BUCKET_NAME, csvFile);
    await snowflakeService.copyInto(tablename, STAGE_NAME, csvFile);
  }
}

async function createSnowflakeTables(snowflakeService, tables) {
  for (const tablename of tables) {
    const columns = models.TABLE_MAP[tablename].snowflakeColumns();
    const resp = await snowflakeService.createTable(tablename, columns);
    console.log(resp);
  }
}

async function deleteSnowflakeTables(snowflakeService, tables) {
  for (const tablename of tables) {
    const resp = await snowflakeService.dropTable(tablename);
    console.log(resp);
  }
}

async function generate(daysAgoToEnd, daysToGenerate) {
  try {
    const endDate = new Date();
    endDate.setHours(0, 0, 0, 0);
    endDate.setDate(endDate.getDate() - daysAgoToEnd);

    await deleteLocalFiles();

    await generateData(endDate, daysToGenerate);

    const snowflakeService = new Snowflake();
    await snowflakeService.doAllGrants("data_apps_load_role");
    await snowflakeService.createSchema("account_usage");
    await deleteSnowflakeTables(snowflakeService, models.TABLE_NAMES);
    await createSnowflakeTables(snowflakeService, models.TABLE_NAMES);
    await uploadSessionDataSnowflake(snowflakeService);
  } catch (e) {
    console.log(e);
    throw e;
  }
}

generate(0, 360).catch(e => console.log(e));
