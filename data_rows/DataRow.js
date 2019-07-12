import stringify from "csv-stringify";
import fs from "graceful-fs";

export default class DataRow {
  static oddsNew() {
    return 1;
  }

  static rollDice() {
    return Math.random() < this.oddsNew();
  }

  columns() {
    return this.constructor.types().reduce((accum, e) => {
      accum[e.name] = e.name;
      return accum;
    }, {});
  }

  static snowflakeColumns() {
    let colString = this.types().reduce((accum, e, idx, array) => {
      let withNewCol = accum + `${e.name} ${e.type}`;
      if (idx !== array.length - 1) {
        withNewCol += ", ";
      }
      return withNewCol;
    }, "");
    return `(${colString})`;
  }
}

export class Writer {
  constructor() {
    this.streams = {};
    this.firsts = {};
  }

  writeFirst(filename, instance) {
    return new Promise(resolve => {
      stringify(
        [instance],
        { columns: instance.columns(), header: true },
        (err, output) => {
          fs.writeFileSync(filename, output);
          resolve();
        }
      );
    });
  }

  writeNotFirst(filename, instance) {
    if (!this.streams[filename]) {
      this.streams[filename] = fs.createWriteStream(filename, { flags: "a" });
    }
    return new Promise(resolve => {
      stringify(
        [instance],
        { columns: instance.columns(), header: false },
        (err, output) => {
          this.streams[filename].write(output, () => {
            resolve();
          });
        }
      );
    });
  }

  write(filename, instance) {
    const first = !this.firsts[filename];
    if (first) {
      this.firsts[filename] = true;
      return this.writeFirst(filename, instance);
    } else {
      return this.writeNotFirst(filename, instance);
    }
  }
}
