import DataRow from "./DataRow";
import TYPES from "./Types";

export default class Tables extends DataRow {
  constructor(id) {
    super();
    this.ID = id;
  }

  static generate() {
    const ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    return ids.map(id => new this(id));
  }

  static types() {
    return [
      {
        name: "ID",
        type: TYPES.integer
      }
    ];
  }
}
