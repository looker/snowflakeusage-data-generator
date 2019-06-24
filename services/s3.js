import fs from "fs";

import * as AWS from "aws-sdk";

export default class s3 {
  constructor() {
    this.awsService = new AWS.S3();
  }

  putFile(filepath, bucketName, s3Name) {
    return new Promise((resolve, reject) => {
      fs.readFile(filepath, (err, data) => {
        if (err) {
          console.log(err);
          reject(err);
        }

        let base64data = new Buffer(data, "binary");

        this.awsService.putObject(
          { Bucket: bucketName, Key: s3Name, Body: base64data },
          (err, data) => {
            console.log(arguments);
            if (err) {
              console.log(err);
              reject(err);
            } else {
              console.log(data);
              resolve(data);
            }
          }
        );
      });
    });
  }
}
