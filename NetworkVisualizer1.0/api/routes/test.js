const express = require("express");
const router = express.Router();
const fs = require("fs");

function runPython(callback) {
  const spawn = require("child_process").spawn;
  const process = spawn("python", [
    "./es_client.py",
    "--debug true",
    "2020/12/01",
  ]);

  console.log("\nTrying to run python...");

  //note this doesn't wait, it just runs the thing and grabs whatever the immediate response is, if any
  process.stdout.on("data", (data) => {
    console.log("Python: " + data.toString());
  });

  process.on("close", (code) => {
    callback();
  });
}

/* GET home page. */
router.get("/", function (req, res, next) {
  runPython(function () {
    console.log("callback done, sending data to page");

    fs.readFile("out.json", "utf8", (err, data) => {
      if (err) {
        console.error(err);
        return;
      }

      res.end(data);
    });
  });
});

module.exports = router;
