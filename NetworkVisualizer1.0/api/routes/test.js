const { randomUUID } = require("crypto");
const express = require("express");
const router = express.Router();
const fs = require("fs");

function runPython(callback, startDate, endDate, transferType, uuid) {
  const spawn = require("child_process").spawn;

  const process = spawn("python3", [
  "./json_concatenate.py",
  "-S", startDate,
  "-E", endDate,
  "-T", transferType,
  "-U", uuid
  ]);

  process.on('error', function(err) {
    console.log("Error from child process, see out.json\n")
  });

  console.log("\nNode trying to run python...\n");

  //note this doesn't wait, it just runs the thing and grabs whatever the immediate response is, if any
  process.stdout.on("data", (data) => {
    console.log("_________Python Begin\n\n" + data.toString() + "\n_________Python End\n");
  });
  let filename = `out_jsons/${uuid}-out.json`
  process.on("close", (code) => {
    callback(filename);
  });
}

/* GET home page. */
router.get("/", function (req, res, next) {

  // console.log(req.query)
  const uuid = randomUUID()
  console.log(uuid)
  runPython(function (filename) {
    console.log("callback done, sending data to page: \n\n");

    fs.readFile(filename, "utf8", (err, data) => {
      if (err) {
        console.log(data)
        console.log("\n** file not found! es_client.py probably didn't succesfully make out.json!**\n")
        // console.error(err);
        // return;
      }
      console.log(data+"\n")
      res.end(data);
    });
  }, req.query.startDate, req.query.endDate, req.query.transferType, uuid);
});

module.exports = router;
