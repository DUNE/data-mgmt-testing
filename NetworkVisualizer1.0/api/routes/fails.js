//Developed by Lydia Brynmoor
const express = require("express");
const router = express.Router();
const fs = require("fs");




function runPython(callback, startDate, endDate) {
  const spawn = require("child_process").spawn;

  const process = spawn("python3", [
  "./es_client.py",
  "-S", startDate,
  "-E", endDate,
  "-M", "4"
  ]);

  process.on('error', function(err) {
    console.log("Error from child process, see out.json\n")
  });

  console.log("\nNode trying to run python... (failure mode 4)\n");

  //note this doesn't wait, it just runs the thing and grabs whatever the immediate response is, if any
  process.stdout.on("data", (data) => {
    console.log("_________Python Begin\n\n" + data.toString() + "\n_________Python End\n");
  });

  process.on("close", (code) => {
    callback();
  });
}





/* GET home page. */
router.get("/", function (req, res, next) {

  // console.log(req.query)

  runPython(function () {
    console.log("callback done, sending data to page: \n\n");

    fs.readFile("fails.json", "utf8", (err, data) => {
      if (err) {
        console.log(data)
        console.log("\n** file not found! es_client.py probably didn't succesfully make the json file we need!**\n")
        // console.error(err);
        // return;
      }
      console.log(data+"\n")
      res.end(data);
    });
  }, req.query.startDate, req.query.endDate);
});

module.exports = router;
