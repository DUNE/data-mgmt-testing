const express = require("express");
const router = express.Router();
const fs = require("fs");
const xml2js = require('xml2js');






function getSites(callback) {

  console.log("\nDownloading dune site location list..\n");


  var http = require('http');
  var options = {
    host: 'dune-cric.cern.ch',
    port: 80,
    path: '/api/dune/vofeed//list/',
    method: 'GET',
    headers: {
    'Accept': 'application/xml',
    }
  };
  var req = http.get(options, function(response) {
    // handle the response
    var res_data = '';
    response.on('data', function(chunk) {
      res_data += chunk;
    });
    response.on('end', function() {
      //console.log(res_data);
      xmlToJson(res_data, callback)
    });
  });
  req.on('error', function(err) {
    console.log("Request error: " + err.message);
  });
}





function xmlToJson (input, pasedCallback) {
  xml2js.parseString(input, (err, result) => {
    if(err) {
        throw err;
    }

    // `result` is a JavaScript object
    // convert it to a JSON string
    const json = JSON.stringify(result, null, 4);

    // log JSON string
    // console.log(json);
    fs.writeFileSync('duneSiteList.json', json)
    pasedCallback()
  });
}






router.get("/", function (req, res, next) {

  getSites(function () {
    console.log("callback done for dune site download \n\n");

    fs.readFile("duneSiteList.json", "utf8", (err, data) => {
      if (err) {
        console.log("\n** file not found! Couldn't find duneSiteList.json**\n")
        // console.error(err);
        return;
      }
      // console.log(data+"\n")
      res.end(data);
    });
  });

});

module.exports = router;
