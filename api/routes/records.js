var express = require('express');
var router = express.Router();

const {processController} = require ("./recordsFunctions")

/* GET users listing. */
router.get('/', async function(req, res, next) {

  res.header("Access-Control-Allow-Origin", "http://localhost:3000"); // update to match the domain you will make the request from
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");

  let result = await processController();

  if (!result.siteOutputWithStats) {
    console.log(" !ERROR!, couldn't retrieve site list.")
    res.status(500).send()
  }

  // sitesObject.then( res.status(200).send(sitesObject) )

  // res.status(666).send({"A": 2})
  console.log("\n Transfers Object succesfully retrieved with   ", result)



  res.status(200).send(result)



});

module.exports = router;
