var express = require('express');
var router = express.Router();

const {processController} = require ("./recordsFunctions")

/* GET users listing. */
router.get('/', async function(req, res, next) {

  res.header("Access-Control-Allow-Origin", "http://localhost:3000"); // update to match the domain you will make the request from
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");

  //console.log(" REQ PARAMS: ", req.query)
  let searchParameters = req.query;

  let result = await processController(searchParameters);
  res.status(200).send(result)



});

module.exports = router;
