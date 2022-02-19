var express = require('express');
var router = express.Router();

const  {buildSites} = require("./siteFunctions");



/* GET users listing. */
router.get('/', async function(req, res, next) {
  
  console.log("in sites router")
  // console.log("domains are: ", req.headers)

  //let sitesObject = await buildSites().then()
  
  res.header("Access-Control-Allow-Origin", "http://localhost:3000"); // update to match the domain you will make the request from
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");

  let sitesObject = await buildSites()    //        .then( result => res.status(200).send(result))

  if (!sitesObject.sites) {
    console.log(" !ERROR!, couldn't retrieve site list.")
    res.status(500).send()
  }

  // sitesObject.then( res.status(200).send(sitesObject) )

  // res.status(666).send({"A": 2})
  console.log("\n Sites Object succesfully retrieved with   ", sitesObject.sites.length , "  sites present.\n")

  console.log('sites objects', sitesObject);

  //let responseObject = sitesObject.sites    // **** WHY DOES THIS WORK? WHY CAN'T I JUST RETURN siteObject, why does it return blank after promise resolved....

  let JSONtransmittableObject = {
    ...sitesObject,
    reverseNameLookupMap: Object.fromEntries(sitesObject.reverseNameLookupMap),
    nameLookupMap: Object.fromEntries(sitesObject.nameLookupMap),
    IDlookupMap: Object.fromEntries(sitesObject.IDlookupMap)

  }

  res.status(200).send(JSONtransmittableObject)

  

});

module.exports = router;
