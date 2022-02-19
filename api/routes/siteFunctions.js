const fs = require('fs');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');











// buildSites will get run by other code, which then returns a fully formed sites Object which can then have transfers added to each site by other module















// const cache = {};

async function buildSites() {

  // if (cache.sitesObject) {
  //     return cache.sitesObject;
  // }

  let rawSites = await downloadSites()
  console.log("\n")
  let processedResults = await processSites(rawSites)
  console.log("\n")
  let postMergeResults = await mergeAllDuplicates(processedResults)
  console.log("\n")
  let sitesObject = await buildLookupTables(postMergeResults)

  //at this stage we will update the location data for any hardcoded sites that are 0.0 in CRIC
  //waiting to do this at this stage so we can use the handy lookup tables we just made

  await loadHardCodedSites(sitesObject)
  console.log("\nOverwriting CRIC downloaded entries with hard coded entrys from /config_files/hardCodedSites.json...")
  
//   console.log("\n\nFinal Site Output Object:\n")
  
  // console.log(sitesObject)


  //update the reverse table after the hard coded sites have been added too

  sitesObject = await buildLookupTables(sitesObject)

//   console.log(sitesObject)

  let geoJsonObject = await createGeoJsonFile(sitesObject)

  sitesObject.geoJsonSites = geoJsonObject;

  // fs.writeFileSync("siteOutput.json", JSON.stringify(sitesObject.sites))
  // fs.writeFileSync("sitesGeoJson.json", JSON.stringify(geoJsonObject))
  // cache.sitesObject = sitesObject;

  return sitesObject
}
























async function loadHardCodedSites (siteListObject) {
  let hardCodedSites = {}

  // console.log(siteListObject, "\n\n\n\n")
  filePath = './config_files/hardCodedSites.json'

  const data = await fs.promises.readFile(filePath, 'utf8'); 

  hardCodedSites = JSON.parse(data)
  // console.log( "\n\n\n\n",hardCodedSites, "\n\n\n")

  for (i in hardCodedSites) {

    let manualyAddedSite = hardCodedSites[i]

    //check to make sure there is a number for both coordinates to avoid adding malformed or example entries from hard coded data
    if ( !isNaN(manualyAddedSite.coordinates.longitude) && !isNaN(manualyAddedSite.coordinates.latitude) ) {
      
      let targetIndex = siteListObject.nameLookupMap.get(manualyAddedSite.name)

      console.log("Found hardcoded entry for  \"" + manualyAddedSite.name + "\"  in file: " + filePath)
      
      //update the coordinates for the appropriate entry in siteListObject
      siteListObject.sites[targetIndex].longitude = manualyAddedSite.coordinates.longitude
      siteListObject.sites[targetIndex].latitude = manualyAddedSite.coordinates.latitude
    }
  }
}





























async function downloadSites() {

  // *TODO* Note don't leave this in   vvvvvvvv      eventually we DO want to secure the site
  process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;


  
  let result = await axios.get('https://dune-cric.cern.ch/api/dune/vofeed//list/', {headers: { "Accept":"application/xml" }})

  console.log("\n")
  // console.log(result.data)

  //NOTE, if there is collision between attribute names set attributeNamePrefix to "_@" and use [""] notation to access them (versus . notation)
  let parser = new XMLParser({ignoreAttributes: false, attributeNamePrefix : ""});
  let objResult = parser.parse(result.data)


  // let siteObject =  await processSites(objResult.root.atp_site)
  let siteObject = objResult.root.atp_site

  return siteObject

 }
 











 function createGeoJsonFile (passedSiteObject) {

  // console.log("\n\n\n\n\n\n\n\n", passedSiteObject, "\n\n\n\n\n\n\n")

  let geoJsonObject = { "type": "FeatureCollection", "features":[]}

  for (x in passedSiteObject.sites) {
    let newSiteObject = {"type": "Feature", "geometry": {"type": "Point", "coordinates": [parseFloat(passedSiteObject.sites[x].longitude), parseFloat(passedSiteObject.sites[x].latitude)]}, "properties": {"internalID": passedSiteObject.sites[x].assignedID} }

    geoJsonObject.features.push(newSiteObject)
  }

  return geoJsonObject
 }




















 
 function processSites(serializedSites) {

  let uniqueSiteIDs = generateUniqueIDarray(serializedSites.length)


  let results = []
  results.sites = []
  for (x in serializedSites) {

    //build the final version of the JS object from this data
    let siteObject = {}

    //set these up for later use as the transfers for this site will be contained here


    siteObject.assignedID = uniqueSiteIDs[x]

    

    siteObject.latitude = parseFloat(serializedSites[x].latitude)
    siteObject.longitude = parseFloat(serializedSites[x].longitude)

    siteObject.names = extractNames(serializedSites[x])

    // siteObject.displayName = siteObject.names[0]

    // console.log("\n")
    siteObject.mergeTarget = findPotentialDuplicateSites(siteObject, results)
    // console.log("\n")

    siteObject.sent = []
    siteObject.received = []
    siteObject.allTransfers= []
    siteObject.transfersByDate = new Map()

    siteObject.totalSent = 0;
    siteObject.totalRecieved = 0;

    results.sites.push(siteObject)
    // console.log(siteObject)
  }

  return results
  // mergeDuplicates(results)

 }

























 function generateUniqueIDarray(count) {

  ids=[]

  for (let i=0; i < count; i++) {
    let newID = Math.floor(100000 + Math.random() * 900000)

    while ( ids.includes(newID) ){
      newID = Math.floor(100000 + Math.random() * 900000)
    }

    ids.push(newID)
  }

  return ids
 }





























 function extractNames (individualSite) {

  // let debugswitch = false

  //this regex selects all the charactesr before _ in name, IE country codes
  let countryCodeRegex = ".*_";
  //then do theString.replace(regexResult, "") to clear out contry code

  //dune prefix followed by two character country code remover IE DUNE_US_BNL_SDCC -> BNL_SDCC   or    DUNE_FR_CCIN2P3_DISK -> CCIN2P3_DISK

  // console.log("raw", individualSite)

  names = [];
  //for instance here we add BR_CBPF and CBPF to the names list that this record might be under
  names.push(individualSite.name)
  names.push(individualSite.name.replace(new RegExp(countryCodeRegex), ""))

  // console.log(individualSite.group)

  //harvest names from the group section
  for (y in individualSite.group) {
    if ( !individualSite.group[y].name.toLowerCase().includes("tier") ) {  //ignore names like "Tier-3"
      if (!names.includes(individualSite.group[y].name)){ //if name isn't already in last, add it
        names.push(individualSite.group[y].name)
        names.push(individualSite.group[y].name.replace(new RegExp(countryCodeRegex), ""))

        //above we push the regular name, and then we try to push it with country code removed IE BR_CBPF and CBPF
      }
    }

    // if (debugswitch) {console.log(names)}
  }

  // console.log("before", names)

  //do a filter to remove duplicates from this entry before we return
  names = [...new Set(names)]

  // console.log("after SET", names, "\n\n")

  return names

 }






























 function findPotentialDuplicateSites(individialSite, allSites) {

  let currentNamesLength = individialSite.names.length
  let siteCount = allSites.sites.length
  // let firstFound = false

  let mergeCandidateList = []
  let mergeParentList = []




  for (let i=0; i < currentNamesLength; i++) {
    let currentName = individialSite.names[i]

    for (let j=1; j < siteCount; j++) {

      // console.log("here")

      let candidateSite = allSites.sites[j] 
      for (let k=0; k < candidateSite.names.length; k++) {

        if (candidateSite.names[k] === currentName) {
          // console.log("overlap found")
          //here we've detected overlap between names IE multiple sites for which they both have UCHICHAGO, I interprit this to mean these are the same site so they'll be merged for the purpose of this program
          
          // console.log(candidateSite.names[k], " versus ", currentName)
          // if (!firstFound) {mergeCandidateList.push(individialSite.assignedID)}
          // firstFound = true
          mergeCandidateList.push(candidateSite.assignedID)
          mergeParentList.push(individialSite.assignedID)
          break
        }
      }

    }

  }

  if (mergeCandidateList > 0) {

    console.log("collision found in site names between these IDs: ", mergeCandidateList, " and ", mergeParentList)

  }



  if (mergeCandidateList.length > 0) {
    return mergeCandidateList
  } else return false

 }


































 function mergeAllDuplicates (sitesObject) {

  let sites = sitesObject.sites
  // console.log(sitesObject.sites)

  for (x in sites) {
    let currentSite = sites[x]

    // console.log(currentSite.mergeTarget)
    // console.log(sitesObject.sites.length)

    if (currentSite.mergeTarget !== false) {
      for (y in currentSite.mergeTarget) {

        let currentMergeTargetID = currentSite.mergeTarget[y]
        let mergeTargetArrayIndex;
        let mergeTargetSite = {}

        for (z in sites) {
          if (sites[z].assignedID === currentMergeTargetID) {
            mergeTargetSite = sites[z]
            mergeTargetArrayIndex = z
          }
        }

        if (currentMergeTargetID===undefined) {
          console.log("ERROR, merge target ID not found!")
        }

        console.log("duplicate entry found, site: ", currentSite.assignedID, " (" + currentSite.names[0] + ") " +"@ array index: ", x ,"   for target: ", currentMergeTargetID , "@ array index: ", mergeTargetArrayIndex)

        //now do the merging
        
        if ( Math.abs(currentSite.longitude) < 1 && Math.abs(currentSite.longitude) < 1) {  // 1-,-1 to 1,1 is just ocean, I checked, so there's no way this can be legit
          currentSite.longitude = mergeTargetSite.longitude
          currentSite.latitude = mergeTargetSite.latitude   //the the parent has bad coordinates then worst case we get 0,0 again and maybe get better ones
        }  

        currentSite.names = currentSite.names.concat(mergeTargetSite.names)
        //this ^ likely will cause duplicates so we will clean those up by making a unique set now...

        currentSite.names = [...new Set(currentSite.names)]

        currentSite.mergeTarget = false   //set to false after we've already done the copying

        //now to delete the child we've absorbed the details of

        sitesObject.sites.splice(mergeTargetArrayIndex, 1)
      }
    }
  }
  // console.log(sitesObject.sites)
  return sitesObject
 }





























 function buildLookupTables(allSites) {





  let IDlookupMap = new Map();
  let nameLookupMap = new Map();

  //lowercase all the names so that we don't have to worry about capitalization for matching later

  for (index in allSites.sites) {

    let thisSitesNames = allSites.sites[index].names
    for ( x in thisSitesNames ) {
      thisSitesNames[x] = thisSitesNames[x].toLowerCase()
    }
  }

  for (x in allSites.sites) {

    let siteObject = allSites.sites[x]

    IDlookupMap.set(siteObject.assignedID, allSites.sites.indexOf(siteObject))

    for (nameIndex in siteObject.names) {
      nameLookupMap.set(siteObject.names[nameIndex], allSites.sites.indexOf(siteObject))
    }

  }

  //also make a map for partial word match 


  let reverseNameLookupMap = new Map();

  for (index in allSites.sites) {

    let thisSitesNames = allSites.sites[index].names
    let finalNames = []
    let thisSitesLookupName = thisSitesNames[0]
    let mapCoord = nameLookupMap.get(thisSitesLookupName)
    // console.log(thisSitesLookupName," found at coord ", mapCoord, "in existing lookup")

    for ( x in thisSitesNames ) {


      let oneName = thisSitesNames[x]
      let originalMapCoord = nameLookupMap.get(oneName)
      let nameParts = oneName.split(/([-|_| ])/)

      // console.log("parts: ", nameParts)


      //(nameParts[y]!=="_" || nameParts[y]!=="-" || nameParts[y]==" " ||)
      

      for (let y=0; y< nameParts.length; y++) {

        if (nameParts[y].length > 2 && nameParts[y]!=="uki" && nameParts[y]!=="grid" ) {
          // console.log("part: ", nameParts[y], "  length", nameParts[y].length)
          finalNames.push(nameParts[y])

        }
      }
    }

    finalNames = [... new Set(finalNames)]

    for (index in finalNames) {
      reverseNameLookupMap.set(finalNames[index], mapCoord)
    }

    // console.log("\n")
    // console.log("final names:   ", finalNames)
    // console.log("\n\n\n\n")
  }

  // console.log(reverseNameLookupMap)

  allSites.IDlookupMap = IDlookupMap
  allSites.nameLookupMap = nameLookupMap
  allSites.reverseNameLookupMap = reverseNameLookupMap

  return allSites

 }








 

// buildSites()


module.exports= {buildSites: buildSites}
