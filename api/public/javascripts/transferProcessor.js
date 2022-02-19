const {buildSites} = require("./siteDownloader");
const fs = require('fs');




function loadRucioConfig() {
  let fs = require('fs');

  fs.readFile("../../config_files/backendConfig.txt", 'utf8', function(error, data){
    let results=data.split("\"")
    console.log("Loading Config: ", results[1], "          " ,results[3])
    return [[results[1]][results[3]]]
  })

}

function dateFormatConverter(passedDate) {
  const date = passedDate.toISOString().split("T")[0].replace(/-/g, "/");
  return date;
}





//searchParamters object should have at least the following attributes, arguments[], search modes[], 

function runPython(callbackFunction, searchParameters) {
    
  
  const spawn = require("child_process").spawn;

    let configData = loadRucioConfig();

    let programName = ""      
    let programPath = configData[0]
    let outputPath = configData[1]

    searchParameters.startDate = dateFormatConvert(searchParameters.startDate)
    searchParameters.endDate = dateFormatConvert(searchParameters.endDate)

    const process = spawn("python3", [
    programPath + programName,
    "-S", searchParameters.startDate,
    "-E", searchParameters.endDate,
    "-M", searchParameters.searchModes,
    "-D", outputPath
    ]);
  
    process.on('error', function(err) {
      console.log("Python error detected, child process encoutered an error. \n")
    });
  
    console.log("\nNode trying to run Python...\n");
  
    //log the standard out from the program
    process.stdout.on("data", (data) => {
      console.log("_________Python Begin\n\n" + data.toString() + "\n_________Python End\n");
    });
  



    process.on("close", (code) => {

        let responseDetails = {statusCode: -42, message:"", success:false, outputFilePath:outputPath, start:searchParameters.startDate, end:searchParameters.endDate}       //TODO, discuss what data we should take from this

        //TODO here we will read the exit code and compare the value to a table below and report on the status.
        
        switch (code) {
            case 0:
              console.log('Python reports no errors upon exit\n');
              break;
            case 1:
                console.log("Python reports unsuccessful exit");
                break;
            case 666:
              console.log('We can have other message codes, like server never responded, or bad credentials or something, lets consider what options to have.');
              break;
            default:
              console.log("Python reported an unknown status code: ", code);
          }

        responseDetails.statusCode=code
        responseDetails.message=""                          //TODO save some descriptive messages for each exit code
        responseDetails.typesRetrieved.push("transfers")    //TODO, read what arguments are being sent to python so we can syncronize that

        if (code==0) {
          responseDetails.success=true
          callbackFunction(responseDetails);
        }
        else{
          callbackFunction(responseDetails);
        }

        
    });

  }

















async function processSomeData(responseObject) {

  // if (!responseObject.processedData){
  //   return
  // }

  // let numberOfDaysToProcess = (responseObject.end.getTime() - responseObject.start.getTime() ) / (1000*3600*24);    //this gives us the number of days we want to look at, thus the number of files

  let fs = require('fs');

  var getDaysArray = function(start, end) {                                             //thanks to enesn on stack overflow for these 8 or so lines
    for(var arr=[],dt=new Date(start); dt<=end; dt.setDate(dt.getDate()+1)){
        arr.push(new Date(dt));
    }
    return arr;
  };

  var daylist = getDaysArray(new Date("2021-02-01"),new Date("2021-02-02"));          //TODO, add the actual date ranges passed into here

  responseObject.start=daylist[0]
  responseObject.end = daylist[daylist.length-1]

  for (i in daylist) {
    daylist[i] = daylist[i].toISOString().slice(0,10)   //this outputs a string in the yyyy-mm-dd format, thought about using localstring but what if the local environment is europe? day and month could be inverted
  }

  // console.log("\nloading data for these days:", daylist)

  let transferObect = {days:[]}

  // fs.readdir("../../backend_components/rucio/cached_searches", (err, files) => {
  //   files.forEach(file => {
  //     console.log(file);
  //   });
  // });
  
  transferObect.daysPresent = daylist

  for (x in daylist) {
    // console.log("\n", daylist[x])
    const data = await fs.promises.readFile("../../backend_components/rucio/cached_searches/out_M0_" + daylist[x] + ".json", 'utf8');
    let day = {date: daylist[x], transfers: JSON.parse(data)}
      // console.log(day,"\n\n")
    transferObect.days.push(day)

  }

  // console.log(transferObect.days[0])
  // console.log(transferObect.days[1])

  return transferObect

}






      // let singleTransfer = transfers[day][record]
      // console.log(singleTransfer)

      // //now assign transfer to each sites set of sent and recevied
      // siteArrayIndexFromLookupTX = sites.nameLookMap.get(singleTransfer.matchedSource)
      // siteArrayIndexFromLookupRX = sites.nameLookMap.get(singleTransfer.matchedDestination)

      // sites.sites[siteArrayIndexFromLookupTX].sent.push(singleTransfer)
      // sites.sites[siteArrayIndexFromLookupRX].received.push(singleTransfer)





function sortTransfers(sitesObject, transferObject) {
  //here the raw file with the transfers by day is passed in, then we combine this data with our site list data structure so each site will have a collection of transfers per day, basically bubble sort

  // console.log(transferObject)

  console.log("transfers: ", transferObject.days)

  // console.log(transferObject.days[0].transfers[0])

  sitesObject.networkTransferTotal = 0;

 for (i in transferObject.days) {
   // console.log(transferObject.days[i].transfers)

   transferBatchDate = transferObject.days[i].date
  //  console.log("batch date is: ", transferBatchDate)

   for (j in transferObject.days[i].transfers) {

      let singleTransfer = transferObject.days[i].transfers[j]

      sitesObject.networkTransferTotal += singleTransfer.file_size

      let from = singleTransfer.matchedSource
      let to = singleTransfer.matchedDestination

      // console.log(from.transfersByDate)

    
      

      // console.log("here", singeTransfer)
      // console.log("src", singeTransfer.matchedSource)
      // console.log("to", singeTransfer.matchedDestination)

      // let singleTransfer = transferObject.days[i].transfers[j]
      // console.log(singleTransfer)

      // console.log(sitesObject.nameLookupMap)
      // console.log(singleTransfer.matchedSource, singleTransfer.matchedDestination)

      //now assign transfer to each sites set of sent and recevied
      

      // console.log("\nto", to, "  from  ", from)

      siteArrayIndexFromLookupDest = sitesObject.nameLookupMap.get(to)
      siteArrayIndexFromLookupSrc = sitesObject.nameLookupMap.get(from)

      let siteObjectTo = sitesObject.sites[siteArrayIndexFromLookupSrc]
      let siteObjectFrom = sitesObject.sites[siteArrayIndexFromLookupDest]

      //update this sites stats to reflect transfers

      console.log("     singeTransfer:   ", singleTransfer.file_size)
      siteObjectFrom.totalSent += singleTransfer.file_size;
      siteObjectTo.totalRecieved += singleTransfer.file_size;

      console.log("   POST ADD   ", siteObjectFrom, siteObjectTo)

      siteObjectTo.allTransfers.push(singleTransfer)
      siteObjectFrom.allTransfers.push(singleTransfer)


      if ( !siteObjectFrom.transfersByDate.get(transferBatchDate) ) {
        siteObjectFrom.transfersByDate.set(transferBatchDate, [])
      }

      let existingSendsOnThisDate = siteObjectFrom.transfersByDate.get(transferBatchDate)
      existingSendsOnThisDate.push(singleTransfer)
      siteObjectFrom.transfersByDate.set(transferBatchDate, existingSendsOnThisDate)



      if ( !siteObjectTo.transfersByDate.get(transferBatchDate) ) {
        siteObjectTo.transfersByDate.set(transferBatchDate, [])
      }

      let existingReceivesOnThisDate = siteObjectTo.transfersByDate.get(transferBatchDate)
      existingReceivesOnThisDate.push(singleTransfer)
      siteObjectTo.transfersByDate.set(transferBatchDate, existingReceivesOnThisDate)


      // console.log("\n\n\nafter, ", siteObjectFrom)
      // console.log(siteObjectFrom.transfersByDate.get(transferBatchDate), "\n\n\n\n")
      // console.log(siteObjectTo.transfersByDate.get(transferBatchDate), "\n\n\n\n")

      // console.log(siteArrayIndexFromLookupRX, siteArrayIndexFromLookupTX)
      // console.log(sitesObject.sites[siteArrayIndexFromLookupRX], )

      // console.log("sender: ", siteObjectTo.names[0], "reciver: " ,siteObjectFrom.names[0])
      // console.log("\n\n\n\nsender ", siteObjectTo, "\n\n\n\n")

      // siteObjectTo.received.push(singleTransfer)
      siteObjectFrom.sent.push(singleTransfer)
      siteObjectTo.received.push(singleTransfer)

      // console.log("sender: ", siteObjectTo.names[0], "reciver: " ,siteObjectFrom.names[0])
      // console.log("\n\n\n\nsender\n", siteObjectTo, "\n\nreceived\n", siteObjectFrom, "\n\n\n\n")

      // sitesObject.sites[siteArrayIndexFromLookupTX].sent.push(singleTransfer)
      // sitesObject.sites[siteArrayIndexFromLookupRX].received.push(singleTransfer)

   }

  //  console.log(sitesObject.sites[sitesObject.nameLookupMap.get(siteObject.sites.names[0])])
   
 }

  // console.log(sitesObject.sites)

  return [sitesObject, transferObject]
}



function siteNameFuzzyMatching (sitesObject, adressField) {

  let adressFieldWords = adressField.toString().split("_")

  // console.log(source, " to ", destination)
  // console.log(sourceWords, " to ", destWords)

  // look for partial match case
  let matches = new Map()

  for (i in adressFieldWords) {
    let candidateWord = adressFieldWords[i]
    
    if ( sitesObject.reverseNameLookupMap.has(candidateWord) ) {
      if ( !matches.has( sitesObject.reverseNameLookupMap.get(candidateWord) ) ) {
        matches.set(sitesObject.reverseNameLookupMap.get(candidateWord), 1)
        // console.log("first word match for: ", candidateWord, " @ Site #", sitesObject.reverseNameLookupMap.get(candidateWord), sitesObject.sites[sitesObject.reverseNameLookupMap.get(candidateWord)].names[0] )
      }
      else {
        let seenNumber = matches.get(sitesObject.reverseNameLookupMap.get(candidateWord))
        seenNumber+=1
        matches.set(sitesObject.reverseNameLookupMap.get(candidateWord), seenNumber)
        // console.log("encountered another word ", candidateWord ," associated with site ", sitesObject.reverseNameLookupMap.get(candidateWord), " known as:" , sitesObject.sites[sitesObject.reverseNameLookupMap.get(candidateWord)].names[0])
      }
    }
    else {
      // console.log(candidateWord, " not found in any faccillity identifiers")
    }
  }

  let occurances = []
  let ids = []
  matches.forEach((value, key) => {
    occurances.push(value)
    ids.push(key)
  })

  let bestGuessIndex = ids[occurances.indexOf(Math.max(...occurances) )]
  let bestGuess = sitesObject.sites[bestGuessIndex].names[0]
  
  // console.log(occurances)
  // console.log(Math.max(...occurances))
  // console.log( bestGuessIndex )
  console.log("closest match for ", adressField , ", is" , bestGuess)

  return bestGuess

}







async function buildSiteTransferRecords(sitesObject, passedTransferObject) {

  console.log("\n\n\n\n\n")
  // console.log(sites)
  console.log("Dataset Includes:", passedTransferObject)
  // console.log("\n\n\n\n")
  // console.log(transfers.days)

  //setup list of unknown sites that don't match any words
  sitesObject.uncertainSites = []

  //LOGIC: go through each transfer, locate source and destination from list, do fuzzy matching if need be
  //       add transfer to each objects "all transfers member", then add it to each sites corresponding, sent and recieved method under the correct date

  for (x in passedTransferObject.days) {
    console.log("\n\n")
    console.log("\n\n")

    for (y in passedTransferObject.days[x].transfers) {
      let transfer = passedTransferObject.days[x].transfers[y]
      
      console.log(transfer)
      let ascertainedSource;
      let ascertainedDestination;

      let source = transfer.source.toLowerCase();
      let destination = transfer.destination.toLowerCase();

      if ( !sitesObject.nameLookupMap.has(source) ) //if inputting the whole faccility name doesn't yield a match we start trying to match individual words
      {
        if (!sitesObject.uncertainSites.includes(source)) {
            sitesObject.uncertainSites.push(source)
        }
        
        console.log("\nidentifying sender...")
        ascertainedSource = siteNameFuzzyMatching(sitesObject, source)   //best guess based on matching words in faccillity from field:
      } else {
        ascertainedSource = sitesObject.sites[sitesObject.nameLookMap.get(source)]
      }
      

      if ( !sitesObject.nameLookupMap.has(destination) ) //if inputting the whole faccility name doesn't yield a match we start trying to match individual words
      {
        if (!sitesObject.uncertainSites.includes(destination)) {
          sitesObject.uncertainSites.push(destination)
        } 
        console.log("\nidentifying reciever...")
        ascertainedDestination = siteNameFuzzyMatching(sitesObject, destination)   //best guess based on matching words in faccillity from field:
      } else {
        ascertainedDestination = sitesObject.sites[sitesObject.nameLookMap.get(destination)]
      }


      console.log("\nmapped as from: ", ascertainedSource, " and to: ", ascertainedDestination, "\n\n\n\n\n\n")

      //note this on the transfer object so we can use it later for processing

      transfer.matchedSource = ascertainedSource ;
      transfer.matchedDestination = ascertainedDestination ;
    }
  }

  return [sitesObject, passedTransferObject]

}







function calculateSiteMetaStats(passedSitesObject) {

  // console.log("      network total:          ", passedSitesObject.networkTransferTotal)
  console.log("\n\n\n      passed site for meta calc:          \n\n", passedSitesObject.geoJsonSites.features)



  for (let i=0; i < passedSitesObject.sites.length; i++) {
    let site = passedSitesObject.sites[i];

    site.rxFractionOfWholePeriod = 0;
    site.txFractionOfWholePeriod = 0;

    //Math.round(number * 10) / 10

    if (site.totalRecieved > 0) {
      site.rxFractionOfWholePeriod = Math.ceil(site.totalRecieved /passedSitesObject.networkTransferTotal * 1000)/1000;
    }
    
    if (site.totalSent > 0 ) {
      site.txFractionOfWholePeriod = Math.ceil(site.totalSent / passedSitesObject.networkTransferTotal * 1000)/1000;
    }

    //add the transfer ratios from the data model to geo JSON for easier display
    for (let y=0; y < passedSitesObject.geoJsonSites.features.length; y++) {
      if (passedSitesObject.geoJsonSites.features[y].properties.internalID == site.assignedID) {
        passedSitesObject.geoJsonSites.features[y].properties.rxRatio = site.rxFractionOfWholePeriod
        passedSitesObject.geoJsonSites.features[y].properties.txRatio = site.txFractionOfWholePeriod

        passedSitesObject.geoJsonSites.features[y].properties.siteLat = site.latitude
        passedSitesObject.geoJsonSites.features[y].properties.siteLon = site.longitude

        passedSitesObject.geoJsonSites.features[y].properties.name = site.names[0]
      }
    }
    
  }

  //add this info to the geoJSON data too

  // for let(i=0; i < passedSitesObject[1].geoJsonSites.features.length; i++) {

  // }

  return passedSitesObject
}







function createGeoJsonTransferFile (passedTransferObject) {

  // console.log("\n\n\n\n\n\n\n\n geoJSON debug", passedTransferObject, "\n\n\n\n\n\n\n")

  let transfersByDay = passedTransferObject[1].days

  // console.log("passed big object geoJSonSites:          ", passedTransferObject[0].sites)

  let geoJSONtransfers = []
  let animatedgeoJsonTransfers = []

  for (let x=0; x < transfersByDay.length; x++) {

    // console.log("top: ", transfersByDay[x])
    
    // console.log("length: ", transfersByDay[x].transfers.length)

    for (let y=0; y < transfersByDay[x].transfers.length; y++) {
      // console.log("each: ", transfersByDay[x].transfers[y])
    

      let transfer = transfersByDay[x].transfers[y]
      
      let siteIndexSent = passedTransferObject[0].nameLookupMap.get(transfer.matchedSource)
      let sendingSite = passedTransferObject[0].sites[siteIndexSent]
      let coordFrom = [parseFloat(sendingSite.longitude), parseFloat(sendingSite.latitude)]
      // console.log("from: ", coordFrom)
      
      let siteIndexRecv = passedTransferObject[0].nameLookupMap.get(transfer.matchedDestination)
      let receivingSite = passedTransferObject[0].sites[siteIndexRecv]
      let coordTo = [parseFloat(receivingSite.longitude), parseFloat(receivingSite.latitude)]
      // console.log("to: ", coordTo)




      let date = new Date(transfer.start_time)
      let firstOfMonthDate = new Date(date.getFullYear(), date.getMonth(), 1)
      let MiddleOfMonthDate = new Date(date.getFullYear(), date.getMonth(), 15)
      let EndOfMonthDate = new Date(date.getFullYear(), date.getMonth(), 28)
      // console.log("  month nunber for date:  ", date.getMonth()+1)
      let firstOfMonthDateUnixTime = Math.floor(firstOfMonthDate/1000)
      let MiddleOfMonthDateUnixTime = Math.floor(MiddleOfMonthDate/1000)
      let EndOfMonthDateUnixTime = Math.floor(EndOfMonthDate/1000)

      let transferTransaction = {"type":"Feature", "geometry":{ "type": "LineString", "coordinates": [ [coordFrom[0], coordFrom[1]], [coordTo[0], coordTo[1]] ]}, "properties": {"from": sendingSite.names[0], "to": receivingSite.names[0], "toLong":coordTo[0], "toLat":coordTo[1], "fromLong":coordFrom[0], "fromLat":coordFrom[1], "size": parseInt(transfer.file_size), "duration": "Ask Zack how to get", "date": transfer.start_time, "from": transfer.source, "to": transfer.destination, "speed": transfer["transfer_speed(MB/s)"] } }
      // let transferTransactionAnim = {"type":"Feature", "geometry":{ "type": "LineString", "coordinates": [ [coordFrom[0], coordTo[0], 0, firstOfMonthDateUnixTime], [coordFrom[1], coordTo[1], 0, MiddleOfMonthDateUnixTime ], [coordFrom[1], coordTo[1], 0, EndOfMonthDateUnixTime ] ] }, "properties": { "from": sendingSite.names[0], "to": receivingSite.names[0], "toLong":coordTo[0], "toLat":coordTo[1], "fromLong":coordFrom[0], "fromLat":coordFrom[1], "size": parseInt(transfer.file_size), "duration": "Ask Zack how to get", "date": transfer.start_time, "from": transfer.source, "to": transfer.destination, "speed": transfer["transfer_speed(MB/s)"] } }
      
      // **** TODO ***** put in haversine equation to calculate middle interpolated coordinates
      // **** TODO ***** NOTE: currently setting everything to the first day of the month to show everything in that month, until I can come up with a better way to visualize
      // console.log("  anim coords ",transferTransactionAnim.geometry.coordinates)

      geoJSONtransfers.push(transferTransaction)
      // animatedgeoJsonTransfers.push(transferTransactionAnim)
      // console.log ("pertinent info: ", transfer, siteIndex )


    }
  }

  let transferObjectFinal={}

  console.log("\n\n\n\n\n\n\n\n geoJSON result", geoJSONtransfers, "\n\n\n\n\n\n\n")
  // console.log("\n\n\n\n\n\n\n\n geoJSON *** ANIMATED TRIPS *** result", animatedgeoJsonTransfers[0].geometry.coordinates, "\n\n\n\n\n\n\n")
 

  let geoJsonMeta = {"type":"FeatureCollection", "features":geoJSONtransfers}
  let geoJsonMetaAnim = {"type":"FeatureCollection", "features":animatedgeoJsonTransfers}

  transferObjectFinal.geoJson = geoJsonMeta;
  transferObjectFinal.animated = geoJsonMetaAnim

  return transferObjectFinal
 }

















// let responseDetails = {statusCode: -42, message:"", success:false, outputFilePath:"../../backend_componenets/rucio/cached_searches"}       //TODO, discuss what data we should take from this

// processSomeData(responseDetails);


async function processController() {

//  loadRucioConfig();
  
  let responseDetails = {statusCode: -42, message:"", success:true, outputFilePath:"../../backend_componenets/rucio/cached_searches"}



  let sites = await buildSites();
  let transfers = await processSomeData(responseDetails)
  let identifiedTransfers = await buildSiteTransferRecords(sites, transfers)

  sites = identifiedTransfers[0]
  transfers = identifiedTransfers[1]

  // console.log(populatedSites[0], populatedSites[1], populatedSites[1].days[0])

  let processedTransfers = await sortTransfers(sites, transfers)

  // *** This is the farthest step in the pipeline, the data model from sites now has all the transfers in it and is ready to send to the front end for visualization
  
  // *** TODO now lets complete the API by setting up the routes

  // console.log(sites)

  
  
  
  let finalObject = [calculateSiteMetaStats(processedTransfers[0]),processedTransfers[1]]

  

  console.log("\n\n\n\n\n\n\n     final object:   ", finalObject[0].geoJsonSites.features)



  let geoJsonTransferObject = await createGeoJsonTransferFile(finalObject)

  // console.log("     geo JSON: ", geoJsonTransferObject.geoJson.features)

  fs.writeFileSync("transfersGeoJson.json", JSON.stringify(geoJsonTransferObject.geoJson))
  fs.writeFileSync("siteOutput.json", JSON.stringify(finalObject[0].sites))
  fs.writeFileSync("sitesGeoJson.json", JSON.stringify(finalObject[0].geoJsonSites))

  // fs.writeFileSync("transfersGeoJsonAnim.json", JSON.stringify(geoJsonObject.animated))    // *** TODO *** not quite working right, think I'm setting the wrong coordinates and also trip view not much without interpolated steps.

}


processController();

module.exports = {}