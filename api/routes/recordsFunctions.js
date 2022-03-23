const {buildSites} = require("./siteFunctions");
const fs = require('fs');
const { json } = require("d3");




async function loadConfig() {
  let fs = require('fs');
  let configObject = {};

  let data = await fs.promises.readFile("./config_files/backendConfig.txt", 'utf8');

  let results=data.split("\n")
  //console.log(results)
  

  results.forEach(line => {
    let variable = line.split(":")[0];
    variable.replace("\'",'')
    let value = line.split("\"")[1];
    console.log("***", variable, value, "***")

    configObject[variable] = value;

  });

  //console.log("\n\n\n\n\n  CONFIG OBJ    ...", configObject ,"\n\n\n\n")

  configObject = {...configObject, statusCode: -42, message:"", success:true}
  configObject.executableName = configObject.es_client_path.split("/")[configObject.es_client_path.split("/").length-1]
  // console.log("dl path: ", configObject.download_path)
  configObject.outputFilePath = configObject.download_path.replace("\"", '');
  // let relativePath = configObject.downloadPath.split("/")
  // configObject.relativeDownloadPath = relativePath[relativePath.length-1]
  // console.log("\nconfig object created:", (configObject));


  return configObject;
}




function dateFormatConverter(passedDate) {
  const date = passedDate.toISOString().split("T")[0].replace(/-/g, "/");
  return date;
}





//searchParamters object should have at least the following attributes, arguments[], search modes[], 

function runPython(startDate, endDate) {

  return new Promise(async (resolve, reject) => {

  
      
    let fs = require('fs');

    let configResult = await loadConfig();
    console.log("\n\n\nconfigobject:", configResult ,"\n\n\n")

    let executableName = configResult.executableName;
    let downloadPath = configResult.outputFilePath
    downloadPath = downloadPath.split("/")[downloadPath.split("/").length-1]

    //console.log("\n\nin python run:\n", startDate, "\n", endDate, "\n", executableName, "\n" ,downloadPath)
    
    const spawn = require("child_process").spawn;

      // searchParameters.startDate = dateFormatConverter(searchParameters.startDate)
      // searchParameters.endDate = dateFormatConverter(searchParameters.endDate)

      //
      const process = spawn("python3", [
      configResult.query_script_path,
      "-S", startDate,
      "-E", endDate,
      "-D", configResult.outputFilePath
      ]);

      console.log("\n\n process arguments: ", process.spawnargs)
    
      process.on('error', function(err) {
        console.log("Python error detected, child process encoutered an error. \n")
        reject();
      });
    
      console.log("\nNode trying to run Python...\n");
    
      //log the standard out from the program
      process.stdout.on("data", (data) => {
        console.log("_________Python STD-OUT Begin\n\n" + data.toString() + "\n\n_________Python End\n");
      });
    
      process.stderr.on("data", (data) => {
        console.log("_________Python ERR-OUT Begin\n\n" + data.toString() + "\n\n_________Python End\n");
      });


      process.on("close", (code) => {
          console.log("closing with code: ", code)

          // let responseDetails = {statusCode: -42, message:"", success:false, outputFilePath:outputPath, start:searchParameters.startDate, end:searchParameters.endDate}       //TODO, discuss what data we should take from this

          // //TODO here we will read the exit code and compare the value to a table below and report on the status.
          
          // switch (code) {
          //     case 0:
          //       console.log('Python reports no errors upon exit\n');
          //       break;
          //     case 1:
          //         console.log("Python reports unsuccessful exit");
          //         break;
          //     case 666:
          //       console.log('We can have other message codes, like server never responded, or bad credentials or something, lets consider what options to have.');
          //       break;
          //     default:
          //       console.log("Python reported an unknown status code: ", code);
          //   }

          // responseDetails.statusCode=code
          // responseDetails.message=""                          //TODO save some descriptive messages for each exit code
          // responseDetails.typesRetrieved.push("transfers")    //TODO, read what arguments are being sent to python so we can syncronize that

          // if (code==0) {
          //   responseDetails.success=true
          //   callbackFunction(responseDetails);
          // }
          // else{
          //   callbackFunction(responseDetails);
          // }

          resolve();
      });
      
    });
  }


























  // async function checkIfFilesCached(fileList) {
  //   console.log(" inside file checking function")

  //   for (let i=0; i < fileList.length; i++) {
  //     try {
  //       fs.open(fileList[i])
  //       console.log("after file check")
  //     } catch (error) {
  //       console.log("some files missing, setting flag to download them.")
  //       return false;
  //     }
  //   }

  //   console.log("all files exist");
  //   return true;
  // }




  function getDaysArray(start, end) {                                             //thanks to enesn on stack overflow for these 8 or so lines
    for(var arr=[],dt=new Date(start); dt<=end; dt.setDate(dt.getDate()+1)){
        arr.push(new Date(dt));
    }
    return arr;
  };















  async function processSomeData(configuration) {

    //console.log("***\n\n", configuration  ,"***\n\n")



        
        let fs = require('fs');
        let transferObject = {days:[]}
        let transferCount = 0;
        let allFilesPresent = true;
        let loadedData;
        let failedLoad = 0;
        let failedFiles = [];
    
        let startDate = configuration.searchParameters.startDate;
        let endDate = configuration.searchParameters.endDate;
      
        let dayList = getDaysArray(new Date(startDate),new Date(endDate));
        let utcDates = [...dayList]
        let fileList = []
      
        for (i in dayList) {
            dayList[i] = dayList[i].toISOString().slice(0,10)   //this outputs a string in the yyyy-mm-dd format, thought about using localstring but what if the local environment is europe? day and month could be inverted
        }
    
        //console.log("\n***\n\n\n", utcDates, "\n***\n\n\n")
    
        for (i in dayList) {  //build list of files we need for this data
    
          let terminatingDay = new Date(utcDates[i]);
          //console.log("term day: ", terminatingDay)
          terminatingDay.setDate(terminatingDay.getDate() + 1);
          terminatingDay = terminatingDay.toISOString().slice(0,10);
          let beginingDay = dayList[i];
          let month = dayList[i].slice(5,7);
          let year = dayList[i].slice(0,4);
          let path = "." + configuration.download_path.substring(1) + "/" + year + "/" + month + "/dune_transfers_display_" + beginingDay.replaceAll('-', '_') + "_to_" + terminatingDay.replaceAll('-', '_') + ".json"
          fileList.push(path)
        }
    
        //console.log("\n***\n\n\n", fileList, "\n***\n\n\n")
    
        for (let i=0; i < fileList.length; i++) {
          try {
            fs.open(fileList[i])
          } catch (error) {
            console.log("some files missing, setting flag to download them.")
            allFilesPresent = false;
            break;
          }
        }
    
        if (!allFilesPresent) {
          await runPython(dayList[0], dayList[dayList.length-1]);
          console.log("after async file download")
          //console.log(await fs.promises.readdir("./backend_components/rucio/rucio_es_cache/2021/01")) // show files in directory
    
          for (x in fileList) {
            console.log("trying to load: ", fileList[x])
            let dailyData = await fs.promises.readFile(fileList[x]);
            let jsonObject;
            //sanitize for extra comma at end
            if (dailyData && dailyData.toString().charAt(dailyData.toString().length-4) === ',') {
              //Uh oh, found extra trailing comma that would have broken things, deleting...
              console.log("\nWarning, found extraneous trailing comma and deleted it in file:", fileList[x], "\n")
              let regex = /\,(?!\s*?[\{\[\"\'\w])/g;
              dailyData = ( dailyData.toString() ).replace(regex, ''); // remove all trailing commas
              jsonObject = JSON.parse(dailyData);
              console.log(dailyData.substring(dailyData.length-10, dailyData.length))

              let dailyTransferRecord = {date: dayList[x], transfers: jsonObject}
              transferObject.days.push(dailyTransferRecord)
              transferCount += Object.keys(jsonObject).length
              //console.log("\ntransfer: ", dailyTransferRecord.transfers);

            } else {

              try {
                let transferJSON = JSON.parse(dailyData);
                
                if (transferJSON && Object.keys(transferJSON) > 1) {
                  //console.log("keys", Object.keys(transferJSON))
                  let dailyTransferRecord = {date: dayList[x], transfers: transferJSON}
                  transferObject.days.push(dailyTransferRecord)
                  transferCount += Object.keys(transferJSON).length
                  // console.log("\ntransfer: ", dailyTransferRecord.transfers);
  
                }
  
              } catch (error) {
                console.log("\nFailed to load, bad JSON syntax, file: ", fileList[x], "\n")
                failedFiles.push(fileList[x]);
                failedLoad += 1;
              }

            }
            

          }

          transferObject.daysPresent = dayList
          
          console.log("\n", transferCount, "transfers retrieved from query");
          console.log("\n", failedLoad , "files failed to load (likely bad JSON):\n\n", failedFiles)

          console.log("transfer object sample: ", transferObject[5])
      
          return transferObject
        }
}








































function sortTransfers(sitesObject, transferObject) {
  //here the raw file with the transfers by day is passed in, then we combine this data with our site list data structure so each site will have a collection of transfers per day, basically bubble sort

  console.log(" \n\n\n IN TRANSFERS")
  //console.log(transferObject.days[0].transfers[0])
  console.log(transferObject.days[0])

  sitesObject.networkTransferTotal = 0;

  for (i in transferObject.days) {
      
      transferDate = transferObject.days[i].date

      for (let j in transferObject.days) {

          if (transferObject.days[j].length > 0) {
            console.log("         T   H I S TRANSFER #", transferObject[i].days[j] )
            console.log(" length: ", transferObject[i])
  
            let singleTransfer = transferObject.days[i].transfers[j];
            sitesObject.networkTransferTotal += singleTransfer.file_size
  
            let from = singleTransfer.matchedSource
            let to = singleTransfer.matchedDestination
  
            siteArrayIndexFromLookupDest = sitesObject.nameLookupMap.get(to)
            siteArrayIndexFromLookupSrc = sitesObject.nameLookupMap.get(from)
    
            //console.log("site array index: ", siteArrayIndexFromLookupDest, siteArrayIndexFromLookupSrc)
  
            let siteObjectTo = sitesObject.sites[siteArrayIndexFromLookupSrc]
            let siteObjectFrom = sitesObject.sites[siteArrayIndexFromLookupDest]
  
            
  
            //console.log("  from, to:   ", siteObjectTo, siteObjectFrom)
  
            siteObjectFrom.totalSent += singleTransfer.file_size;
            siteObjectTo.totalRecieved += singleTransfer.file_size;
  
            
  
            siteObjectTo.allTransfers.push(singleTransfer)
            siteObjectFrom.allTransfers.push(singleTransfer)
  
            //console.log(" siiiiiiiii ttee ", siteObjectTo, siteObjectFrom)
            let existingSendsOnThisDate = []
            
            if (siteObjectFrom.transfersByDate.get(transferDate)) {
              existingSendsOnThisDate = siteObjectFrom.transfersByDate.get(transferDate)
            }
  
            existingSendsOnThisDate.push(singleTransfer)
            siteObjectFrom.transfersByDate.set(transferDate, existingSendsOnThisDate)
  
            let existingReceivesOnThisDate = []
            
            if (siteObjectTo.transfersByDate.get(transferDate)) {
              existingReceivesOnThisDate = siteObjectFrom.transfersByDate.get(transferDate)
            }
            
            existingReceivesOnThisDate.push(singleTransfer)
            siteObjectTo.transfersByDate.set(transferDate, existingReceivesOnThisDate)
      
            siteObjectFrom.sent.push(singleTransfer)
            siteObjectTo.received.push(singleTransfer)
          }

          
      }
  }
  return [sitesObject, transferObject]
}



function siteNameFuzzyMatching (sitesObject, adressField) {

  let adressFieldWords = adressField.toString().split("_");
  
  if (adressFieldWords[0].length === adressField.length && adressFieldWords[0]==adressField) {
    //console.log(" ----- spliting - char")
    adressFieldWords = adressField.toString().split("-");
  }

  //console.log("post split: ", adressFieldWords, adressField)

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

  let bestGuess;
  //if nothing was found, put these in the unknown box
  if (ids.length === 0) {
    bestGuess = "unknown";
    return bestGuess;
  } else {
    let bestGuessIndex = ids[occurances.indexOf(Math.max(...occurances) )]
    let bestGuess = sitesObject.sites[bestGuessIndex].names[0]
    
    console.log("closest match for ", adressField , ", is" , bestGuess)
    return bestGuess;
  }
 
  
}







async function buildSiteTransferRecords(sitesObject, passedTransferObject) {

  //setup list of unknown sites that don't match any words
  sitesObject.uncertainSites = [];

  for (x in passedTransferObject.days) {
    for (y in passedTransferObject.days[x].transfers) {

      let transfer = passedTransferObject.days[x].transfers[y];
      let source;
      let destination;

      if (transfer.source && transfer.destination) {
        source = transfer.source.toLowerCase();
        destination = transfer.destination.toLowerCase();

        if ( !sitesObject.nameLookupMap.has(source) ) { //if inputting the whole faccility name doesn't yield a match we start trying to match individual words
          sitesObject.uncertainSites.push(source)
        }
  
        if ( !sitesObject.nameLookupMap.has(destination) ) { //if inputting the whole faccility name doesn't yield a match we start trying to match individual words
          sitesObject.uncertainSites.push(destination)
        }

      }
    }
  }

  sitesObject.uncertainSites = new Set(sitesObject.uncertainSites);
  sitesObject.uncertainSites = [... sitesObject.uncertainSites];  //convert to array
  console.log("\n  Uncertain sites: ", sitesObject.uncertainSites);

  for (x in sitesObject.uncertainSites) {

    let site = sitesObject.uncertainSites[x];
    
    let siteNameBestGuess =  siteNameFuzzyMatching(sitesObject, site);

    //console.log("\n\n SITE BEST GUESS: ", siteNameBestGuess ,"\n\n")

    let siteIndex = sitesObject.nameLookupMap.get(siteNameBestGuess);

    //console.log("\n\n", site ,"\n");
    //console.log("for  ", siteNameBestGuess ," new site index: ", siteIndex)
    sitesObject.nameLookupMap.set(site, siteIndex);
  }

  // Now that sites are updated so transfers go to the (mostly) right ones, we process transfers.
 

  for (x in passedTransferObject.days) {
    for (y in passedTransferObject.days[x].transfers) {
      let transfer = passedTransferObject.days[x].transfers[y]

      if (transfer && transfer.source && transfer.destination) {
        let source = transfer.source.toLowerCase();
        let destination = transfer.destination.toLowerCase();

        transfer.matchedSource = source;
        transfer.matchedDestination = destination;
        //console.log("\nsource: ", source, "\ndest: ", destination, "\n\ntransfer: ", transfer)
      }
    }
  }

  return [sitesObject, passedTransferObject]

}







function calculateSiteMetaStats(passedSitesObject) {

  // console.log("      network total:          ", passedSitesObject.networkTransferTotal)
  // console.log("\n\n\n      passed site for meta calc:          \n\n", passedSitesObject.geoJsonSites.features)



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

  //console.log("\n\n\n\n\n\n\n\n geoJSON debug", passedTransferObject, "\n\n\n\n\n\n\n")

  let transfersByDay = passedTransferObject[1].days
  let failCount = 0;

  let geoJSONtransfers = []
  // let animatedgeoJsonTransfers = []

  for (let x=0; x < transfersByDay.length; x++) {

    for (let y=0; y < transfersByDay[x].transfers.length; y++) {
      let transfer = transfersByDay[x].transfers[y]
      //console.log("tranfer matched: ", transfer.matchedSource, transfer.matchedDestination)

      if (transfer.matchedSource && transfer.matchedDestination && passedTransferObject[0].nameLookupMap.get(transfer.matchedSource) &&passedTransferObject[0].nameLookupMap.get(transfer.matchedDestination)) {
        
        //.log("tranfer matched: ", transfer.matchedSource, transfer.matchedDestination)

          let siteIndexSent = passedTransferObject[0].nameLookupMap.get(transfer.matchedSource)
          let sendingSite = passedTransferObject[0].sites[siteIndexSent]

          let siteIndexRecv = passedTransferObject[0].nameLookupMap.get(transfer.matchedDestination)
          let receivingSite = passedTransferObject[0].sites[siteIndexRecv]

          //console.log("both sies::   ", siteIndexRecv)
          
          let coordTo = [parseFloat(receivingSite.longitude), parseFloat(receivingSite.latitude)]
          let coordFrom = [parseFloat(sendingSite.longitude), parseFloat(sendingSite.latitude)]
    
          let transferTransaction = {"type":"Feature", "geometry":{ "type": "LineString", "coordinates": [ [coordFrom[0], coordFrom[1]], [coordTo[0], coordTo[1]] ]}, "properties": {"from": sendingSite.names[0], "to": receivingSite.names[0], "toLong":coordTo[0], "toLat":coordTo[1], "fromLong":coordFrom[0], "fromLat":coordFrom[1], "size": parseInt(transfer.file_size), "duration": "Ask Zack how to get", "date": transfer.start_time, "from": transfer.source, "to": transfer.destination, "speed": transfer["transfer_speed(MB/s)"] } }
          
          // let transferTransactionAnim = {"type":"Feature", "geometry":{ "type": "LineString", "coordinates": [ [coordFrom[0], coordTo[0], 0, firstOfMonthDateUnixTime], [coordFrom[1], coordTo[1], 0, MiddleOfMonthDateUnixTime ], [coordFrom[1], coordTo[1], 0, EndOfMonthDateUnixTime ] ] }, "properties": { "from": sendingSite.names[0], "to": receivingSite.names[0], "toLong":coordTo[0], "toLat":coordTo[1], "fromLong":coordFrom[0], "fromLat":coordFrom[1], "size": parseInt(transfer.file_size), "duration": "Ask Zack how to get", "date": transfer.start_time, "from": transfer.source, "to": transfer.destination, "speed": transfer["transfer_speed(MB/s)"] } }
          // **** TODO ***** put in haversine equation to calculate middle interpolated coordinates

          geoJSONtransfers.push(transferTransaction)
          // animatedgeoJsonTransfers.push(transferTransactionAnim)
          // console.log ("pertinent info: ", transfer, siteIndex )
    
      }
    }
  }

  let transferObjectFinal={}

  // // console.log("\n\n\n\n\n\n\n\n geoJSON result", geoJSONtransfers, "\n\n\n\n\n\n\n")
  // // console.log("\n\n\n\n\n\n\n\n geoJSON *** ANIMATED TRIPS *** result", animatedgeoJsonTransfers[0].geometry.coordinates, "\n\n\n\n\n\n\n")
 

  let geoJsonMeta = {"type":"FeatureCollection", "features":geoJSONtransfers}
  //let geoJsonMetaAnim = {"type":"FeatureCollection", "features":animatedgeoJsonTransfers}

  transferObjectFinal.geoJson = geoJsonMeta;
  //transferObjectFinal.animated = geoJsonMetaAnim

  return transferObjectFinal
 }
















async function processController(searchParameters) {

  let configObject = await loadConfig();
  configObject.searchParameters = searchParameters;

  let sites = await buildSites();




  let transfers = await processSomeData(configObject)




  let identifiedTransfers = await buildSiteTransferRecords(sites, transfers)

  sites = identifiedTransfers[0]
  transfers = identifiedTransfers[1]

  let processedTransfers = await sortTransfers(sites, transfers)
  let finalObject = [calculateSiteMetaStats(processedTransfers[0]),processedTransfers[1]]
  let geoJsonTransferObject = await createGeoJsonTransferFile(finalObject)
  let omniObject = {"transferGeoJSON": geoJsonTransferObject, "siteOutputWithStats": finalObject[0].sites, "sitesGeoJSON": finalObject[0].geoJsonSites}

  //console.log(omniObject)

  return omniObject

}


// processController();

module.exports = {processController: processController}