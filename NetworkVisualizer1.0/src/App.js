//Developed by Lydia Brynmoor and Zachary Lee
import React, { useState } from "react";
import {
  Collapse,
  Button,
  Table,
  Dropdown,
  DropdownToggle,
  DropdownMenu,
  DropdownItem,
  Badge,
  Alert,
  Spinner,
  Card,
  CardImg,
  CardText,
  CardBody,
  CardTitle,
  CardSubtitle,
} from "reactstrap";
import ReactTooltip from "react-tooltip";
import ReactDOM from "react-dom";
import Tooltip from "react-simple-tooltip";
import DayPicker, { DateUtils } from "react-day-picker";
import DayPickerInput from "react-day-picker/DayPickerInput";
import "react-day-picker/lib/style.css";
import { Bar } from "react-chartjs-2";
import {
  ComposableMap,
  Geographies,
  Geography,
  Graticule,
  ZoomableGroup,
  Line,
  Marker,
} from "react-simple-maps";
import "./css/bootstrap.min.css";
import "./App.css";
// import siteData from "./data/duneSiteList.json"

// var fs = require('fs');

var resultsFound = false;
var siteUnclicked = true;
var failuresFound = false;
var periodTransfers = 0
var periodFailures = 0

const srGraphOptions = {
  scales: {
    xAxes: [
      {
        gridLines: {
          color: "rgba(0, 0, 0, 0)",
        },
      },
    ],
    yAxes: [
      {
        gridLines: {
          color: "rgba(0, 0, 0, 0)",
        },
      },
    ],
  },
  indexAxis: "y",
  // Elements options apply to all of the options unless overridden in a dataset
  // In this case, we are setting the border of each horizontal bar to be 2px wide
  elements: {
    bar: {
      borderWidth: 2,
    },
  },
  responsive: true,
  plugins: {
    legend: {
      position: "right",
      display: false,
    },
    title: {
      display: true,
      text: "Send Recieve Ratio",
    },
  },
};

function dateFormatConverter(passedDate) {
  const date = passedDate.toISOString().split("T")[0].replace(/-/g, "/");
  return date;
}

function checkIfResultsFound() {
  // console.log("results found flag says: " + resultsFound)

  if (resultsFound === undefined) {
    return;
  }

  if (resultsFound) {
    return "Results Found";
  } else {
    return "No Results";
  }
}

//Sites not properly listed in the CRIC database at the time of coding
//longitude first, then latitude
const markers = [
  // { markerOffset: 1, otherName:"", name: "Atlantis", coordinates: [-43, 32.6] },

  {
    markerOffset: 1,
    otherName: "",
    name: "RAL_ECHO",
    coordinates: [1.2, 51.6],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "QMUL",
    coordinates: [0.0404, 51.5241],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "SURFSARA",
    coordinates: [4.904, 52.368],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "GPGrid",
    coordinates: [-88.57, 41.24],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "GPGRID",
    coordinates: [-88.27, 41.84],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "CERN_PDUNE_CASTOR",
    coordinates: [6, 46],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "PRAGUE",
    coordinates: [14.469, 50.123],
  },
  {
    markerOffset: 1,
    otherName: "FNAL_DCACHE_TEST",
    name: "FNAL_DCACHE",
    coordinates: [-88.27, 41.84],
  },

  {
    markerOffset: 1,
    otherName: "BNL-SDCC-CE01",
    name: "DUNE_US_BNL_SDCC",
    coordinates: [-72.876311, 40.86794],
  },
  {
    markerOffset: 1,
    otherName: "DUNE_FR_CCIN2P3",
    name: "DUNE_FR_CCIN2P3_XROOTD",
    coordinates: [4.87, 45.78],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "NERSC",
    coordinates: [-122.272778, 37.871667],
  },
  {
    markerOffset: 1,
    othername: "WSU",
    name: "WSU - GRID_CE2",
    coordinates: [-83.067, 42.358],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "CERN_PDUNE_EOS",
    coordinates: [6.04, 46.23],
  },
  {
    markerOffset: 1,
    otherName: "US_FNAL",
    name: "GPGRID",
    coordinates: [-88.57, 41.24],
  },
  {
    markerOffset: 1,
    otherName:"RAL-PP",
    name:"UKI-SOUTHGRID-RALLPP",
    coordinates: [-1.31,51.57],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "T3_US_NERSC",
    coordinates: [-122.3, 37.867],
  }, //placeholder location 4-12-21 someone promised I'd get this, is it 2040 yet?
  {
    markerOffset: 1,
    otherName: "",
    name: "BR_CBPF",
    coordinates: [-43.174, -22.954],
  }, //placeholder location 4-12-21 someone promised I'd get this, is it 2040 yet?
  {
    markerOffset: 1,
    otherName: "",
    name: "CA_VICTORIA",
    coordinates: [-123.31, 48.47],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "IN_TIFR",
    coordinates: [72.806, 18.907],
  },
  { markerOffset: 1, otherName: "BNL", name: "US_BNL", coordinates: [-72, 40] },
  {
    markerOffset: 1,
    otherName: "FNAL",
    name: "US_FNAL",
    coordinates: [-88.255, 41.841],
  },
  {
    markerOffset: 1,
    otherName: "SU-ITS-CE2",
    name: "US_SU_ITS",
    coordinates: [-76.14, 43.04],
  },
  {
    markerOffset: 1,
    otherName: "SLATE_US_NMSU_DISCOVERY",
    name: "NMSU-DISCOVERY",
    coordinates: [-106.77, 32.31],
  },
  {
    markerOffset: 1,
    otherName: "JINR",
    name: "JINR_CONDOR_CE",
    coordinates: [37.196, 56.743],
  }, //TODO WRONG
  {
    markerOffset: 1,
    otherName: "",
    name: "BR_UNICAMP",
    coordinates: [-47.05691000711719, -22.81839974327466],
  },

  //stuff commented out below has been found in the API results from CRIC API so I figure we should favor that

  // { markerOffset: 1, name: "MANCHESTER", coordinates: [2.2, 53] },
  // { markerOffset: 1, name: "LANCASTER", coordinates: [-2.74, 54.012] },
  // { markerOffset: 1, name: "LIVERPOOL", coordinates: [-3, 53.4] },
  // { markerOffset: 1, name: "NIKHEF", coordinates: [4.951, 52.3] },
  // { markerOffset: 1, name: "IMPERIAL", coordinates: [0.17, 51.4] },
  // { markerOffset: 1, name: "QMUL", coordinates: [-0.041, 51.523] },
  // { markerOffset: 1, name: "RAL-PP", coordinates: [51.57, -1.31] },
];
const geoUrl = "./world-110m.json";

function App() {

  //Sets up objects used throughout the app that need to maintain an internal
  //state between calls
  const [transfers, setTransfers] = useState([]);
  const [failures, setFailures] = useState([]);
  const [individualSiteData, setIndividualSiteData] = useState([]);

  const [dateRange, setDateRange] = useState({
    from: undefined,
    to: undefined,
  });
  const [savedStartDate, setSavedStartDate] = useState();
  const [savedEndDate, setSavedEndDate] = useState();
  const [processingStatus, setProcessingStatus] = useState("Waiting for user entry");
  

  //Resets the "DateRange" object created earlier to a default of undefined
  const resetCalendarDateClick = () => {
    setDateRange({ from: undefined, to: undefined });
  };

  //If no or only one day has been selected, then sets the date range
  //to a range that is bounded by the "from" and "to" days.
  //If "to" has already been selected, then the date range gets reset
  //before anything else happens.
  const handleDateClick = (day) => {
    if (dateRange.to) {
      resetCalendarDateClick();
    } else {
      const range = DateUtils.addDayToRange(day, dateRange);
      setDateRange(range);
    }
  };

//Pulls the specific data regarding how much data a given site (selected via a
//passed index for a passed list of site data) and processes into a new format
//describing display parameters
  const populateSiteGraph = (passedSiteIndex, passedSites) => {
    // console.log(passedSites[passedSiteIndex].name + " recieve ratio: " + passedSites[passedSiteIndex].fractionOfDataReceived)

    const srGraphData = {
      labels: ["Sent", "Receive"],
      datasets: [
        {
          label: "Transmit & Receive Ratio",
          data: [0, 0],
          backgroundColor: [
            "rgba(255, 99, 132, 0.2)",
            "rgba(54, 162, 235, 0.2)",
          ],
          borderColor: ["rgba(255, 99, 132, 1)", "rgba(54, 162, 235, 1)"],
          borderWidth: 1,
        },
      ],
    };

    const sendRatio = passedSites[passedSiteIndex].fractionOfDataReceived;
    const recvRatio = passedSites[passedSiteIndex].fractionOfDataSent;

    // console.log("printed tx/rx = " + sendRatio + " " + recvRatio)

    srGraphData.datasets[0].data[1] =
      passedSites[passedSiteIndex].fractionOfDataReceived;
    srGraphData.datasets[0].data[0] =
      passedSites[passedSiteIndex].fractionOfDataSent;

    // console.log("model tx/rx = " + srGraphData.datasets[0].data[0] + " " + srGraphData.datasets[0].data[1])

    return srGraphData;
  };







































  //Sets up the site list based on CRIC data and hardcoded overrides
  //then
  const parseSiteList = () => {
    console.log(
      `fetching DUNE site date from backend http://localhost:3001/getsites`
    );
    fetch(`http://localhost:3001/getsites`, {crossDomain:true})
      .then((res) => res.json())
      .then((res) => {
        //res.root.atp_site[0].$.latitude
        // console.log(res.root.atp_site)
        console.log("Dune Site list updated as of: " + res.root.last_update[0]);

        const mappedSites = res.root.atp_site.map((item) => {
          var otherNameString = item.group[1].$.name;
          let re = new RegExp("[A-Z][A-Z]_");

          // console.log(otherNameString, [parseFloat(item.$.longitude),parseFloat(item.$.latitude)])

          //getting rid of UK_ US_ CA_ etc prefixes below
          if (re.test(otherNameString)) {
            otherNameString = otherNameString.substring(3).toUpperCase();
          }

          if (
            parseFloat(item.$.longitude) === 0 || 0.0 &&
            parseFloat(item.$.latitude) === 0 || 0.0
          ) {
            console.log("0,0 entry detected: " + otherNameString);
          }

          return {
            markerOffset: 1,
            name: item.$.name.toUpperCase(),
            otherName: otherNameString,
            coordinates: [
              parseFloat(item.$.longitude),
              parseFloat(item.$.latitude),
            ],
          };
        });

        //overwite the downloaded list with any hardcoded ones we have, then combine the rest
        markers.forEach((item, i) => {
          const matchId = mappedSites.findIndex(
            (element) =>
              element.name === item.name || element.name === item.otherName
          );

          if (matchId > -1) {
            console.log("replacing: ")
            console.log(mappedSites[matchId])
            console.log("with ")
            console.log(item)
            mappedSites[matchId] = item;
          } else {
            mappedSites.push(item);
          }
        });

        //append these to the existing hardcoded sites
        // mappedSites.forEach(x =>  markers.push(x))

        console.log(mappedSites);
        console.log(markers);
        // console.log(res.root.atp_site[0].group[1].$.name)

        parseTransfers(mappedSites);
        parseFailures(mappedSites);

        // if (showFailureMode){
        //   console.log("getting transfers")
        //   parseTransfers(mappedSites);
        // }
        // else {
        //   console.log("getting failures")
        //   parseFailures(mappedSites);
        // }
      });
  };










  const parseTransfers = (passedSites) => {

    resultsFound = false;
    setProcessingStatus("Processing query...");
    //Ensures that if we're only passing one day to the backend, we don't
    //pass it any undefined values
    if (dateRange.to === undefined) {
      dateRange.to = dateRange.from;
    }


    //Updates our two persistent date objects
    setSavedStartDate(dateFormatConverter(dateRange.from));
    setSavedEndDate(dateFormatConverter(dateRange.to));

    //Determines mode to pass based on if checkup mode is enabled
    let mode = "1";
    if (!showCheckupMode){
      mode = "0";
    }

    var dateParameters = new URLSearchParams({
      startDate: dateFormatConverter(dateRange.from),
      endDate: dateFormatConverter(dateRange.to),
      searchMode: mode,
    });

    console.log(
      `fetching transfer data from: http://localhost:3001/getTransfers?` +
        dateParameters.toString()
    );

    //Passes date parameters to and calls the routed script that calls the backend
    //python script, then waits for completion
    fetch(`http://localhost:3001/getTransfers?` + dateParameters.toString(), {crossDomain:true})
      //TODO: set a timeout on the promise above so that if there is just NO out.json file it won't hang

      .then((res) => res.json())
      .then((res) => {
        let allTransferedAmount = 0;

        console.log("transfer data result: ");
        console.log(res.data);

        //Checks that the "name" property exists in our JSON (effectively
        //checking for correct formatting) and makes sure that it hasn't been
        //passed the "There's been an error" template
        if (
          res.data[0].hasOwnProperty("name") &&
          res.data[0].source !== "ERROR"
        ) {
          //TODO: modify this so that if the search fails we don't crash, maybe try/accept or if statement

          //Processes all sent transfers
          var sourceLocationAlt = "None";
          var destinationLocationAlt = "None";
          var mysteryCoordinates = [42,42];

          const mappedTransfers = res.data.map((entry) => {
            const sourceLocation = passedSites.find(
              (location) => entry.source === location.name || entry.source === location.otherName
            );

            const destinationLocation = passedSites.find(
              (location) => entry.destination === location.name || entry.destination === location.otherName
            );

            const speedInMB = parseFloat(entry["transfer_speed(MB/s)"]).toFixed(
              2
            );

            //Tracks the total amount of data transferred for this time period
            //independent of sites
            allTransferedAmount += entry.file_size;
            periodTransfers ++

            // console.log(entry.file_size)

            //Checks for issues with the data and reformats it for
            //writing to the map
            if (!sourceLocation && !destinationLocation) {
              return {
                from: sourceLocationAlt,
                to: destinationLocationAlt,
                fromCoord: mysteryCoordinates,
                toCoord: mysteryCoordinates,
                speedInMB: speedInMB,
                sentToDestSizeMB: entry.file_size / 1048576,
                filesTransfered: 0
              };
            } else if (!sourceLocation) {
              return {
                from: sourceLocationAlt,
                to: destinationLocation.name,
                fromCoord: mysteryCoordinates,
                toCoord: destinationLocation.coordinates,
                speedInMB: speedInMB,
                sentToDestSizeMB: entry.file_size / 1048576,
                filesTransfered: 0
              };
            } else if (!destinationLocation) {
              return {
                from: sourceLocation.name,
                to: destinationLocationAlt,
                fromCoord: sourceLocation.coordinates,
                toCoord: mysteryCoordinates,
                speedInMB: speedInMB,
                sentToDestSizeMB: entry.file_size / 1048576,
                filesTransfered: 0
              };
            } else {
              return {
                from: sourceLocation.name,
                to: destinationLocation.name,
                fromCoord: sourceLocation.coordinates,
                toCoord: destinationLocation.coordinates,
                speedInMB: speedInMB,
                sentToDestSizeMB: entry.file_size / 1048576,
                filesTransfered: 0
              };
            }
          });

          console.log("mapped transfers: ");
          console.log(mappedTransfers);

          allTransferedAmount /= 1048576; //adjusting to mb

          setTransfers(mappedTransfers);

          // console.log(markers)

          const collectionOfSiteObjects = passedSites.map((x) => {
            return {
              ...x,
              totalSent: 0,
              totalReceived: 0,
            };
          });

          console.log("collection site objects:");
          console.log(collectionOfSiteObjects);

          collectionOfSiteObjects.forEach((entry) => {
            res.data
              .filter((jsonThing) => {
                return jsonThing.source === entry.name;
              })
              .forEach((item, i) => {
                entry.totalSent += item.file_size / 1048576; //dividing the total bytes into megabytes 1024 b to kb, 1024 kb to mb
              });

            res.data
              .filter((jsonThing) => {
                return jsonThing.destination === entry.name;
              })
              .forEach((item, i) => {
                entry.totalReceived += item.file_size / 1048576; //dividing the total bytes into megabytes 1024 b to kb, 1024 kb to mb
              });

            entry.fractionOfDataSent = entry.totalSent / allTransferedAmount;
            entry.fractionOfDataReceived =
            entry.totalReceived / allTransferedAmount;

            entry.filesTransfered ++

            entry.totalSent = parseFloat(entry.totalSent).toFixed(2);
            entry.totalReceived = parseFloat(entry.totalReceived).toFixed(2);
            entry.fractionOfDataSent = parseFloat(
              entry.fractionOfDataSent
            ).toFixed(4);
          });

          resultsFound = true;
          setProcessingStatus("Results found");
          // console.log("Results found:")
          // console.log(collectionOfSiteObjects);

          //Sets the state object holding our search results to the new
          //results we just processed
          // setIndividualSiteData(collectionOfSiteObjects);
          parseFailures(collectionOfSiteObjects);
        } else {
          resultsFound = false;
          setProcessingStatus("No results found");
          setIndividualSiteData([]);
          console.log("No results returned for DUNE transfers");
          console.log(resultsFound);
        }

        //resetCalendarDateClick();
      });
  };























  //Parses through a set of failed transfer data
  const parseFailures = (passedSites) => {
    resultsFound = false;
    setProcessingStatus("Processing query...");

    //Makes sure we don't pass something undefined to our search
    if (dateRange.to === undefined) {
      dateRange.to = dateRange.from;
    }

    setSavedStartDate(dateFormatConverter(dateRange.from));
    setSavedEndDate(dateFormatConverter(dateRange.to));

    //Determines mode based on if checkup mode is enabled
    let mode = "3";
    if (!showCheckupMode){
      mode = "4";
    }

    //Sets up our date parameters
    var dateParameters = new URLSearchParams({
      startDate: dateFormatConverter(dateRange.from),
      endDate: dateFormatConverter(dateRange.to),
      searchMode: mode,
    });

    console.log(
      `fetching failure data from: http://${window.location.hostname}:3001/getFails?` +
        dateParameters.toString()
    );

    //Passes our date parameters to the routed script that calls the es_client script
    //in failures mode, then waits for completion
    fetch(`http://${window.location.hostname}:3001/getFails?` + dateParameters.toString(), {crossDomain:true})
      //TODO: set a timeout on the promise above so that if there is just NO out.json file it won't hang

      .then((res) => res.json())
      .then((res) => {
        let totalNumberFailed = 0;

        console.log("failure data result: ");
        console.log(res.data);

        //Checks to make sure we have a properly formatted, non-error
        //template
        if (
          res.data[0].hasOwnProperty("name") &&
          res.data[0].source !== "ERROR"
        ) {
          //TODO: modify this so that if the search fails we don't crash, maybe try/accept or if statement


          var sourceLocationAlt = "None";
          var destinationLocationAlt = "None";
          var mysteryCoordinates = [42,42];

          const mappedFailures = res.data.map((entry) => {
            const sourceLocation = passedSites.find(
              (location) => entry.source === location.name || entry.source === location.otherName
            );

            const destinationLocation = passedSites.find(
              (location) => entry.destination === location.name || entry.destination === location.otherName
            );

            const failureCount = entry.count

            totalNumberFailed += failureCount;
            periodFailures ++

            // console.log("******** fails: ", totalNumberFailed ,"     **********")
            // console.log(entry)
            // console.log(entry.file_size)
            //Reformats the data depending on which locations in the transfer
            //were valid/known
            if (!sourceLocation && !destinationLocation) {
              return {
                from: sourceLocationAlt,
                to: destinationLocationAlt,
                fromCoord: mysteryCoordinates,
                toCoord: mysteryCoordinates,
                failCount:failureCount
              };
            } else if (!sourceLocation) {
              return {
                from: sourceLocationAlt,
                to: destinationLocation.name,
                fromCoord: mysteryCoordinates,
                toCoord: destinationLocation.coordinates,
                failCount:failureCount
              };
            } else if (!destinationLocation) {
              return {
                from: sourceLocation.name,
                to: destinationLocationAlt,
                fromCoord: sourceLocation.coordinates,
                toCoord: mysteryCoordinates,
                failCount:failureCount
              };
            } else {
              return {
                from: sourceLocation.name,
                to: destinationLocation.name,
                fromCoord: sourceLocation.coordinates,
                toCoord: destinationLocation.coordinates,
                failCount:failureCount
              };
            }
          });

          setProcessingStatus("Results found");

          console.log("mapped failures: ");
          console.log(mappedFailures);

          setFailures(mappedFailures);

          console.log(markers)

          const collectionOfSiteObjects = passedSites.map((x) => {
            return {
              ...x,
              totalFailuresSent: 0,
              totalFailuresReceived: 0,
            };
          });

          // console.log("collection site objects:");
          // console.log(collectionOfSiteObjects);

          collectionOfSiteObjects.forEach((entry) => {
            res.data
              .filter((jsonThing) => {
                return jsonThing.source === entry.name;
              })
              .forEach((item, i) => {
                entry.totalFailuresSent += item.count
              });
          
            res.data
              .filter((jsonThing) => {
                return jsonThing.destination === entry.name;
              })
              .forEach((item, i) => {
                entry.totalFailuresReceived += item.count
              });
          
            entry.fractionOfSendErrors = entry.totalFailuresSent / totalNumberFailed;
            entry.fractionOfRecErrors = entry.totalFailuresReceived / totalNumberFailed;
            // entry.failRatio = /entry.failCount
          });

          resultsFound = true;
          // console.log("Results found:")
          // console.log(collectionOfSiteObjects);
          
          setIndividualSiteData(collectionOfSiteObjects);
        }
        // else {
        //   setProcessingStatus("No results found");
          // failuresFound = false;
          // console.log("No results returned for DUNE transfers");
          // console.log(resultsFound);
        


      });
  };






  function calculateTotalFailRatio(){
    const failRatio = periodFailures/(periodTransfers+periodFailures)
    console.log("\n\ntransfers: ", periodTransfers, "      failures: ", periodFailures)
    console.log("failure ration this period: ", failRatio)
    return failRatio
  }


  function calculateAllSiteFailRatios(passedFailSites){

    for (let x in passedFailSites) {
      passedFailSites[x].failRatio = passedFailSites[x].failCount / periodFailures
      console.log("period faiulres for ",passedFailSites[x].from , " =  ", passedFailSites[x].failRatio)

      return
    }
  }













  const processTransferAndCollapse = () => {
    parseSiteList();
    toggle();
  };

  const collapseLegend = () => {
    toggleLegendCard();
  };

  const changeLegendText = () => {

    if (legendOpen) {
      return "Hide Legend"
    }
    else {
      return "Show Legend"
    }
  }

  const changeCheckupText = () => {

    if (!showCheckupMode) {
      return "View Network Test"
    }
    else {
      return "View Normal Results"
    }
  }

  const changeFailureText = () => {

    if (!showFailureMode) {
      return "View Failed Transfers"
    }
    else {
      return "View Completed Transfers"
    }
  }

  const getFailures = () => {
    console.log(showFailureMode)
    parseSiteList();
  }

  const [tooltip, setTooltip] = useState("");
  const [mapPosition, setMapPosition] = useState({
    coordinates: [0, 0],
    zoom: 1,
  });
  const [isOpen, setIsOpen] = useState(false);
  const [searchResultStatus, setSearchResultStatus] = useState(); //TODO actually get this working so empty text returned unless search complete, then return results found or not
  const [selectedSiteIndex, setSelectedSiteIndex] = useState();
  const [dropdownOpen, setDropDownOpen] = useState(false);
  const [showCheckupMode, setshowCheckupMode] = useState(false);
  const [showFailureMode, setshowFailureMode] = useState(false);
  const [legendOpen, setLegendOpen] = useState(false);

  const toggle = () => setIsOpen(!isOpen);
  const toggleLegendCard = () => setLegendOpen(!legendOpen);
  const toggleDropDown = () => setDropDownOpen(!dropdownOpen);
  const toggleCheckupMode = () => setshowCheckupMode(!showCheckupMode);
  const toggleFailMode = () => setshowFailureMode(!showFailureMode);

  const renderMap = () => {
    console.log("checking, failure mode is: " + showFailureMode);
    if (!showFailureMode) {
      return renderTransferMap();
    } else {
      calculateAllSiteFailRatios();
      return renderFailMap();
    }
  };





  const renderFailMap = () => {
    return               <div id={"map"}>
                          <ComposableMap data-tip=""   projectionConfig={{
    scale: 155,
    rotation: [-11, 0, 0],
  }}
  width={800}
  height={375}
  style={{ width: "100%", height: "auto" }}  >

                            <ZoomableGroup
                              zoom={0.90}
                              center={[0, 0]}
                              onMoveEnd={setMapPosition}
                              maxZoom={24}
                            >
                              <Geographies geography={geoUrl}>
                                {({ geographies }) =>
                                  geographies.map((geo) => (
                                    <Geography
                                      key={geo.rsmKey}
                                      geography={geo}
                                      fill="#9998A3"
                                      stroke="#EAEAEC"
                                      strokeWidth={Math.min(3/mapPosition.zoom,0.45)}
                                      style={{
                                        default: { outline: "none" },
                                        hover: { outline: "none" },
                                        pressed: { outline: "none" },
                                        onClick: { outline: "none" },
                                      }}
                                    />
                                  ))
                                }
                              </Geographies>
                              {failures.map((oneOfThem, i) => {
                                return (
                                  <>
                                  <Line
                                    key={`a-${i}`}
                                    to={oneOfThem.toCoord}
                                    from={oneOfThem.fromCoord}
                                    stroke="#000000"
                                    strokeWidth={Math.max((1.5/mapPosition.zoom)+0.05, 0.25)}
                                    onMouseEnter={() => {
                                      // setTooltip(`Last AVG speed: ${oneOfThem.speedInMB} MB/s`);       //need to consider what, if any, we want to put in tooltip over transfer line
                                    }}
                                    onMouseLeave={() => {
                                      setTooltip("");
                                    }}
                                  />
                                  <Line
                                    key={`b-${i}`}
                                    to={oneOfThem.toCoord}
                                    from={oneOfThem.fromCoord}
                                    stroke="#fdff33"
                                    strokeWidth={Math.max(1.5/mapPosition.zoom, 0.2)}
                                    onMouseEnter={() => {
                                      // setTooltip(`Last AVG speed: ${oneOfThem.speedInMB} MB/s`);       //need to consider what, if any, we want to put in tooltip over transfer line
                                    }}
                                    onMouseLeave={() => {
                                      setTooltip("");
                                    }}
                                  />
                                </>
                                );
                              })}
                              //could add another line here ^ to show ratio of send
                              //vs recieve between individual sites but it's one
                              //within another not side by side so doesn't look great.
                              {individualSiteData.map(
                                (
                                  {
                                    name,
                                    coordinates,
                                    markerOffset,
                                    fractionOfSendErrors,
                                    fractionOfRecErrors
                                  },
                                  i
                                ) => {
                                  console.log('fraction of send errors', fractionOfSendErrors)
                                  return (
                                  <Marker
                                    key={`m-${i}`}
                                    coordinates={coordinates}
                                    onClick={() => {
                                      //alert("click action here");
                                      //alert("radius click")
                                    }}
                                  >
                                    <circle
                                      r={40 * fractionOfSendErrors}
                                      fill="rgba(255,0,0,0.4)"
                                    />{" "}
                                    //send fraction circle
                                    <circle
                                      r={40 * fractionOfRecErrors}
                                      fill="rgba(12,123,220,0.4)"
                                    />{" "}
                                    //recieve fraction circle
                                  </Marker>
                                )}
                              )}
                              {individualSiteData.map(
                                (
                                  {
                                    name,
                                    coordinates,
                                    markerOffset,
                                    fractionOfSendErrors,
                                    fractionOfRecErrors,
                                  },
                                  i
                                ) => (
                                  <Marker
                                    key={i}
                                    coordinates={coordinates}
                                    onClick={() => {
                                      setSelectedSiteIndex(i);
                                    }}
                                    onMouseEnter={() => {
                                      setTooltip(
                                        "failure tool tip"
                                        // `${name}<br> TX: ${totalSent} MB <br>  RX: ${totalReceived} MB`
                                      );
                                    }}
                                    onMouseLeave={() => {
                                      setTooltip("");
                                    }}
                                  >
                                    <circle
                                      r={2.2 * calculateTotalFailRatio() / mapPosition.zoom}
                                      fill="rgba(75,0,146,1)"
                                    />
                                  </Marker>
                                )
                              )}
                            </ZoomableGroup>
                          </ComposableMap>
                        </div>
  };










  const renderTransferMap = () => {
    return                     <div id={"map"}>
                          <ComposableMap data-tip=""   projectionConfig={{
    scale: 155,
    rotation: [-11, 0, 0],
  }}
  width={800}
  height={375}
  style={{ width: "100%", height: "auto" }}  >

                            <ZoomableGroup
                              zoom={0.90}
                              center={[0, 0]}
                              onMoveEnd={setMapPosition}
                              maxZoom={24}
                            >
                              <Geographies geography={geoUrl}>
                                {({ geographies }) =>
                                  geographies.map((geo) => (
                                    <Geography
                                      key={geo.rsmKey}
                                      geography={geo}
                                      fill="#9998A3"
                                      stroke="#EAEAEC"
                                      strokeWidth={Math.min(3/mapPosition.zoom,0.45)}
                                      style={{
                                        default: { outline: "none" },
                                        hover: { outline: "none" },
                                        pressed: { outline: "none" },
                                      }}
                                    />
                                  ))
                                }
                              </Geographies>
                              {transfers.map((oneOfThem, i) => {
                                return (
                                  <>
                                  <Line
                                    key={`a-${i}`}
                                    to={oneOfThem.toCoord}
                                    from={oneOfThem.fromCoord}
                                    stroke="#000000"
                                    strokeWidth={Math.max((1.5/mapPosition.zoom)+0.05, 0.25)}
                                    onMouseEnter={() => {
                                      // setTooltip(`Last AVG speed: ${oneOfThem.speedInMB} MB/s`);       //need to consider what, if any, we want to put in tooltip over transfer line
                                    }}
                                    onMouseLeave={() => {
                                      setTooltip("");
                                    }}
                                  />
                                  <Line
                                    key={`b-${i}`}
                                    to={oneOfThem.toCoord}
                                    from={oneOfThem.fromCoord}
                                    stroke="#F53"
                                    strokeWidth={Math.max(1.5/mapPosition.zoom, 0.2)}
                                    onMouseEnter={() => {
                                      // setTooltip(`Last AVG speed: ${oneOfThem.speedInMB} MB/s`);       //need to consider what, if any, we want to put in tooltip over transfer line
                                    }}
                                    onMouseLeave={() => {
                                      setTooltip("");
                                    }}
                                  />
                                </>
                                );
                              })}
                              //could add another line here ^ to show ration of send
                              //vs recieve between individual sites but it's one
                              //within another not side by side so doesn't look great.
                              {individualSiteData.map(
                                (
                                  {
                                    name,
                                    coordinates,
                                    markerOffset,
                                    totalSent,
                                    fractionOfDataSent,
                                    fractionOfDataReceived,
                                  },
                                  i
                                ) => (
                                  <Marker
                                    key={`a-${i}`}
                                    coordinates={coordinates}
                                    onClick={() => {
                                      //alert("click action here");
                                      //alert("radius click")
                                    }}
                                  >
                                    <circle
                                      r={40 * fractionOfDataSent}
                                      fill="rgba(87,235,51,0.4)"
                                    />{" "}
                                    //send fraction circle
                                    <circle
                                      r={40 * fractionOfDataReceived}
                                      fill="rgba(12,123,220,0.4)"
                                    />{" "}
                                    //recieve fraction circle
                                  </Marker>
                                )
                              )}
                              {individualSiteData.map(
                                (
                                  {
                                    name,
                                    coordinates,
                                    markerOffset,
                                    totalSent,
                                    totalReceived,
                                    fractionOfDataSent,
                                    fractionOfDataReceived,
                                  },
                                  i
                                ) => (
                                  <Marker
                                    key={`b-${i}`}
                                    coordinates={coordinates}
                                    onClick={() => {
                                      setSelectedSiteIndex(i);
                                    }}
                                    onMouseEnter={() => {
                                      setTooltip(
                                        `${name}<br> TX: ${totalSent} MB <br>  RX: ${totalReceived} MB`
                                      );
                                    }}
                                    onMouseLeave={() => {
                                      setTooltip("");
                                    }}
                                  >
                                    <circle
                                      r={2.2 / mapPosition.zoom}
                                      fill="rgba(75,0,146,1)"
                                    />
                                  </Marker>

                                )
                              )}
                            </ZoomableGroup>
                          </ComposableMap>
                        </div>
  };



  return (
    <div class="container-fluid">
      <div class="row">
        <div class="col-md-12">
          <div class="row" id="titleRow">
            <div class="col-md-12">
              <div class="page-header">

              <div class="row">
                <div class="col-md-8">
                  <h1>DUNE Network Monitor</h1>
                  <h6>Interactive Transfer Visualization Map</h6>
                </div>

                <div class="col-md-4">
                  <img src="https://www.dunescience.org/wp-content/uploads/2016/12/dune-horiz-logo-lg.png" id="duneLogoPic"></img>
                </div>
              </div>

              </div>
            </div>
          </div>
          <div class="row" id="mapTitleAnStatusRow">
            <div class="col-md-9" id="mainSectionCol">
              <div class="row" id="legendRow">
                <div class="col-md-12" id="legendCardCol">
                <Card id="legendCard">

                  <CardBody>

                    <div class="row">
                      <div class="col-md-12">
                        <CardTitle class="cardTitle" tag="h5">
                          Legend
                          <Button id="collapseLegendButton" color="primary" onClick={collapseLegend}>
                            {changeLegendText()}
                          </Button>
                        </CardTitle>
                      </div>

                    </div>

                      <Collapse isOpen={legendOpen}>
                    <div>
                    <CardSubtitle tag="h6" className="mb-2 text-muted">
                      These symbols represent the flow of data, and the
                      send/recieve ratio of different sites
                    </CardSubtitle>

                    <CardText>
                      <p>
                        Dune institutions are represented on the world map by
                        small purple dots while transfers between these sites
                        are represented by orange curves connecting the two.
                      </p>{" "}
                      <p>
                        The green and blue circles represent the ratio of data
                        sent and recieved (respectively) out of all transfered
                        during that period.{" "}
                      </p>{" "}
                      <p>
                        {" "}
                        The larger the colored radius around a site, the greater
                        the fraction of all data transfered during the queried
                        time period it was reponsible for.{" "}
                      </p>
                    </CardText>

                    <div class="col-md-12">
                      <div class="row">
                        <div class="col-sm-3 centAlignCol">
                          <h6>Data Sent</h6>

                          <svg height="100" width="100">
                            <circle
                              cx="50"
                              cy="50"
                              r="50"
                              stroke="black"
                              stroke-width="0"
                              fill="rgba(0,235,51,0.4)"
                            />
                            Sorry, your browser does not support inline SVG.
                          </svg>
                        </div>
                        <div class="col-sm-3 centAlignCol">
                          <h6>Data Received</h6>
                          <svg height="100" width="100">
                            <circle
                              cx="50"
                              cy="50"
                              r="50"
                              stroke="black"
                              stroke-width="0"
                              fill="rgba(12,123,220,0.4)"
                            />
                            Sorry, your browser does not support inline SVG.
                          </svg>
                        </div>
                        <div class="col-sm-3 centAlignCol">
                          <h6>Dune Institution</h6>
                          <svg height="100" width="100">
                            <circle
                              cx="50"
                              cy="50"
                              r="10"
                              stroke="black"
                              stroke-width="0"
                              fill="rgba(75,0,146,1)"
                            />
                            Sorry, your browser does not support inline SVG.
                          </svg>
                        </div>
                        <div class="col-sm-3 centAlignCol">
                          <h6>Transfer Path</h6>

                          <svg viewBox="0 0 100 40" version="1.1">
                            <line
                              x1="20"
                              y1="19"
                              x2="80"
                              y2="19"
                              stroke="black"
                              stroke-width="2"
                              stroke="#F53"
                            />
                          </svg>
                        </div>
                      </div>

                    </div>
                    </div>
                      </Collapse>



                  </CardBody>

                </Card>
                </div>

              </div>
              <div class="row" id="mapRow">
                <div class="col-md-12" id="mapCol">
                <Card id="mapCard">
                  <CardImg top width="100%" />
                  <CardBody>
                    <div class="row">
                      <div class="col-md-8">
                        <CardTitle class="cardTitle" tag="h4">
                          Transfer Map{" "}
                        </CardTitle>
                      </div>
                    </div>

                    <CardSubtitle tag="h6" className="mb-2 text-muted">
                      {" "}
                    </CardSubtitle>
                    <CardText>

                    </CardText>

                    <div class="row">
                      <div class="col-md-12">
                        {renderMap()}
                        <ReactTooltip html={true}>{tooltip}</ReactTooltip>
                      </div>
                    </div>
                  </CardBody>
                </Card>
                </div>
              </div>
              <div class="row" id="listRow">
                <div class="col-md-12">




                <Card id="statusCard">
                  <div class="row">
                    <div class="col-md-12">
                      <CardTitle class="cardTitle" tag="h5">
                        Log of Transfers
                      </CardTitle>
                    </div>
                  </div>

                  <div class="row">
                    <div class="col-md-12">
                      <CardBody>
                        <Table>
                          <thead>
                            <tr>
                              <th>To</th>
                              <th>From</th>
                              <th>Speed</th>
                              <th>Filesize</th>
                            </tr>
                          </thead>
                          <tbody>
                            {transfers.map((transfer, i) => {
                              return (
                                <tr key={i}>
                                  <td>{transfer.to}</td>
                                  <td>{transfer.from}</td>
                                  <td>{transfer.speedInMB}</td>
                                  <td>{transfer.sentToDestSizeMB}</td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </Table>
                      </CardBody>
                    </div>
                  </div>
                </Card>







                </div>
              </div>
            </div>
            <div class="col-md-3 position-fixed" id="fixedRightCol">
              <div class="rightSideFixed">
                <div class="row" id="statusCardRow">
                  <div class="col-md-12">
                    <Card id="statusCard">
                      <div class="row">
                        <div class="col-md-12">
                          <CardTitle class="cardTitle" tag="h5">
                            Status
                          </CardTitle>
                        </div>
                      </div>

                      <div class="row">
                        <div class="col-md-6">
                          <Badge color="success">Dune CRIC API</Badge>
                        </div>
                        <div class="col-md-6">
                          <Badge color="success">Elasticsearch DB</Badge>
                        </div>
                      </div>

                      <div class="row">
                        <div class="col-md-12">
                          <CardBody></CardBody>
                        </div>
                      </div>
                    </Card>
                  </div>
                </div>

                <div class="row" id="optionsCardRow">
                  <div class="col-md-12">
                    <Card id="optionsCard">
                      <div class="row">
                        <div class="col-md-12">
                          <CardTitle class="cardTitle" tag="h5">
                            Search Options
                          </CardTitle>
                        </div>
                      </div>
                      <div class="row" id="checkupModeSwitchRow">
                        <Button id="getCheckupButton" color="primary" onClick={() => {toggleCheckupMode();}}>
                          {changeCheckupText()}
                        </Button>
                      </div>
                      <div class="row">
                        <div class="col-md-12">
                          <CardBody></CardBody>
                        </div>
                      </div>
                      <div class="row" id="mapModeSwitchRow">
                        <Button id="getFailuresButton" color="primary" onClick={() => {toggleFailMode();}}>
                          {changeFailureText()}
                        </Button>
                      </div>
                      <div class="row">
                        <div class="col-md-12">
                          <CardBody></CardBody>
                        </div>
                      </div>
                    </Card>
                  </div>
                </div>

                <div class="row" id="searchButtonRow">
                  <div class="col-md-12" id="newSearchCardCol">
                    <Card id="searchCard">
                      <div class="row">
                        <div class="col-md-12">
                          <CardTitle class="cardTitle" tag="h5">
                            Search
                          </CardTitle>
                          <p>Last Query: {processingStatus}</p>
                        </div>
                      </div>

                      <div class="row">
                        <div class="col-md-12">
                          {resultsFound && (
                            <p>
                              Showing Transfers from: <b> {savedStartDate} </b>{" "}
                              to <b> {savedEndDate} </b>
                            </p>
                          )}
                        </div>
                      </div>

                      <div class="row">
                        <div class="col-md-12">
                          <CardBody>
                            <div class="row">
                              <div class="col-md-3 centAlignCol">
                                <Button
                                  size="normal"
                                  color="primary"
                                  onClick={toggle}
                                  style={{ marginBottom: "1rem" }}
                                >
                                  New Search
                                </Button>
                              </div>
                              <div class="col-md-9 centAlignCol">




                              </div>
                            </div>

                            <div class="row">
                              <div class="col-md-12">
                                <Collapse isOpen={isOpen}>
                                  <div class="row">
                                    <div class="col-md-12">
                                      <CardText>
                                        {" "}
                                        <b>Select a date (or range) below. </b>
                                      </CardText>
                                    </div>
                                  </div>

                                  <div class="row">
                                    <div class="col-md-12">
                                      <DayPicker
                                        selectedDays={[
                                          dateRange.from,
                                          dateRange,
                                        ]}
                                        onDayClick={handleDateClick}
                                      />
                                    </div>
                                  </div>

                                  <div class="row" id="calendarButtonRow">
                                    <div class="col-md-12">
                                      <div class="row">
                                        <div class="col-md-4">
                                          <Button
                                            color="primary"
                                            disabled={!dateRange.from}
                                            onClick={
                                              processTransferAndCollapse
                                            }
                                          >
                                            Get Transfers
                                          </Button>
                                        </div>

                                        <div class="col-md-8">
                                          <Button
                                            color="primary"
                                            disabled={!dateRange.from}
                                            onClick={resetCalendarDateClick}
                                          >
                                            Reset Selected Dates
                                          </Button>
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                </Collapse>
                              </div>
                            </div>
                          </CardBody>
                        </div>
                      </div>
                    </Card>
                  </div>
                </div>
                <Card id="detailCard">
                  <div class="row">
                    <div class="col-md-12">
                      <CardTitle class="cardTitle" tag="h5">
                        Site Detail
                      </CardTitle>
                    </div>
                  </div>

                  <div class="row">
                    <div class="col-md-12">
                      <CardBody>
                        <div class="row">
                          <div class="col-md-12">
                            <p>
                              Site:{" "}
                              {selectedSiteIndex !== undefined &&
                                individualSiteData[selectedSiteIndex].name}{" "}
                            </p>
                          </div>
                        </div>

                        <div class="row" id="siteGraphRow">
                          <div class="col-md-12">
                            <Bar
                              data={
                                selectedSiteIndex !== undefined &&
                                populateSiteGraph(
                                  selectedSiteIndex,
                                  individualSiteData
                                )
                              }
                              options={srGraphOptions}
                            />
                          </div>
                        </div>
                      </CardBody>
                    </div>
                  </div>
                </Card>
              </div>


            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
