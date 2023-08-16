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
import { HelpModal } from "./components/HelpModal";
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
import { QuestionMarkOutlined } from "@mui/icons-material";
import { Button as MuiButton, Tooltip as MuiTooltip } from "@mui/material";
// import { HelpModal } from "./components/HelpModal";
// import siteData from "./data/duneSiteList.json"

// var fs = require('fs');
var resultsFound = false;
var siteUnclicked = true;

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

// function checkIfResultsFound() {
//   // console.log("results found flag says: " + resultsFound)

//   if (resultsFound == undefined) {
//     return;
//   }

//   if (resultsFound) {
//     return "Results Found";
//   } else {
//     return "No Results";
//   }

// }

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
    name: "DUNE_FR_CCIN2P3_DISK",
    coordinates: [4.87, 45.78],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "DUNE_FR_CCIN2P3_TAPE",
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
    otherName: "FNAL-FERMIGRID", //"US_FNAL",
    name: "GPGRID",
    coordinates: [-88.255, 41.841],
  },
  // {
  //   markerOffset: 1,
  //   otherName: "FNAL-FERMIGRID", //"US_FNAL",
  //   name: "GPGrid",
  //   coordinates: [-88.255, 41.841],
  // },
  // {
  //   markerOffset: 1,
  //   otherName: "FNAL-FermiGrid", //"US_FNAL",
  //   name: "GPGRID",
  //   coordinates: [-88.255, 41.841],
  // },
  // {
  //   markerOffset: 1,
  //   otherName: "FNAL-FermiGrid", //"US_FNAL",
  //   name: "GPGrid",
  //   coordinates: [-88.255, 41.841],
  // },
  // {
  //   markerOffset: 1,
  //   otherName: "GPGRID", //"US_FNAL",
  //   name: "FNAL-FERMIGRID",
  //   coordinates: [-88.255, 41.841],
  // },
  // {
  //   markerOffset: 1,
  //   otherName: "GPGrid", //"US_FNAL",
  //   name: "FNAL-FERMIGRID",
  //   coordinates: [-88.255, 41.841],
  // },
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
    otherName: "DUNE_IN_TIFR",
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
  {
    markerOffset: 1,
    otherName: "SWAN",
    name: "SWAN-CE1",
    coordinates: [-96.016817, 41.247321],
  },
  {
    markerOffset: 1,
    otherName: "SWAN-CE1",
    name: "SWAN",
    coordinates: [-96.016817, 41.247321],
  },
  {
    markerOffset: 1,
    otherName: "Swan",
    name: "SWAN-CE1",
    coordinates: [-96.016817, 41.247321],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "DUNE_US_FNAL_DISK_STAGE",
    coordinates: [-88.255, 41.841],
  },
  {
    markerOffset: 1,
    otherName: "pic",
    name: "DUNE_ES_PIC",
    coordinates: [2.11, 41.5],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "DUNE_CERN_EOS",
    coordinates: [6.04, 46.23],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "SURFSARA",
    coordinates: [4.953, 52.356],
  },
  {
    markerOffset: 1,
    otherName: "",
    name: "No data for these dates",
    coordinates: [0, 0],
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

const baseUrlBackend = "http://fermicloud855.fnal.gov:3000";
const geoUrl = "./world-110m.json";

function App() {
  const [transfers, settransfers] = useState([]);
  const [individualSiteData, setIndividualSiteData] = useState([]);

  const [dateRange, setDateRange] = useState({
    from: undefined,
    to: undefined,
  });
  const [savedStartDate, setSavedStartDate] = useState();
  const [savedEndDate, setSavedEndDate] = useState();

  // Current transfer types
  const SAM = "SAM" //
  const RUCIO = "RUCIO" //
  const RUCIO_AGGREGATE = "RUCIO_AGGREGATE" //
  const RUCIO_FAILED = "RUCIO_FAILED" //
  const RUCIO_AGGREGATE_FAILED = "RUCIO_AGGREGATE_FAILED"
  const TEST_MODE = "TEST_MODE" //
  const TEST_MODE_FAILED = "TEST_MODE_FAILED"

  const [transferType, setTransferType] = useState(SAM) //

  const [mode, setMode] = useState("Select Transfer Mode") //
  const resetCalendarDateClick = () => {
    setDateRange({ from: undefined, to: undefined });
  };

  const [openHelp, setOpenHelp] = useState(false)

  // testing
  const [textTransfer, setTextTransfer] = useState("Transfers")
  const [logText, setLogText] = useState() //

  const [checkIfResultsFound, setCheckIfResultsFound] = useState("No Results")

  const [entryOne, setEntryOne] = useState("Speed (MB/s)")
  const [entryTwo, setEntryTwo] = useState("Filesize (MB)")

  const handleGetTransfersClick = () => {
    if (mode === "Select Transfer Mode") {
      setMode("SAM Transfers"); // change if default changes to something other than SAM
      setTextTransfer("SAM Transfers"); // change if default changes to something other than SAM
    } else {
      setTextTransfer(mode)
    };
    proccessTransferAndCollapse();
    
    if (mode.includes("Failed")) {
      setEntryOne("Reason")
      setEntryTwo("Count")
    } else {
      setEntryOne("Speed (MB/s)")
      setEntryTwo("Filesize (MB)")
    }
  };

  const handleDateClick = (day) => {
    if (dateRange.to) {
      resetCalendarDateClick();
    } else {
      const range = DateUtils.addDayToRange(day, dateRange);
      setDateRange(range);
    }
  };

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

  const parseSiteList = () => {
    console.log(
      "fetching DUNE site date from backend fermicloud855.fnal.gov:3001/getsites"
    );
    fetch("http://fermicloud855.fnal.gov:3001"+"/getsites")
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
            parseFloat(item.$.longitude) == 0 &&
            parseFloat(item.$.latitude) == 0
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
              element.name == item.name || element.name == item.otherName
          );

          if (matchId > -1) {
            // console.log("replacing: ")
            // console.log(mappedSites[matchId])
            // console.log("with ")
            // console.log(item)
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
      });
  };

  const parseTransfers = (passedSites) => {
    resultsFound = false;

    if (dateRange.to === undefined) {
      dateRange.to = dateRange.from;
    }

    setSavedStartDate(dateFormatConverter(dateRange.from));
    setSavedEndDate(dateFormatConverter(dateRange.to));

    setLogText(`${dateFormatConverter(dateRange.from)} - ${dateFormatConverter(dateRange.to)}`);

    var dateParameters = new URLSearchParams({
      startDate: dateFormatConverter(dateRange.from),
      endDate: dateFormatConverter(dateRange.to),
      transferType: transferType
    });

    console.log(
      "fetching transfer data from: fermicloud855.fnal.gov:3001/test?" +
        dateParameters.toString()
    );

    fetch("http://fermicloud855.fnal.gov:3001" + "/test?" + dateParameters.toString())
      //TODO: set a timeout on the promise above so that if there is just NO out.json file it won't hang

      .then((res) => res.json())
      .then((res) => {
        let allTransferedAmount = 0;

        console.log("result: ");
        console.log(res.data);

        if (res.data[0]["source"] !== "No data for these dates") {
          setCheckIfResultsFound('Results Found')
        } else {
          setCheckIfResultsFound('No Results')
        }

        if (
          res.data[0].hasOwnProperty("name") &&
          res.data[0].source !== "ERROR"
        ) {
          //TODO: modify this so that if the search fails we don't crash, maybe try/accept or if statement


          var sourceLocationAlt = "None"; 
          var destinationLocationAlt = "None";
          var mysteryCoordinates = [42,42];

          const mappedTransfers = res.data.map((entry) => {
            const sourceLocation = passedSites.find(
              (location) => entry.source === location.name | entry.source === location.otherName
            );

            const destinationLocation = passedSites.find(
              (location) => entry.destination === location.name | entry.destination === location.otherName
            );

            const speedInMB = parseFloat(entry["transfer_speed(MB/s)"]).toFixed(
              2
            );

            allTransferedAmount += entry.file_size;

            // trying to account for error here

            const failCount = entry["count"];

            const failReason = entry["reason"];

            // console.log(entry.file_size)

            if (!sourceLocation && !destinationLocation) {
              return {
                from: sourceLocationAlt,
                to: destinationLocationAlt,
                fromCoord: mysteryCoordinates,
                toCoord: mysteryCoordinates,
                speedInMB: speedInMB,
                sentToDestSizeMB: entry.file_size / 1000000,
                count: failCount, //
                reason: failReason, //
              };
            } else if (!sourceLocation) {
              return {
                from: sourceLocationAlt,
                to: destinationLocation.name,
                fromCoord: mysteryCoordinates,
                toCoord: destinationLocation.coordinates,
                speedInMB: speedInMB,
                sentToDestSizeMB: entry.file_size / 1000000,
                count: failCount, //
                reason: failReason, //
              };
            } else if (!destinationLocation) {
              return {
                from: sourceLocation.name,
                to: destinationLocationAlt,
                fromCoord: sourceLocation.coordinates,
                toCoord: mysteryCoordinates,
                speedInMB: speedInMB,
                sentToDestSizeMB: entry.file_size / 1000000,
                count: failCount, //
                reason: failReason, //
              };
            } else {
              return {
                from: sourceLocation.name,
                to: destinationLocation.name,
                fromCoord: sourceLocation.coordinates,
                toCoord: destinationLocation.coordinates,
                speedInMB: speedInMB,
                sentToDestSizeMB: entry.file_size / 1000000,
                count: failCount, //
                reason: failReason, //
              };
            }
          });

          console.log("mapped transfers: ");
          console.log(mappedTransfers);

          allTransferedAmount /= 1000000; //adjusting to mb

          settransfers(mappedTransfers);

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
                entry.totalSent += item.file_size / 1000000; //dividing the total bytes into megabytes 1024 b to kb, 1024 kb to mb
              });

            res.data
              .filter((jsonThing) => {
                return jsonThing.destination === entry.name;
              })
              .forEach((item, i) => {
                entry.totalReceived += item.file_size / 1000000; //dividing the total bytes into megabytes 1024 b to kb, 1024 kb to mb
              });

            entry.fractionOfDataSent = entry.totalSent / allTransferedAmount;
            entry.fractionOfDataReceived =
              entry.totalReceived / allTransferedAmount;

            entry.totalSent = parseFloat(entry.totalSent).toFixed(2);
            entry.totalReceived = parseFloat(entry.totalReceived).toFixed(2);
            entry.fractionOfDataSent = parseFloat(
              entry.fractionOfDataSent
            ).toFixed(4);
          });

          resultsFound = true;
          // console.log("Results found:")
          // console.log(collectionOfSiteObjects);

          setIndividualSiteData(collectionOfSiteObjects);
        } else {
          resultsFound = false;
          console.log("No results returned for DUNE transfers");
          console.log(resultsFound);
        }

        resetCalendarDateClick();
      });
  };

  const proccessTransferAndCollapse = () => {
    parseSiteList();
    toggle();
  };

  const collapseLegend = () => {
    toggleLegendCard();

    const buttonElement = document.getElementById("collapseLegendButton");
    if (buttonElement) {
      buttonElement.textContent = legendOpen ? "Collapse Legend" : "Expand Legend";
    }
  };

  const [tooltip, setTooltip] = useState("");
  const [mapPosition, setMapPosition] = useState({
    coordinates: [0, 0],
    zoom: 1,
  });
  const [isOpen, setIsOpen] = useState(false);
  const [searchResultStatus, setSearchResultStatus] = useState(); //TODO actually get this working so empty text returned unless search complete, then return results found or not
  const [selectedSiteIndex, setSelectedSiteIndex] = useState();
  const [dropdownOpen, setDropDownOpen] = useState(false);
  const [showFailureMode, setshowFailureMode] = useState(false);
  const [legendOpen, setLegendOpen] = useState(false);

  const toggle = () => setIsOpen(!isOpen);
  const toggleLegendCard = () => setLegendOpen(!legendOpen);
  const toggleDropDown = () => setDropDownOpen(!dropdownOpen);
  const toggleFailMode = () => setshowFailureMode(!showFailureMode);

  const renderMap = () => {
    console.log("checking, failure mode is: " + showFailureMode);
    if (!showFailureMode) {
      renderTransferMap();
    } else {
      renderFailMap();
    }
  };

  const renderFailMap = () => {
    return <p>fail mode map will go here </p>;
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
                            >
                              <Geographies geography={geoUrl}>
                                {({ geographies }) =>
                                  geographies.map((geo) => (
                                    <Geography
                                      key={geo.rsmKey}
                                      geography={geo}
                                      fill="#9998A3"
                                      stroke="#EAEAEC"
                                    />
                                  ))
                                }
                              </Geographies>
                              {transfers.map((oneOfThem, i) => {
                                return (
                                  <Line
                                    key={i}
                                    to={oneOfThem.toCoord}
                                    from={oneOfThem.fromCoord}
                                    stroke="#F53"
                                    strokeWidth={1}
                                    onMouseEnter={() => {
                                      // setTooltip(`Last AVG speed: ${oneOfThem.speedInMB} MB/s`);       //need to consider what, if any, we want to put in tooltip over transfer line
                                    }}
                                    onMouseLeave={() => {
                                      setTooltip("");
                                    }}
                                  />
                                );
                              })}
                              //could add another line here ^ to show ration of send
                              vs recieve between individual sites but it's one
                              within another not side by side so doesn't look great.
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
                                    key={i}
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
                                    key={i}
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
                            {legendOpen ? "Collapse Legend" : "Expand Legend"}
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
                      <div class="col-md-9">
                        <CardTitle class="cardTitle" tag="h4">
                          Transfer Map{" "}
                        </CardTitle>
                      </div>
                      {/* <div class="col-md-3" id="mapModeSwitchCol">
                        <Button
                          color="primary"
                          onClick={console.log(
                            "in future this will switch map view"
                          )}
                        >
                          Toggle Failure View
                        </Button>
                      </div> */}
                    </div>

                    <CardSubtitle tag="h6" className="mb-2 text-muted">
                      {" "}
                    </CardSubtitle>
                    <CardText>

                    </CardText>

                    <div class="row">
                      <div class="col-md-12">
                        {renderTransferMap()}
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
                      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                        <div style={{ textAlign: 'left' }}>
                          <CardTitle className="cardTitle" tag="h5">
                            Log of {textTransfer}
                          </CardTitle>
                        </div>
                        <div style={{ textAlign: 'right' }}>
                          <CardTitle className="cardTitle" tag="h5">
                            {logText}
                          </CardTitle>
                        </div>
                      </div>
                    </div>
                  </div>
                      {/* <CardTitle class="cardTitle" tag="h5">
                        Log of {textTransfer} {logText}
                      </CardTitle>
                    </div>
                  </div> */}

                  <div class="row">
                    <div class="col-md-12">
                      <CardBody>
                        <Table>
                          <thead>
                            <tr>
                              <th>To</th>
                              <th>From</th>
                              <th>{entryOne}</th>
                              <th>{entryTwo}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {transfers.map((transfer, i) => {
                              let logContent;

                              if (transfer.speedInMB !== "NaN") {
                                logContent = (
                                  <tr key={i}>
                                  <td>{transfer.to}</td>
                                  <td>{transfer.from}</td>
                                  <td>{transfer.speedInMB}</td>
                                  <td>{transfer.sentToDestSizeMB.toFixed(0)}</td>
                                </tr>
                                );
                              } else {
                                logContent = (
                                  <tr key={i}>
                                  <td>{transfer.to}</td>
                                  <td>{transfer.from}</td>
                                  <td>{transfer.reason}</td>
                                  <td>{transfer.count}</td>
                                </tr>
                                );
                              }

                              return logContent;
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
            <div class="col-md-3" id="fixedRightCol"> {/*col-md-3 position-fixed*/}
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

                <div class="row" id="searchButtonRow">
                  <div class="col-md-12" id="newSearchCardCol">
                    <Card id="searchCard">
                      <div class="row">
                        <div class="col-md-12">
                          <div style={{display: "flex", alignItems: "center", flexDirection:"row", justifyContent:"space-between"}}>
                          <CardTitle class="cardTitle" style={{margin: 0}}tag="h5">
                            Search
                            
                          </CardTitle>
                          <MuiTooltip title="Help" placement="top">
                          <MuiButton sx={{minWidth:0}}style={{color: "#F2682B47",  margin: 0, padding: 0}} onClick={() => setOpenHelp(true)}><QuestionMarkOutlined fontSize={"20px"} style={{margin: 0, padding:2, height: "24px", width: "24px", backgroundColor: "#777777", color: "#cdcdcd", border: "1px solid #777777", borderRadius: 100}}/></MuiButton>
                          </MuiTooltip>
                          
                          </div>
                          
                          
                          <p style={{margin: "8px 0px"}}>Last Query: {checkIfResultsFound}</p>
                        </div>
                      </div>

                      <div class="row">
                        <div class="col-md-12">
                          {resultsFound && (
                            <p>
                              Showing <b> {textTransfer} </b> from: <b> {savedStartDate} </b>{" "}
                              to <b> {savedEndDate} </b>
                            </p>
                          )}
                        </div>
                      </div>

                      <div class="row">
                        <div class="col-md-12">
                          <CardBody>
                            <div style={{display: "flex", flexDirection:"column", alignItems: "center", margin: "0px 16px"}}>
                              {/* <div class="col-md-3 centAlignCol"> */}
                                <Button
                                  size="normal"
                                  color="primary"
                                  onClick={toggle}
                                  style={{ marginBottom: "8px" , width: "100%"}}
                                >
                                  New Search
                                </Button>
                              {/* </div> */}
                              {/* <div class="col-md-9 centAlignCol"> */}
                               <Dropdown
                                  isOpen={dropdownOpen}
                                  toggle={toggleDropDown}
                                  style={{ marginBottom: "8px", width: "100%" }}
                                  onClick={console.log(
                                    "will set mode in future"
                                  )}
                                >
                                  <DropdownToggle style={{width: "100%"}}caret>
                                    {mode}
                                  </DropdownToggle>
                                  <DropdownMenu>
                                    <DropdownItem header>
                                      Successful Transfers{" "}
                                    </DropdownItem>
                                    <DropdownItem onClick={() => {setMode("SAM Transfers"); setTransferType(SAM)}}value="3">
                                      SAM Transfers
                                    </DropdownItem>
                                    <DropdownItem value="5" onClick={() => {setMode("Rucio Transfers"); setTransferType(RUCIO)}}>
                                      Rucio Transfers
                                    </DropdownItem>
                                    <DropdownItem value="6" onClick={() => {setMode("Rucio Aggregate Transfers"); setTransferType(RUCIO_AGGREGATE)}}>
                                      Rucio Aggregate Transfers
                                    </DropdownItem>
                                    <DropdownItem divider />
                                    <DropdownItem header>
                                      Failed Transfers{" "}
                                    </DropdownItem>
                                    <DropdownItem value="7" onClick={() => {setMode("Rucio Failed Transfers"); setTransferType(RUCIO_FAILED)}}>
                                      Rucio Failed Transfers
                                    </DropdownItem>
                                    <DropdownItem value="7" onClick={() => {setMode("Rucio Aggregate Failed Transfers"); setTransferType(RUCIO_AGGREGATE_FAILED)}}>
                                      Rucio Aggregate Failed Transfers
                                    </DropdownItem>
                                    <DropdownItem divider />
                                    <DropdownItem header>
                                      Diagnostic Mode Tests{" "}
                                    </DropdownItem>
                                    <DropdownItem onClick={() => {setMode("Test Mode Successful"); setTransferType(TEST_MODE)}}  value="2">
                                      Test Mode Successful
                                    </DropdownItem>
                                    <DropdownItem onClick={() => {setMode("Test Mode Failed"); setTransferType(TEST_MODE_FAILED)}}  value="4">
                                      Test Mode Failed
                                    </DropdownItem>
                                  </DropdownMenu>
                                </Dropdown>
                              {/* </div> */}
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
                                      <div style={{display: "flex", flexDirection: "column", alignItems: "center", margin:"0px 16px"}}>
                                        {/* <div  style={{width: "100%", margin: "0px 16px"}}> */}
                                          <Button
                                            color="primary"
                                            disabled={!dateRange.from}
                                            style={{
                                              width: "100%",
                                              margin: "0px 16px",
                                              marginBottom: "8px"
                                            }}
                                            onClick={
                                              () => {handleGetTransfersClick()}
                                            }
                                          >
                                            Get Transfers
                                          </Button>
                                        {/* </div> */}

                                        {/* <div class="col-md-8"> */}
                                          <Button
                                            color="primary"
                                            disabled={!dateRange.from}
                                            style={{
                                              width: "100%",
                                              margin: "0px 16px",
                                              marginBottom: "8px"
                                            }}
                                            onClick={resetCalendarDateClick}
                                          >
                                            Reset Selected Dates
                                          </Button>
                                        {/* </div> */}
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
                              {selectedSiteIndex != undefined &&
                                individualSiteData[selectedSiteIndex].name}{" "}
                            </p>
                          </div>
                        </div>

                        <div class="row" id="siteGraphRow">
                          <div class="col-md-12">
                            <Bar
                              data={
                                selectedSiteIndex != undefined &&
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
          <HelpModal open={openHelp} onClose={() => setOpenHelp(false)}/>
    </div>

  );
}

export default App;