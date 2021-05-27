import React, { useState } from "react";
import ReactTooltip from "react-tooltip";
import ReactDOM from "react-dom";
import Tooltip from "react-simple-tooltip";
import Picker from 'react-month-picker'
import {
  ComposableMap,
  Geographies,
  Geography,
  Graticule,
  ZoomableGroup,
  Line,
  Marker,
} from "react-simple-maps";
import "./App.css";

const markers = [
  { markerOffset: 1, name: "MANCHESTER", coordinates: [2.2, 53] },
  { markerOffset: 1, name: "RAL_ECHO", coordinates: [1.2, 51.6] },
  { markerOffset: 1, name: "CERN_PDUNE_CASTOR", coordinates: [6, 46] },
  { markerOffset: 1, name: "IMPERIAL", coordinates: [0.17, 51.4] },
  { markerOffset: 1, name: "FNAL_DCACHE", coordinates: [-88.27, 41.84] },
];

const geoUrl =
  "./world-110m.json";

function App() {
  const [transfers, settransfers] = useState([]);
  const [individualSiteData, setIndividualSiteData] = useState([]);

  const action = () => {







    fetch("http://localhost:3001/test")
      .then((res) => res.json())
      .then((res) => {
        let allTransferedAmount = 0;

        const mappedTransfers = res.data.map((entry) => {
          const sourceLocation = markers.find(
            (location) => entry.source === location.name
          );
          const destinationLocation = markers.find(
            (location) => entry.destination === location.name
          );
          const speedInMB = parseFloat(entry["transfer_speed(MB/s)"]).toFixed(
            2
          );
          allTransferedAmount += entry.file_size;

          return {
            from: sourceLocation.name,
            to: destinationLocation.name,
            fromCoord: sourceLocation.coordinates,
            toCoord: destinationLocation.coordinates,
            speedInMB: speedInMB,
          };
        });

        allTransferedAmount /= 1048576; //adjusting to mb

        settransfers(mappedTransfers);

        const collectionOfSiteObjects = markers.map((marker) => {
          return {
            ...marker,
            totalSent: 0,
            totalReceived: 0,
          };
        });

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

          entry.totalSent = parseFloat(entry.totalSent).toFixed(2);
          entry.totalReceived = parseFloat(entry.totalReceived).toFixed(2);
          entry.fractionOfDataSent = parseFloat(
            entry.fractionOfDataSent
          ).toFixed(4);
        });

        console.log(collectionOfSiteObjects);

        setIndividualSiteData(collectionOfSiteObjects);
      });




  };

  const [tooltip, setTooltip] = useState("");
  const [position, setPosition] = useState({ coordinates: [0, 0], zoom:   1 });


  return (
    <div className="App">
      <header className="App-header">
        <h1>DUNE Network Activity Monitor - Alpha</h1>
      </header>

      <div class="basicRow">
        <div class="basicColumn">
          <div id={"map"}>
            <ComposableMap data-tip="">
              <ZoomableGroup zoom={3} center={[-34, 34]}>

                <Geographies geography={geoUrl}>
                  {({ geographies }) =>
                    geographies.map((geo) => (
                      <Geography key={geo.rsmKey} geography={geo} fill="#9998A3" stroke="#EAEAEC" />
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
                //could add another line here ^ to show ration of send vs
                recieve between individual sites but it's one within another not
                side by side so doesn't look great.
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
                    <Marker key={i} coordinates={coordinates}
                    onClick={() => {
                      //alert("click action here");
                      //alert("radius click")
                    }}
                    >
                      <circle r={1} fill="rgba(255,255,255,1)" />
                      <circle
                        r={20 * fractionOfDataSent}
                        fill="rgba(87,235,51,0.4)"
                      />{" "}
                      //send fraction circle
                      <circle
                        r={20 * fractionOfDataReceived}
                        fill="rgba(12,123,220,0.35)"
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
                        //alert("center click");
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
                      <circle r={1} fill="rgba(75,0,146,1)" />
                    </Marker>
                  )
                )}
              </ZoomableGroup>
            </ComposableMap>
          </div>
          <ReactTooltip html={true}>{tooltip}</ReactTooltip>

        </div>



        <div class="basicColumn">

          <button onClick={action}>Update DUNE Transfer Data</button>


          <p>JSON Response Data - December, 2020:</p>

          {JSON.stringify(transfers)}
        </div>
      </div>
    </div>
  );
}

export default App;
