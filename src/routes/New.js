import React, { useEffect, useState } from "react";
import { useDispatch } from "react-redux";
import Map from "../components/Map";
import { addDataToMap, removeDataset } from 'kepler.gl/actions';
import data from '../siteOutputGeoJson.json';
import { processGeojson, processRowObject } from "kepler.gl/dist/processors";
import siteData from "../sitesGeoJson.json"
import transferData from "../transfersGeoJson.json"
import { Button } from 'reactstrap';

const axios = require('axios').default;
axios.defaults.baseURL = "localhost"
// import combinedData from "../modifiedgeoJson.json"


function getSites() {
    return axios.get("http://localhost:3001/sites");
}
function getTransfers() {
    return axios.get('http://localhost:3001/records', { //need to address bug where it crashes if fed reverse time, IE first later than second
        params: {
            startDate: '01-01-2020',
            endDate: "03-23-2022"
        }
      })
}

function getSearchResults() {
    alert("still need to hook this up to es_caching...")
}


function New() {

    const dispatch = useDispatch();
    let objectCache = false;

    const mapConfig = {
        visState: {

        }
    };

    const clickGetSites = () => {
        getSites().then((res) => {

            //add display name for showing on the map
            for (let i=0; i < res.data.sites.length; i++) {
                res.data.sites[i].displayName= res.data.sites[i].names[0]
            }

            console.log(res.data)
            objectCache = res.data

            dispatch(
                removeDataset('sites')
            );

            dispatch(
                addDataToMap({
                    datasets: {
                        info: {
                            label: "Dune Sites",
                            id: "sites",
                        },
                        
                        data: processRowObject( res.data.sites ),
                        // data: processGeojson ( res.data.geoJsonSites )           //shows up as polygon, we want either point or cluster format
                    },
                    options : {
                        Radius: 20
                    }
                })
            );
        });
    };





    const clickGetTransfers = () => {
        getTransfers().then((res) => {

            console.log("records/transfers:   ",res.data)
            let transfers = []
            let sitesObject = {}

            if (objectCache) {
                console.log("using cached version")
            } else {
                sitesObject = getSites()
                console.log(" downloading sites for use in transfer processing function ")
            }


            //erase old sites, since they might not have the stats and transfers attached yet
            dispatch(
                removeDataset('sites')
            );


            //re draw the sites on the map with their statistics filled in
            dispatch(
                addDataToMap({
                    datasets: {
                        info: {
                            label: "sites",
                            id: "sites",
                        },
                        
                        data: processRowObject(res.data.siteOutputWithStats),
                    }
                })
            );


            console.log("transfer geo JSON:   ", res.data.transferGeoJSON.geoJson)


            dispatch(
                addDataToMap({
                    datasets: {
                        info: {
                            label: "transfers",
                            id: "transfers",
                        },
                        
                        data: processGeojson(res.data.transferGeoJSON.geoJson),
                    }
                })
            );




        });






    };







    useEffect(() => {
        clickGetSites();
    }, []);

    return <>


    {/* <Button variant="contained">Get Sites Only</Button>
    <Button variant="contained">Get Transfers</Button> */}
    <Map></Map>
    <Button onClick={clickGetSites}>Get Just Sites</Button>
    <Button onClick={clickGetTransfers}>Get Transfers - Cached</Button>
    <Button onClick={getSearchResults}>Get Transfers - New Query</Button>

    </>;
}

export default New;