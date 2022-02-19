import ReactTooltip from "react-tooltip";
import ReactDOM from "react-dom";
import {
  ComposableMap,
  Geographies,
  Geography,
  Graticule,
  ZoomableGroup,
  Line,
  Marker,
} from "react-simple-maps";



const ErrorCircles = (props) => {
    return (
        <Marker
            coordinates={props.coordinates}
            // onClick={() => {
            // alert("click action here");
            // alert("radius click")
            // }}
        >
            <circle
            r={40 * props.txErrorFraction}
            fill="rgba(87,235,51,0.4)"
            />{" "}
            //send fraction circle

            <circle
            r={40 * props.rxErrorFraction}
            fill="rgba(12,123,220,0.4)"
            />{" "}
            //recieve fraction circle
        </Marker>

    );
};

export default ErrorCircles;