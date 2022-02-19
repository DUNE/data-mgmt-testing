import KeplerGl from 'kepler.gl';

const Map = props => (
  <>
    <KeplerGl
        id="foo"
        mapboxApiAccessToken="pk.eyJ1IjoiaWhlYXJ0cHVnczg5IiwiYSI6ImNrdzNidzBvN2FrbjUzMHFwdjJra3RuNHcifQ.zwbTlZiRGrZKU-fcW8idNQ"
        height={1200}
        width={2500}
        type="timeRange"
        plotType="histogram"
        />


  </>
);

export default Map;