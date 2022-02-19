import React, { useState } from "react";
import Old from "./routes/Old";
import {
    BrowserRouter,
    Routes,
    Route
  } from "react-router-dom";
import New from "./routes/New";



function App() {
  return <BrowserRouter>
    <Routes>
        <Route path = "/" element={<Old />} />
        <Route path = "/new" element={<New />} />
    </Routes>
  </BrowserRouter>;
}




export default App;