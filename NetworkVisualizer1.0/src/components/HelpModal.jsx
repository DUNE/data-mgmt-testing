import { Close } from "@mui/icons-material";
import { Modal, Box, Typography, IconButton} from "@mui/material"
import React from 'react'

const style = {
    position: 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
    maxWidth: 600,
    minWidth: 500,
    padding: "24px",
    bgcolor: 'background.paper',
    backgroundColor: "#EEEEEE",
    boxShadow: 24,
    fontFamily: "Poppins, san-serif",
    borderRadius: "10px"
  };
export const HelpModal = ({open, onClose}) => {
    return <Modal open={open} onClose={onClose}>
<Box sx={style}>
    <div style={{display:"flex", flexDirection:"row", justifyContent:"space-between", alignItems:"center"}}>
          <Typography  variant="h6" sx={{color: "black", fontWeight: 600,  fontFamily: "Poppins, san-serif",}}component="h2">
            Using the Search Query 
          </Typography>
          <IconButton onClick={onClose}><Close/></IconButton>
          </div>
          <Typography sx={{ mt: 2,  fontFamily: "Poppins, san-serif",}}>
           To begin a search query, select "New Search" to open the calendar, then select the start and end of the date range. 
           Use the "Select Transfer Mode" toggledown menu to select the type of transfer to visualize (default is SAM Transfers). The "Get Transfers" button
           will then fill the transfer map and log of transfers if data is available.  
           <br/> <br/>
           *Note that "Rucio Aggregate Transfers" combines and summarizes the Rucio transfers for each day for a more concise
           visualization and transfer log. It is highly recommended to use "Rucio Aggregate Transfers" over "Rucio Transfers"
           if querying more than one date to avoid web page slowdowns.
          </Typography>
        </Box>
    </Modal>
}