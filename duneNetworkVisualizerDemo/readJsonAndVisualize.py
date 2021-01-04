import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import sys
import plotly.graph_objects as go

passedName = sys.argv[1]

fileName = os.sep.join(["output", passedName])


with open(fileName, "r") as fid:
    data = json.load(fid)["data"]

server_names = set()
speeds = {}

for x in data:
    server_names.add(x["source"])
    server_names.add(x["destination"])
    if (x["source"],x["destination"]) not in speeds:
        speeds[(x["source"],x["destination"])] = [round(float(x["transfer_speed(MB/s)"]), 2)]
    else:
        speeds[(x["source"], x["destination"])].append(round(float(x["transfer_speed(MB/s)"]), 2))

# print(server_names)
# print(speeds)

avg_speeds  = {}

latLongForServers = {}
latLongForServers["MANCHESTER"] = (53.48,-2.24)
latLongForServers["RAL_ECHO"] = (47.37,8.54)
latLongForServers["CERN_PDUNE_CASTOR"] = (46.14,6.02)
latLongForServers["IMPERIAL"] = (51.49,-0.17)
latLongForServers["FNAL_DCACHE"] = (41.48,-88.27)


for key, val in speeds.items():
    #source, dest = key
    avg_speeds[key] = str(np.mean(val)) + " MB/s"



# for x in avg_speeds:
#     print(x)
#     print(latLongForServers[x[0]][0] , latLongForServers[x[0]][1])
#     print(latLongForServers[x[1]][0] , latLongForServers[x[1]][1])
#     print("\n")
#








fig = go.Figure(go.Scattergeo())

df_flight_paths = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/2011_february_aa_flight_paths.csv')
df_flight_paths.head()

flight_paths = []
for i in avg_speeds:
    fig.add_trace(
        go.Scattergeo(
            lon = [latLongForServers[i[0]][1],latLongForServers[i[1]][1]],
            lat = [latLongForServers[i[0]][0],latLongForServers[i[1]][0]],
            mode = 'lines',
            name = i[0] + " to " + i[1],
            text= avg_speeds[i],
            hoverinfo="text",
            line = dict(width = 1,color = 'red'),
        )
    )


fig.update_layout(
    title_text = data[0]["start_time"] + " DUNE file transfer speed (hover for values)",
    showlegend = True,
    geo = dict(
        projection_type = 'natural earth',
        showland = True,
        landcolor = 'rgb(243, 243, 243)',
        countrycolor = 'rgb(204, 204, 204)',
    ),
)

fig.show()
