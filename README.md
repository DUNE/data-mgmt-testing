# data-mgmt-testing
Data management testing Software developed by:

- Heidi Schellman (Fermilab)
- Steven Timm (Fermilab)
- Luke Penner (Oregon State University)
- Zachary Lee (Oregon State University)
- Lydia Brynmoor (Oregon State University)
- Sean Gilligan (Oregon State University)
- Lisa Paton (University of Alaska Anchorage)
- Joseph Yeung (University of Chicago)

This repository contains code that monitors and visualizes data transfer events from SAM and Rucio. NetworkVisualizer1.0 contains the code connected to the "DUNE Network Monitor" webpage. ESClient contains the primary backend scripts responsible for querying Elasticsearch, as well as the associated cron scripts for daily caching.

### Rucio-focused es_client information
ESClient/README.md

ESClient/MIGRATION_GUIDE_RUCIO.md

ESClient/rucio_es_client.py

### SAM-focused xroot_es_client information
ESClient/README.md

ESClient/MIGRATION_GUIDE_SAM.md

ESClient/xroot_es_client.py
