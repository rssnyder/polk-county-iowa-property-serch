# polk county iowa property serch

live: https://polkproperties.ttdsm.org/

the polk county assesor page is a favorite of mine

there was an [axios article](https://www.axios.com/local/des-moines/2026/03/17/polk-assessor-website-key-search-function-county) talking about how the engineer who maintains it is retiring so they expect a site blackout while they work on getting a contract to rebuild it

we extracted the information available for download in csv format, loaded the parcel records into a table and the sale records into another

you can pull the raw CSV files from the county here:
- https://www.assess.co.polk.ia.us/cgi-bin/web/tt/infoqry.cgi?tt=downloads/downloads

or my combined residential, commercial, and agricultural records with sales database:
- https://s3.rileysnyder.dev/public/polk/db/polk_county.db
 
running: `./webapp/polksearch -addr 0.0.0.0:8080 -db polk_county_3-15-2026.db -end-date "March 15, 2026"`

credit to the techtalkdsm discord server
