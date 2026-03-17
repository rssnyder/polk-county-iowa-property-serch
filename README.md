# polk county iowa property serch

live: https://polkproperties.ttdsm.org/

the polk county assesor page is a favorite of mine

there was an [axios article](https://discord.com/channels/905606844876206080/910557874403541043/1483461836056559719) talking about how the engineer who maintains it is retiring so they expect a site blackout while they work on getting a contract to rebuild it

we extracted the information avalible for download in csv format, loaded the parcle records into a table and the sale records into another

you can pull the raw CSV or sqlite files here:
- https://s3.rileysnyder.dev/public/POLKCOUNTY.csv
- https://s3.rileysnyder.dev/public/polk_county_3-15-2026.db
 
running: `./webapp/polksearch -addr 0.0.0.0:8080 -db polk_county_3-15-2026.db -end-date "March 15, 2026"`

credit to the techtalkdsm discord server
