# michigan-state-legislative-districts

Gubernatorial election results by Michigan state house/senate districts, using 2022 districts

## Data Sources

### Shapefiles

Via [Michigan GIS Open Data](https://gis-michigan.opendata.arcgis.com/search?tags=boundaries):

* [2014 Voting Precincts](https://gis-michigan.opendata.arcgis.com/maps/2014-voting-precincts)
* [2018 Voting Precincts](https://gis-michigan.opendata.arcgis.com/maps/2018-voting-precincts)
* [Minor Civil Divisions (Cities & Townships)](https://gis-michigan.opendata.arcgis.com/maps/minor-civil-divisions-cities-townships-)

Via [Michigan Independent Citizens Redistricting Commission Mapping Data](https://www.michigan.gov/micrc/mapping-process/mapping-data):

* [My Districting Michigan](https://michigan.mydistricting.com/legdistricting/michigan/comment_links) - click on "State House" or "State Senate" on the horizontal navigation bar

### Election Results

* Visit [Michigan Secretary of State election results and data](https://www.michigan.gov/sos/elections/Election-Results-and-Data)
* Scroll down to "November General Election Results" & open this section
* Click on "November General Election Results by Precinct"
* [This link](http://miboecfr.nictusa.com/cgi-bin/cfr/precinct_srch.cgi) will open. Follow the instructions to download election results. (Please note: `County Code` and `City/Town Code` in these files are NOT the same as `countyfips` and `mcdfips` in the shapefiles!)
