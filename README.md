# we-count
data and documentation for the "we count" project with ADFC
We-count ist ein Projekt des DLRs in Zusammenarbeit mit den Allgemeinen Deutschen Fahrrad-Club e.V. (ADFC).
Ziel ist es ein Simulationsmodell in SUMO vom berliner Berzirk Treptow-Köpenick zu modellieren mithilfe von Zähldaten, welche mittels Telraam-Kameras aufgenommen werden/wurden.

Was ist Telraam?
Das Telraam-Gerät ist eine Kombination aus einem Raspberry Pi-Mikrocomputer, Sensoren und einer Kamera mit niedriger Auflösung.
Das Gerät wird an der Innenseite eines Obergeschossfensters mit Blick auf die Straße montiert.
Um die Verkehrszählungsdaten direkt an die zentrale Datenbank zu senden, benötigt das Gerät eine kontinuierliche WLAN-Verbindung zum Internet.
Da das Gerät elektrisch betrieben wird, benötigt es auch eine Steckdose in Reichweite.
Hierbei werden keine Bilder gespeichert, sondern lediglich die Zählwerte übertragen. Somit gibt es seitens des Datenschutzes "keine" Probleme. 

Standortermittlung

Zur Ermittlung geeignert Standorte für die Kamreas wurden zunächst verfügbare Daten gesammelt und analysiert. 
Hierbei stellt das Land Berlin folgende Daten zur Verfügung:

Digitalte Plattform Stadtverkehr (https://viz.berlin.de/verkehr-in-berlin/) 

- Verkehrsdetektion mittels passiver Infrarot Kameras von Siemens (TEU Sensoren), liefen tagesaktuelle Zähldaten des MIVs 
- Verkehrsdetektion mittels Induktionsschleifen auf Autobahnen 
- Verkehrsmengen KFZ und LKW für den DTVw im übergeordneten berliner Straßennetz (2016-2019) 
- Standorte der TEU-Sensoren können als geojson-Datei heruntergeladen werden 

Stadt Berlin (https://fbinter.stadt-berlin.de/fb/index.jsp?loginkey=showMap&mapId=k_vmengen2019@senstadt)

-Verkehrsmengenkarte DTVw 2019 
- Übergeordnetes Straßennetz Bestand (https://fbinter.stadt-berlin.de/fb/index.jsp?loginkey=showMap&mapId=verkehr_strnetz@senstadt)
- Dauerzählstellen für den Radverkehr, tagesaktuelle Zähldaten (https://data.eco-counter.com/ParcPublic/?id=4728), Stundenwerte von 2012-2020 können als Excel-Tabelle heruntergeladen werden
- Verkehrsmodell Berlin Gesamtprognose 2025 (https://www.berlin.de/sen/uvk/verkehr/verkehrsdaten/verkehrsmodell/)

SimRa-Projekt (https://simra-project.github.io/map.html?region=berlin)

- Karte mit allen gezählten Fahrten auf den berliner Straßennetz inklusive der Auswertung wo es zu beinahe Unfällen kam 

Anhand der SimRa-Karte, den TEU- und Fahrradzählstandorten und der Unterteilung in über- und untergeordnetes Straßennetz wurden potenzielle Telraam-Standorte ermittelt. 
Hierbei lag/liegt der Fokus insbesondere auf Standorten, welche in der Nähe der verfügbaren Datenquellen liegen. Des Weiteren soll die Lage der Standorte das Hauptstraßennetz von Treptow-Köpenick abdecken. 
Mithilfe der SimRa-Karte wurden zusätzlich Strecken analysiert, welche eine hohe Anzahl an Fahrten aufwiesen/aufweisen. 
Die Standorte sollten folgende Kriterien erfüllen, um gute Zähldaten generieren zu können:
- große Fenster, Blick direkt auf die Straße
- möglichst keine Bäume und Parktflächen davor
- Erdgeschoss oder 1. Etage 
- Wlan-Verbindung 

Alle Datenquellen und potenzielle Standorte wurden/werden innerhalb einer umap-Karte gespeicher/gekennzeichnet. 
Hierbei besitzt die Karte die Standorte der TEU-Sensoren, Fahrradzählstellen, potenzielle Telraam-Standorte sowie alternativen Standorten und die momentan bereits aktiven Telraams. 
Zudem sind die Umrisslinien von Treptow-Köpenick gekennzeichnet. 
Die Karte kann als geojson-Datei heruntergeladen aber auch bearbeitet hochgeladen werden. 
Es lassen sich in der Karte die einzelnen Ebenen ein- und ausblenden. 

english
data and documentation for the "we count" project with ADFC
We-count is a DLR project in cooperation with the Allgemeine Deutsche Fahrrad-Club e.V. (ADFC). The aim is to model a simulation model in SUMO from the Berlin district of Treptow-Köpenick with the help of counting data that is / was recorded by Telraam cameras.

What is Telraam?
The Telraam device is a combination of a Raspberry Pi microcomputer, sensors and a low resolution camera. The device is mounted on the inside of an upper floor window with a view of the street. In order to send the traffic count data directly to the central database, the device requires a continuous WiFi connection to the Internet. Since the device is operated electrically, it also needs a socket within reach. No images are saved here, only the count values ​​are transmitted. So there are "no" problems with data protection.

Location determination

In order to determine suitable locations for the Kamreas, available data were first collected and analyzed. The State of Berlin provides the following data for this:

Digital urban transport platform (https://viz.berlin.de/verkehr-in-berlin/)

- Traffic detection using passive infrared cameras from Siemens (TEU sensors), daily counting data from the MIV ran
- Traffic detection using induction loops on motorways
- Traffic volumes of cars and trucks for the DTVw in the superordinate Berlin road network (2016-2019)
- The locations of the TEU sensors can be downloaded as a geojson file

City of Berlin (https://fbinter.stadt-berlin.de/fb/index.jsp?loginkey=showMap&mapId=k_vhaben2019@senstadt)

-Traffic volume map DTVw 2019
- Superordinate road network inventory (https://fbinter.stadt-berlin.de/fb/index.jsp?loginkey=showMap&mapId=verkehr_strnetz@senstadt)
- Permanent counting stations for cycling, daily counting data (https://data.eco-counter.com/ParcPublic/?id=4728), hourly values ​​from 2012-2020 can be downloaded as an Excel spreadsheet
- Berlin traffic model overall forecast 2025 (https://www.berlin.de/sen/uvk/verkehr/verkehrsdaten/verkehrsmodell/)

SimRa project (https://simra-project.github.io/map.html?region=berlin)

- Map with all counted journeys on the Berlin road network including the analysis of where almost accidents occurred

Potential Telraam locations were identified using the SimRa card, the TEU and bicycle counting locations and the subdivision into higher-level and subordinate road networks.
The focus here was / is in particular on locations that are close to the available data sources. Furthermore, the location of the locations should cover the main road network of Treptow-Köpenick.
With the help of the SimRa card, routes with a high number of journeys were also analyzed.
The locations should meet the following criteria in order to be able to generate good counting data:
- large windows, direct view of the street
- If possible, no trees and parking spaces in front of them
- Ground floor or 1st floor
- WiFi connection

All data sources and potential locations have been / are stored / marked within an umap map.
The map has the locations of the TEU sensors, bicycle counting points, potential Telraam locations as well as alternative locations and the currently active Telraams.
The outlines of Treptow-Köpenick are also marked.
The map can be downloaded as a geojson file, but it can also be uploaded edited.
The individual levels can be shown and hidden on the map.
