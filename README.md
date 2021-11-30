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
