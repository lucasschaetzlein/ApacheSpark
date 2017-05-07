# Apache Spark
...

## SparkStreaming

Die Anwendung [Twitter.scala](https://github.com/lucasschaetzlein/ApacheSpark/blob/master/SparkStreaming/src/Twitter.scala) stellt einige Funktionen zur Verarbeitung von abgesetzten Tweets bereit. 

1. Anzahl der abgesetzten Tweets
2. Durchschnittliche Zeichenlänge der abgesetzten Tweets 
3. Die beliebtesten Hashtags der letzten abgesetzten Tweets

Es besteht die Möglichkeit die Ergebnisse in einer Datenbank zu speichern. Die Datenbank läuft in einem Docker Container und muss vor dem Start der Anwendung gestartet werden.

### Vorbereitungen zur Verwendung des Dockers:
- [Docker](https://www.docker.com/) herunterladen und installieren
- mySql Docker Repository laden: `docker pull mysql`

### Starten des Dockers / der Datenbank:
`docker-compose -f docker-compose.yml up` <br>
ggf: `docker network create local`


## GraphX

Die Anwendung [FlightData.scala](https://github.com/lucasschaetzlein/ApacheSpark/blob/master/GraphX/src/FlightData.scala) baut anhand von Daten über Flughäfen und Flugrouten einen Graphen auf. Nachdem der Graph generiert wurde kann dieser analysiert werden. Folgende Methoden zur Analyse werden durch die Anwendung bereitgestellt.

1. Anzahl der Flughäfen
2. Anzahl der Flüge zwischen zwei Flughäfen
3. Top 10 Flüge zwischen zwei Flughäfen
