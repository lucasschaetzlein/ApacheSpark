# Apache Spark
...

## SparkStreaming

Die Anwendung [Twitter.scala](https://github.com/lucasschaetzlein/ApacheSpark/blob/master/SparkStreaming/src/Twitter.scala) stellt einige Funktionen zur Verarbeitung von abgesetzten Tweets bereit. 

1. Anzahl der abgesetzten Tweets
2. Durchschnittliche Zeichenlänge der abgesetzten Tweets 
3. Die beliebtesten Tweets

Es besteht die Möglichkeit die Ergebnisse in einer Datenbank zu speichern. Die Datenbank läuft in einem Docker Container und muss vor dem Start der Anwendung gestartet werden.

### Vorbereitungen zur Verwendung des Dockers:
- [Docker](https://www.docker.com/) herunterladen und installieren
- mySql Docker Repository laden: `docker pull mysql`

### Starten des Dockers / der Datenbank:
`docker-compose -f docker-compose.yml up` <br>
ggf: `docker network create local`


## GraphX
...

