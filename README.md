# Apache Spark
...

## SparkStreaming

Die Anwendung [Twitter.scala](https://github.com/lucasschaetzlein/ApacheSpark/blob/master/SparkStreaming/src/Twitter.scala) liest die Autoren von abgesetzten Tweets ab und schreibt diese in eine Datenbank. Die Datenbank läuft in einem Docker Container und muss vor dem Start der Anwendung gestartet werden.

### Vorbereitungen zur Verwendung des Dockers:
- [Docker](https://www.docker.com/) herunterladen und installieren
- mySql Docker Repository laden: `docker pull mysql`

### Starten des Dockers / der Datenbank:
`docker-compose -f docker-compose.yml up`

docker network create local



## GraphX
...

