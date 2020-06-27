# RTB fraud detection
A Flink application to detect fraudulent events in clicks and displays data streams.

## What this applications does
This Flink application consumes two data streams from Kakfa: 
- clicks stream: e.g. {"eventType":"click","uid":"d88fd895-c5bd-4508-8d0b-7ff855cf89aa11","timestamp":1592491225,"ip":"238.186.83.58","impressionId":"0377cf80-0cbd-420a-b1d9-14684ec030cf"},
- events streams: e.g. {"eventType":"display","uid":"c2c99a87-c8d5-4fbe-ae39-1fe411b9406e15","timestamp":1592491215,"ip":"238.186.83.58","impressionId":"2e6167c8-48a1-4a01-9063-6b94e5ed13e2"}

Three potentially fraudulent patterns are being detected: 
- Number of clicks by IP > 6 in a 1-hour tumbling window
- Number of displays by IP > 15 a 1-hour tumbling window
- CTR by UserID > some threshold under various conditions:
  - UID CTR > 0.50 and UID has been shown at least 2 displays a 1-hour tumbling window
  - UID CTR > 0.25 and UID has been shown at least 10 displays a 1-hour tumbling window

Events that triggers these patterns are considered suspiscious and are outputed to three different text files, respectively:
- clicks_fraud_events.txt
- displays_fraud_events.txt
- ctr_fraud_events.txt

## How to build this application

## How to run this application
