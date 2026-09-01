# Smart Factory IoT — pipeline Kafka + MongoDB

Esercitazione di data engineering: una pipeline di streaming che simula i sensori di una
fabbrica, ne trasporta le letture con **Apache Kafka** e le archivia su **MongoDB**,
riconoscendo al volo le misure fuori soglia.

Progetto a due, diviso su un contratto dati concordato in anticipo: il **Producer** (Studente A)
e il **Consumer** (Studente B).

## L'idea

Quattro ambiti di sensori (ambientale, macchinari, logistica, qualità) per 40 dispositivi
distribuiti su quattro reparti e quattro linee di produzione. Il Producer genera **1.000.000 di
letture** e le pubblica su Kafka; il Consumer le legge in tempo reale, le arricchisce e le smista
su MongoDB separando gli allarmi dal flusso normale.

```
                    ┌──────────────┐
  40 sensori  ──►   │   PRODUCER   │ ──► raw_telemetry (MongoDB)
   simulati         └──────┬───────┘
                           │  4 topic Kafka
                           │  ambientale · macchinari · logistica · qualita
                           ▼
                    ┌──────────────┐ ──► processed_events
                    │   CONSUMER   │ ──► alerts
                    └──────────────┘
```

## Il contratto dati

`contratto_dati.json` è il pezzo che ha reso possibile lavorare in parallelo. Definito prima di
scrivere il codice, fissa lo schema del messaggio, i tipi di misura per ambito con i rispettivi
range, le soglie di allarme e quale collection MongoDB è di competenza di chi.

Avendolo concordato all'inizio, ognuno ha potuto sviluppare e testare la propria metà senza
aspettare l'altro: bastava rispettare il formato.

```json
{
  "device_id": "MAC-014",
  "ambito": "macchinari",
  "timestamp": "2026-06-08T09:30:00Z",
  "tipo": "temperatura_motore",
  "valore": 78.4,
  "unita": "C",
  "reparto": "Assemblaggio",
  "linea": "L2"
}
```

## Producer — `producer.py`

Genera i messaggi e li instrada verso due destinazioni contemporaneamente:

- **Kafka**, con il topic corrispondente all'ambito e il `device_id` come chiave di partizione, in
  modo che le letture di uno stesso dispositivo restino ordinate tra loro
- **MongoDB**, nella collection `raw_telemetry` come archivio grezzo

La scrittura su Mongo avviene in **bulk insert da 1.000 documenti** invece che un `insert_one` per
messaggio: con un milione di record la differenza tra le due strategie è sostanziale. Gli indici su
`device_id` e `timestamp` vengono creati all'avvio. A fine esecuzione lo script riporta il
throughput in messaggi al secondo e la distribuzione effettiva tra i quattro ambiti.

I valori sono generati dentro range realistici, distinguendo le misure continue (temperatura,
vibrazione) da quelle intrinsecamente intere (pezzi prodotti, rpm, ppm, codici RFID, esiti 0/1).

## Consumer — `consumer.py`

Si iscrive ai quattro topic come **consumer group**, così da poter essere scalato su più istanze
che si dividono le partizioni. Per ogni messaggio:

1. **arricchisce** — aggiunge `minuto_esatto`, il timestamp troncato al minuto, che è la chiave su
   cui si appoggiano le aggregazioni per finestra temporale
2. **valuta** — confronta la misura con la soglia del suo tipo e imposta il flag `is_alert`
3. **smista** — scrive sempre in `processed_events`, e duplica in `alerts` solo le letture oltre
   soglia, in modo che le query sugli allarmi non debbano scandire l'intero flusso

## Come si esegue

Servono Kafka su `localhost:9092` e MongoDB su `localhost:27017`.

```bash
pip install kafka-python pymongo faker

python consumer.py   # prima il consumer, così non perde messaggi
python producer.py   # poi il producer, in un secondo terminale
```

Il consumer parte con `auto_offset_reset='earliest'`: se viene avviato in ritardo recupera comunque
i messaggi già presenti nel topic, e si ferma con `Ctrl+C`.

## Stack

Python · Apache Kafka (`kafka-python`) · MongoDB (`pymongo`) · Faker
