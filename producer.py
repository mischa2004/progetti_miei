# ============================================================
# PRODUCER.py — Studente A (IoT Gateway / Producer)
#
# Cosa fa questo file:
# 1. Genera messaggi JSON finti che simulano sensori IoT
# 2. Pubblica ogni messaggio su Kafka (nel topic giusto)
# 3. Salva i dati grezzi su MongoDB (collection raw_telemetry)
# 4. Misura il throughput (quanti messaggi al secondo)
# ============================================================

import json
import time
import random
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
from pymongo import MongoClient

# --- CONFIGURAZIONE ---

KAFKA_SERVER = "localhost:9092"
MONGO_SERVER = "localhost:27017"
MONGO_DATABASE = "smart_factory"

TOTALE_MESSAGGI = 1_000_000    # quanti messaggi generare
BATCH_SIZE = 1_000             # ogni quanti messaggi fare il bulk insert su MongoDB

# --- DEFINIZIONE DEI SENSORI PER AMBITO ---
# Ogni ambito ha una lista di tipi di misura, con unità e range di valori realistici

SENSORI = {
    "ambientale": [
        {"tipo": "temperatura",   "unita": "C",   "min": 15.0, "max": 45.0},
        {"tipo": "umidita",       "unita": "%",   "min": 20.0, "max": 95.0},
        {"tipo": "co2",           "unita": "ppm", "min": 300,  "max": 2000},
    ],
    "macchinari": [
        {"tipo": "vibrazione",         "unita": "mm/s", "min": 0.5,  "max": 15.0},
        {"tipo": "rpm",                "unita": "rpm",  "min": 500,  "max": 5000},
        {"tipo": "consumo_energetico", "unita": "kWh",  "min": 1.0,  "max": 80.0},
        {"tipo": "temperatura_motore", "unita": "C",    "min": 40.0, "max": 120.0},
    ],
    "logistica": [
        {"tipo": "posizione_gps_lat", "unita": "deg",  "min": 45.0, "max": 45.5},
        {"tipo": "posizione_gps_lon", "unita": "deg",  "min": 9.0,  "max": 9.5},
        {"tipo": "lettura_rfid",      "unita": "code", "min": 1000, "max": 9999},
    ],
    "qualita": [
        {"tipo": "pezzi_prodotti",  "unita": "pezzi", "min": 0,  "max": 500},
        {"tipo": "scarti",          "unita": "pezzi", "min": 0,  "max": 50},
        {"tipo": "esito_controllo", "unita": "bool",  "min": 0,  "max": 1},
    ],
}

# Lista dei reparti e delle linee della fabbrica
REPARTI = ["Assemblaggio", "Verniciatura", "Stampaggio", "Magazzino"]
LINEE = ["L1", "L2", "L3", "L4"]

# Lista di device_id pre-generati (10 dispositivi per ambito = 40 totali)
DEVICE_IDS = {
    "ambientale": [f"AMB-{str(i).zfill(3)}" for i in range(1, 11)],
    "macchinari": [f"MAC-{str(i).zfill(3)}" for i in range(1, 11)],
    "logistica":  [f"LOG-{str(i).zfill(3)}" for i in range(1, 11)],
    "qualita":    [f"QUA-{str(i).zfill(3)}" for i in range(1, 11)],
}


# ============================================================
# FUNZIONE: genera un singolo messaggio
# ============================================================
def genera_messaggio(fake, ambito, timestamp_base):
    """
    Crea un messaggio JSON che simula la lettura di un sensore.

    Parametri:
        fake: oggetto Faker per generare dati casuali
        ambito: uno tra "ambientale", "macchinari", "logistica", "qualita"
        timestamp_base: data/ora di partenza per simulare il tempo

    Ritorna:
        un dizionario Python con tutti i campi del messaggio
    """

    # Scegli un sensore casuale per questo ambito
    sensore = random.choice(SENSORI[ambito])

    # Scegli un device_id casuale per questo ambito
    device_id = random.choice(DEVICE_IDS[ambito])

    # Genera un valore casuale nel range del sensore
    valore = round(random.uniform(sensore["min"], sensore["max"]), 2)

    # Costruisci il messaggio
    messaggio = {
        "device_id": device_id,
        "ambito": ambito,
        "timestamp": timestamp_base.isoformat() + "Z",
        "tipo": sensore["tipo"],
        "valore": valore,
        "unita": sensore["unita"],
        "reparto": random.choice(REPARTI),
        "linea": random.choice(LINEE),
    }

    return messaggio


# ============================================================
# FUNZIONE PRINCIPALE
# ============================================================
def main():
    print("=" * 60)
    print("  PRODUCER — Smart Factory IoT")
    print("=" * 60)

    # --- 1. CONNESSIONE A KAFKA ---
    print("\n[1/4] Connessione a Kafka...")
    producer_kafka = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        # Converte il dizionario Python in stringa JSON in bytes
        value_serializer=lambda messaggio: json.dumps(messaggio).encode("utf-8"),
        key_serializer=lambda chiave: chiave.encode("utf-8"),
    )
    print("  [OK] Connesso a Kafka su", KAFKA_SERVER)

    # --- 2. CONNESSIONE A MONGODB ---
    print("\n[2/4] Connessione a MongoDB...")
    client_mongo = MongoClient(MONGO_SERVER)
    db = client_mongo[MONGO_DATABASE]
    collection_raw = db["raw_telemetry"]
    print("  [OK] Connesso a MongoDB, database:", MONGO_DATABASE)

    # --- 3. CREAZIONE INDICI SU MONGODB ---
    # Gli indici rendono veloci le ricerche per device_id e timestamp
    collection_raw.create_index("device_id")
    collection_raw.create_index("timestamp")
    print("  [OK] Indici creati su raw_telemetry")

    # --- 4. GENERAZIONE E INVIO MESSAGGI ---
    print(f"\n[3/4] Generazione di {TOTALE_MESSAGGI:,} messaggi...")
    print(f"  Batch size: {BATCH_SIZE:,} (bulk insert ogni {BATCH_SIZE:,} messaggi)")

    fake = Faker("it_IT")  # Faker in italiano
    ambiti = list(SENSORI.keys())  # ["ambientale", "macchinari", "logistica", "qualita"]
    batch_mongo = []  # lista dove accumuliamo i messaggi per il bulk insert
    timestamp_corrente = datetime(2026, 6, 8, 6, 0, 0)  # partiamo dalle 6:00

    # Contatori
    messaggi_per_ambito = {"ambientale": 0, "macchinari": 0, "logistica": 0, "qualita": 0}

    tempo_inizio = time.time()

    for i in range(1, TOTALE_MESSAGGI + 1):

        # Scegli un ambito casuale
        ambito = random.choice(ambiti)

        # Fai avanzare il timestamp di un po' (simula il passare del tempo)
        timestamp_corrente += timedelta(milliseconds=random.randint(10, 100))

        # Genera il messaggio
        messaggio = genera_messaggio(fake, ambito, timestamp_corrente)

        # ---- PUBBLICA SU KAFKA ----
        # Il topic è il nome dell'ambito, la chiave è il device_id
        producer_kafka.send(
            topic=ambito,
            key=messaggio["device_id"],
            value=messaggio,
        )

        # ---- ACCUMULA PER MONGODB ----
        batch_mongo.append(messaggio)
        messaggi_per_ambito[ambito] += 1

        # ---- BULK INSERT ogni BATCH_SIZE messaggi ----
        if len(batch_mongo) >= BATCH_SIZE:
            collection_raw.insert_many(batch_mongo)
            batch_mongo = []  # svuota il batch

        # Stampa progresso ogni 100.000 messaggi
        if i % 100_000 == 0:
            trascorso = time.time() - tempo_inizio
            velocita = i / trascorso
            print(f"  ... {i:>10,} / {TOTALE_MESSAGGI:,}  ({velocita:,.0f} msg/s)")

    # Salva gli ultimi messaggi rimasti nel batch
    if batch_mongo:
        collection_raw.insert_many(batch_mongo)

    # Assicurati che Kafka abbia inviato tutto
    producer_kafka.flush()

    tempo_fine = time.time()

    # --- 5. RISULTATI ---
    durata = tempo_fine - tempo_inizio
    throughput = TOTALE_MESSAGGI / durata

    print(f"\n[4/4] COMPLETATO!")
    print("=" * 60)
    print(f"  Messaggi generati:  {TOTALE_MESSAGGI:,}")
    print(f"  Tempo totale:       {durata:.1f} secondi")
    print(f"  Throughput:         {throughput:,.0f} messaggi/secondo")
    print(f"\n  Distribuzione per ambito:")
    for ambito, conteggio in messaggi_per_ambito.items():
        percentuale = conteggio / TOTALE_MESSAGGI * 100
        print(f"    {ambito:<15} {conteggio:>10,}  ({percentuale:.1f}%)")
    print("=" * 60)

    # Chiudi le connessioni
    producer_kafka.close()
    client_mongo.close()


# Avvia il programma
if __name__ == "__main__":
    main()
