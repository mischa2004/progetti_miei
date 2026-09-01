# ============================================================
# CONSUMER.py — Studente B (Stream Processor / Consumer)
#
# Cosa fa questo file:
# 1. Si iscrive ai 4 topic Kafka come un Consumer Group
# 2. Legge i messaggi man mano che il Producer li genera
# 3. Valida e Arricchisce i dati (allarmi, formato data)
# 4. Salva i risultati su MongoDB in `processed_events` e `alerts`
# ============================================================

import json
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient

# --- CONFIGURAZIONE ---
KAFKA_SERVER = "localhost:9092"
MONGO_SERVER = "localhost:27017"
MONGO_DATABASE = "smart_factory"

TOPICS = ["ambientale", "macchinari", "logistica", "qualita"]
CONSUMER_GROUP = "gruppo-studenteb-1"

# Le soglie di allarme dal nostro contratto dati
SOGLIE_ALLARME = {
    "temperatura": 35.0,
    "umidita": 80.0,
    "co2": 1000.0,
    "vibrazione": 8.0,
    "rpm": 4000.0,
    "consumo_energetico": 50.0,
    "temperatura_motore": 85.0
}


def main():
    print("=" * 60)
    print("  CONSUMER — Smart Factory IoT")
    print("=" * 60)

    # --- 1. CONNESSIONE A MONGODB ---
    print("\n[1/3] Connessione a MongoDB...")
    client_mongo = MongoClient(MONGO_SERVER)
    db = client_mongo[MONGO_DATABASE]
    
    col_processed = db["processed_events"]
    col_alerts = db["alerts"]
    
    # Creiamo gli indici per velocizzare le query future
    col_processed.create_index("device_id")
    col_processed.create_index("minuto_esatto")
    col_alerts.create_index("tipo")
    print("  [OK] Connesso a MongoDB. Tabelle pronte.")

    # --- 2. CONNESSIONE A KAFKA ---
    print("\n[2/3] Connessione a Kafka...")
    consumer = KafkaConsumer(
        *TOPICS, # Si iscrive a tutti i topic nella lista
        bootstrap_servers=KAFKA_SERVER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest', # Se parte in ritardo, legge dall'inizio
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) # Trasforma in dizionario
    )
    print(f"  [OK] Connesso ai topic: {TOPICS}")
    print("\n[3/3] In attesa di messaggi... (Premi Ctrl+C per fermare)\n")

    # --- 3. LOOP INFINITO DI LETTURA ---
    # Man mano che i messaggi arrivano in Kafka, questo loop si attiva
    contatore = 0
    contatore_allarmi = 0

    try:
        for messaggio_kafka in consumer:
            # Estraggo il payload JSON vero e proprio
            dato = messaggio_kafka.value
            
            # ---------------------------------------------
            # FASE A: VALIDAZIONE (simulata)
            # In questo caso sappiamo che il dato è buono
            # ---------------------------------------------
            
            # ---------------------------------------------
            # FASE B: ARRICCHIMENTO
            # ---------------------------------------------
            
            # 1. Creiamo un formato temporale comodo "YYYY-MM-DD HH:MM" troncando i secondi
            try:
                # Togliamo la "Z" finale e lasciamo leggere la data a fromisoformat.
                # Attenzione: il Producer scrive i millisecondi solo quando ci sono, quindi
                # ogni tanto arriva un timestamp "tondo" tipo 2026-06-08T06:00:20Z.
                # Un formato fisso con .%f fallirebbe proprio su quelli.
                dt = datetime.fromisoformat(dato["timestamp"].removesuffix("Z"))
                # Rimettiamo in stringa, ma togliamo secondi e millisecondi
                dato["minuto_esatto"] = dt.strftime("%Y-%m-%d %H:%M:00")
            except Exception:
                # Se per caso il formato della data fosse strano, metto una stringa vuota
                dato["minuto_esatto"] = ""

            # 2. Controllo le soglie di allarme
            tipo_misura = dato["tipo"]
            valore_misura = dato["valore"]
            is_alert = False
            
            if tipo_misura in SOGLIE_ALLARME:
                soglia = SOGLIE_ALLARME[tipo_misura]
                if valore_misura > soglia:
                    is_alert = True
            
            # Aggiungo il nuovo campo al JSON
            dato["is_alert"] = is_alert

            # ---------------------------------------------
            # FASE C: SALVATAGGIO
            # ---------------------------------------------
            
            # Salvo sempre il dato pulito e arricchito nella tabella principale
            col_processed.insert_one(dato)
            
            # Se è un allarme, ne faccio una COPIA nella tabella degli allarmi
            if is_alert:
                col_alerts.insert_one(dato)
                contatore_allarmi += 1

            # Stampo un aggiornamento ogni tanto
            contatore += 1
            if contatore % 10000 == 0:
                print(f"  Elaborati: {contatore:,} messaggi | Allarmi trovati: {contatore_allarmi:,}")

    except KeyboardInterrupt:
        print("\nArresto manuale del Consumer.")
    finally:
        consumer.close()
        client_mongo.close()
        print("\nConsumer terminato e disconnesso.")

if __name__ == "__main__":
    main()
