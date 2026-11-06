import json
from confluent_kafka import Consumer

# Configuration du récepteur
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ia-analytics-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['serre_telemetry'])

print("🧠 Cerveau IA activé. Analyse en temps réel du microclimat...")

while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    if msg.error():
        print(f"Erreur réseau: {msg.error()}")
        continue
    
    # Lecture des capteurs
    data = json.loads(msg.value().decode('utf-8'))
    temp = data['temperature']
    humidite = data['humidite']
    
    print(f"📊 Analyse des capteurs -> Température: {temp}°C | Humidité: {humidite}%")
    
    # --- LOGIQUE DE DÉTECTION ET PRISE DE DÉCISION ---
    if temp > 35.0:
        print("   🚨 ALERTE CRITIQUE : Risque de brûlure des plants (Surchauffe) !")
        print("   ⚙️  ACTION AUTOMATIQUE : Ouverture des fenêtres et activation des ventilateurs...\n")
        
    elif humidite < 40.0:
        print("   ⚠️ ALERTE : Stress hydrique détecté (Air trop sec) !")
        print("   💧 ACTION AUTOMATIQUE : Déclenchement du système d'irrigation goutte-à-goutte...\n")
        
    else:
        print("   ✅ Climat optimal. Aucune action requise.\n")