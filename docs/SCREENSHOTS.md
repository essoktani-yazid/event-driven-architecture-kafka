# Galerie Visuelle & Preuves d'Exécution

> Ce document regroupe l'ensemble des captures d'écran validant le bon fonctionnement de chaque composant de l'architecture événementielle (Cluster Kafka, Ingestion REST, Consumer/Supplier réactifs, Kafka Streams et Dashboard Web).

---

## 1. Infrastructure Conteneurisée

### Démarrage du cluster Kafka & Zookeeper via Docker Compose
Vérification de l'état opérationnel (`Up`) des conteneurs `sdia-zookeeper` (port `2181`) et `sdia-kafka-broker` (port `9092`).

<div align="center">
  <img src="assets/screenshots/01-docker-compose-up.png" alt="Démarrage Docker Compose" width="90%" style="border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);" />
</div>

---

### Test interactif Console Producer & Consumer sur Topic T1
Validation des échanges directs en ligne de commande entre le producteur et le consommateur natifs de Kafka.

<div align="center">
  <img src="assets/screenshots/02-kafka-console-producer-consumer.png" alt="Console Producer et Consumer sur T1" width="90%" style="border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);" />
</div>

---

## 2. Ingestion & Consommation Réactive

### Publication d'événements via le Contrôleur REST (`/publish`)
Appel HTTP GET déclenchant l'envoi d'un événement `PageEvent` vers le topic `T2` au moyen de `StreamBridge`.

<div align="center">
  <img src="assets/screenshots/03-rest-publish-event.png" alt="Publication REST PageEvent" width="90%" style="border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);" />
</div>

---

### Logs d'interception du consommateur (`pageEventConsumer`)
Réception asynchrone et affichage formaté de l'événement intercepté dans la console de l'application Spring Boot.

<div align="center">
  <img src="assets/screenshots/04-console-consumer-log.png" alt="Logs Consumer Spring Cloud Stream" width="90%" style="border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);" />
</div>

---

## 3. Streaming Continu & Traitement Fenêtré Kafka Streams

### Production automatisée périodique du Supplier sur le Topic T3
Flux continu d'événements `PageEvent` générés automatiquement toutes les 200 ms par le bean `pageEventSupplier`.

<div align="center">
  <img src="assets/screenshots/05-supplier-poller-topic.png" alt="Supplier Poller Topic T3" width="90%" style="border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);" />
</div>

---

### Agrégation fenêtrée en temps réel Kafka Streams sur le Topic T4
Résultat du traitement analytique (`KStreamFunction`) partitionné par clé de page (`P1`, `P2`) et comptabilisé sur des fenêtres glissantes de 5 secondes.

<div align="center">
  <img src="assets/screenshots/06-kafka-streams-window-count.png" alt="Kafka Streams Windowed Count sur T4" width="90%" style="border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);" />
</div>

---

## 4. Exposition Réactive & Interface Utilisateur

### Flux réactif Server-Sent Events (SSE) via `/analytics`
Diffusion en direct (1 paquet par seconde) des comptages récents agrégés depuis le state store local `count-store`.

<div align="center">
  <img src="assets/screenshots/07-analytics-sse-endpoint.png" alt="SSE Analytics Endpoint" width="90%" style="border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);" />
</div>

---

### Tableau de bord web interactif en streaming (Smoothie.js)
Visualisation dynamique et fluide des courbes de trafic en temps réel pour chaque page consultée.

<div align="center">
  <img src="assets/screenshots/08-realtime-web-dashboard.png" alt="Tableau de bord Smoothie.js" width="90%" style="border-radius: 8px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);" />
</div>

---

[⬅ Retour au README principal](../README.md)
