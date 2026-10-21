<div align="center">
  <!-- En-tête créatif animé -->
  <img src="https://capsule-render.vercel.app/api?type=waving&color=00B4D8&height=150&section=header&text=EVENT-DRIVEN%20KAFKA%20STREAM&fontSize=42&fontAlignY=35&fontColor=ffffff" width="100%"/>

  <!-- Animation texte descriptive -->
  <img src="https://readme-typing-svg.herokuapp.com?font=JetBrains+Mono&weight=700&size=19&duration=3000&pause=1000&color=00B4D8&center=true&vCenter=true&width=650&lines=Architecture+%C3%89v%C3%A9nementielle+avec+Spring+Cloud+Stream;Traitement+de+Flux+Temps+R%C3%A9el+Kafka+Streams;Visualisation+Dynamique+SSE+%26+Smoothie.js;Agr%C3%A9gation+Fen%C3%AAtre+5s+%26+Interactive+Query+Store" alt="Typing SVG" />

  <br/>

  <!-- Badges technologiques -->
  <p>
    <img src="https://img.shields.io/badge/version-1.0.0-blue?style=for-the-badge" alt="Version">
    <img src="https://img.shields.io/badge/Java-17%20%7C%2021-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white" alt="Java">
    <img src="https://img.shields.io/badge/Spring_Boot-4.1.1-6DB33F?style=for-the-badge&logo=spring-boot&logoColor=white" alt="Spring Boot">
    <img src="https://img.shields.io/badge/Apache_Kafka-7.3.0-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white" alt="Kafka">
    <img src="https://img.shields.io/badge/Docker_Compose-Supported-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
    <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License">
  </p>
</div>

---

## À propos du projet

**Event-Driven Architecture Kafka** est une solution complète de traitement et de visualisation d'événements en streaming conçue avec **Spring Cloud Stream** et **Apache Kafka**.

Elle permet aux ingénieurs et développeurs de construire une chaîne de traitement événementielle réactive, intégrant :
- L'ingestion asynchrone de messages via REST (`StreamBridge`) et génération périodique continue (`Supplier` cadencé à 200 ms).
- Le traitement de flux en temps réel avec **Kafka Streams** (filtrage, partitionnement, fenêtrage de 5 secondes et agrégation).
- La matérialisation de l'état dans un store local interrogeable (`count-store` via `InteractiveQueryService`).
- La diffusion réactive vers le client via **Server-Sent Events (SSE)** et le rendu graphique animé avec **Smoothie.js**.

> [!NOTE]
> Pour une analyse approfondie des choix techniques et des schémas d'architecture détaillés, consultez le document [Architecture du Système](docs/ARCHITECTURE.md) et la [Référence des API](docs/API.md).

---

## Fonctionnalités Clés

- **Architecture Découplée & Asynchrone :** Communication sans adhérence directe entre producteurs et consommateurs grâce aux topics Kafka.
- **Moteur Kafka Streams Intégré :** Traitement analytique en temps réel calculant la fréquence de consultation de pages sur fenêtres temporelles glissantes sans blocage.
- **Stockage d'État Local Haute Performance :** Utilisation d'un state store RocksDB/In-Memory interrogeable dynamiquement à la demande.
- **Diffusion Réactive SSE :** Transmission en streaming continu des données calculées sans rechargement ni polling agressif.
- **Dashboard Dynamique Ultra-Fluide :** Visualisation par courbes de navigation interactives grâce à la bibliothèque Smoothie Charts.

---

## Stack Technique

| Couche | Technologies |
| :--- | :--- |
| **Backend & Ingestion** | Java 17/21, Spring Boot 4.1.1, Spring Cloud Stream 2025.1.3 |
| **Streaming & Message Broker** | Apache Kafka 7.3.0, Confluent Platform, Kafka Streams Binder |
| **Frontend & Visualisation** | HTML5, JavaScript moderne, Smoothie.js (Canvas temps réel) |
| **Infrastructure & DevOps** | Docker, Docker Compose, Apache Zookeeper, Maven Wrapper |

---

## Aperçu Visuel & Galerie

Le bon fonctionnement de chaque étape du pipeline (infrastructure conteneurisée, ingestion REST, streaming continu, traitement fenêtré Kafka Streams et restitution graphique) est illustré par des captures d'écran réelles :

<div align="center">
  <table width="100%">
    <tr>
      <td width="50%" align="center">
        <b>Tableau de Bord Smoothie.js (Temps Réel)</b><br/><br/>
        <img src="docs/assets/screenshots/08-realtime-web-dashboard.png" alt="Dashboard" width="100%" style="border-radius: 6px;" />
      </td>
      <td width="50%" align="center">
        <b>Cluster Kafka & Zookeeper (Docker)</b><br/><br/>
        <img src="docs/assets/screenshots/01-docker-compose-up.png" alt="Docker Cluster" width="100%" style="border-radius: 6px;" />
      </td>
    </tr>
  </table>

  <br/>
  
  <p>
    👉 <b><a href="docs/SCREENSHOTS.md">Consulter la galerie complète des 8 captures d'écran d'exécution &rarr;</a></b>
  </p>
</div>

---

## Pour commencer

### Prérequis

- **Docker & Docker Compose** (version 24.0 ou supérieure)
- **Java JDK 17 ou 21**
- **Maven** (ou utilisation directe du wrapper `./mvnw`)

### Installation & Lancement

1. **Cloner le projet :**
   ```bash
   git clone https://github.com/essoktani-yazid/event-driven-architecture-kafka.git
   cd event-driven-architecture-kafka
   ```

2. **Démarrer les conteneurs Kafka & Zookeeper :**
   ```bash
   docker compose up -d
   ```

3. **Compiler et exécuter l'application Spring Boot :**
   ```bash
   ./mvnw clean spring-boot:run
   ```

4. **Accéder aux interfaces et services :**
   - API de publication : `http://localhost:8080/publish?name=P1&topic=T2`
   - Flux analytique SSE : `http://localhost:8080/analytics`
   - Dashboard Smoothie.js : `http://localhost:8080/index.html`

---

## Structure du Projet

```text
.
├── compose.yml                                       # Orchestration Docker (Kafka Broker & Zookeeper)
├── pom.xml                                           # Dépendances Maven (Spring Boot, Cloud Stream, KStreams)
├── docs/
│   ├── ARCHITECTURE.md                               # Schémas et description détaillée de l'architecture
│   ├── API.md                                        # Spécification exhaustive des endpoints et contrats
│   ├── SCREENSHOTS.md                                # Galerie complète des 8 captures d'écran
│   └── assets/
│       └── screenshots/                              # Fichiers images des captures d'écran
├── src/
│   ├── main/
│   │   ├── java/net/essoktani/kafkaspringcloudstream/
│   │   │   ├── KafkaSpringCloudStreamApplication.java # Point d'entrée Spring Boot
│   │   │   ├── controllers/
│   │   │   │   └── PageEventController.java          # Endpoints REST /publish et réactif SSE /analytics
│   │   │   ├── events/
│   │   │   │   └── PageEvent.java                    # Modèle de données immuable (Record)
│   │   │   └── handlers/
│   │   │       └── PageEventHandler.java            # Consumer, Supplier et fonction de traitement KStream
│   │   └── resources/
│   │       ├── application.properties                # Configuration des bindings et du binder Kafka Streams
│   │       └── static/
│   │           └── index.html                        # Dashboard Smoothie.js temps réel
│   └── test/
│       └── java/net/essoktani/kafkaspringcloudstream/
│           └── KafkaSpringCloudStreamApplicationTests.java
├── LICENSE                                           # Licence MIT officielle
└── README.md                                         # Documentation principale du projet
```

---

## Auteur

<div align="center">

**Yazid ESSOKTANI**  
*Full-Stack Web & AI Developer*

[![Portfolio](https://img.shields.io/badge/Portfolio-essoktani.tech-00B4D8?style=for-the-badge&logo=google-chrome&logoColor=white)](https://www.essoktani.tech/)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Yazid_ESSOKTANI-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/yazid-essoktani-337549251)
[![GitHub](https://img.shields.io/badge/GitHub-@essoktani--yazid-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/essoktani-yazid)

</div>

---

## Licence

Distribué sous la licence **MIT**. Consultez le fichier [`LICENSE`](LICENSE) pour plus d'informations.
