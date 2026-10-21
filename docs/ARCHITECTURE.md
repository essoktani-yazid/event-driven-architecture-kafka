# Architecture du Système — Event-Driven Architecture avec Apache Kafka & Spring Cloud Stream

> Ce document détaille l'architecture logicielle, les flux asynchrones, le moteur de traitement en temps réel Kafka Streams et les choix de conception du projet **Event-Driven Architecture Kafka**.

---

## 1. Vue d'ensemble (High-Level Design)

Le projet implémente une **Architecture Événementielle (EDA - Event-Driven Architecture)** hautement scalable et découplée, articulée autour de **Spring Cloud Stream** et **Apache Kafka**.

Elle combine plusieurs paradigmes majeurs du génie logiciel moderne :
1. **Production & Ingestion d'événements :** Publication manuelle (via contrôleur REST et `StreamBridge`) et génération automatisée par échantillonnage continu (`Supplier` cadencé à 200 ms).
2. **Consommation Réactive :** Souscription asynchrone aux messages via des bindings fonctionnels Spring Cloud Stream (`Consumer<PageEvent>`).
3. **Traitement de Flux en Temps Réel (Stream Processing) :** Moteur **Kafka Streams** agrégeant les événements de visites par page sur des fenêtres temporelles glissantes de 5 secondes (`TimeWindows.ofSizeWithNoGrace`).
4. **Requêtes Interactives & Persistance en Mémoire :** Stockage d'état local via un state store RocksDB/In-Memory (`count-store`) interrogé dynamiquement par `InteractiveQueryService`.
5. **Diffusion Réactive & Visualisation Live :** Exposition des résultats sous forme de flux **Server-Sent Events (SSE)** vers une interface web dynamique construite avec **Smoothie.js**.

```mermaid
flowchart TD
    subgraph ClusterDocker ["Infrastructure Docker Compose"]
        ZK["Apache Zookeeper<br/><code>port: 2181</code>"]
        KB["Apache Kafka Broker<br/><code>port: 9092 / 29092</code>"]
        ZK --- KB
        
        subgraph TopicsKafka ["Topics Kafka"]
            T1["Topic T1<br/>(Tests CLI)"]
            T2["Topic T2<br/>(REST & Consumer)"]
            T3["Topic T3<br/>(Supplier Feed)"]
            T4["Topic T4<br/>(Windowed Stream Out)"]
        end
        KB --- TopicsKafka
    end

    subgraph AppSpring ["Application Spring Cloud Stream (Port 8080)"]
        subgraph IngestionCouche ["Couche d'Ingestion"]
            CTL_PUB["PageEventController<br/><code>GET /publish</code>"] -->|StreamBridge.send| T2
            SUPPLIER["PageEventHandler.pageEventSupplier<br/><code>Supplier&lt;PageEvent&gt; (200ms)</code>"] -->|Poller| T3
        end

        subgraph ConsommationCouche ["Couche de Consommation"]
            T2 -->|Subscribe| CONSUMER["PageEventHandler.pageEventConsumer<br/><code>Consumer&lt;PageEvent&gt;</code>"]
        end

        subgraph StreamEngine ["Moteur Kafka Streams (Real-Time Analytics)"]
            T3 -->|KStream In| KFUNC["PageEventHandler.KStreamFunction<br/>• filter(duration > 100)<br/>• map(name, duration)<br/>• groupByKey()<br/>• windowedBy(5s)<br/>• count()"]
            KFUNC -->|Aggregated KStream| T4
            KFUNC -->|Materialized.as| STORE[("Local State Store<br/><code>count-store</code><br/>(ReadOnlyWindowStore)")]
        end

        subgraph ApiReactive ["Exposition Réactive & Interactive Queries"]
            STORE -.->|InteractiveQueryService.getQueryableStore| CTL_ANALYTICS["PageEventController<br/><code>GET /analytics</code><br/><code>Flux&lt;Map&lt;String, Long&gt;&gt;</code> (SSE)"]
        end
    end

    subgraph ClientLayer ["Client & Visualisation Utilisateur"]
        USER((Utilisateur / Navigateur)) -->|HTTP GET /publish| CTL_PUB
        CTL_ANALYTICS -->|EventSource SSE Stream| WEB_UI["Dashboard Web Temps Réel<br/><code>static/index.html</code> (Smoothie.js)"]
        USER -->|Visualisation en direct| WEB_UI
    end
```

---

## 2. Composants Principaux

| Composant | Package / Fichier | Rôle & Responsabilités |
| :--- | :--- | :--- |
| **Apache Zookeeper** | `compose.yml` (`confluentinc/cp-zookeeper:7.3.0`) | Coordination du cluster Kafka, synchronisation des métadonnées et élection de leader. |
| **Kafka Broker** | `compose.yml` (`confluentinc/cp-kafka:7.3.0`) | Courtier de messages distribué gérant les partitions, la réplication et la persistance des topics (`T1`, `T2`, `T3`, `T4`). |
| **Modèle de Domaine** | `net.essoktani.kafkaspringcloudstream.events.PageEvent` | Record Java 17/21 immuable représentant un événement de navigation (`name`, `user`, `date`, `duration`). |
| **REST Event Producer** | `net.essoktani.kafkaspringcloudstream.controllers.PageEventController` | Expose l'endpoint `GET /publish` pour injecter des événements à la demande via `StreamBridge`. |
| **Event Consumer** | `net.essoktani.kafkaspringcloudstream.handlers.PageEventHandler#pageEventConsumer` | Bean `Consumer<PageEvent>` enregistrant et loguant les événements reçus du topic `T2`. |
| **Event Supplier** | `net.essoktani.kafkaspringcloudstream.handlers.PageEventHandler#pageEventSupplier` | Bean `Supplier<PageEvent>` générant automatiquement des visites aléatoires (`P1`, `P2`, `U1`, `U2`) toutes les 200 ms vers `T3`. |
| **Kafka Streams Processor** | `net.essoktani.kafkaspringcloudstream.handlers.PageEventHandler#KStreamFunction` | Pipeline fonctionnel calculant le volume de visites par page sur des fenêtres de 5 secondes avec state store matérialisé `count-store`. |
| **Interactive Query Service** | `net.essoktani.kafkaspringcloudstream.controllers.PageEventController#analytics` | Interroge le store local fenêtré `count-store` et pousse un flux continu SSE (`text/event-stream`) chaque seconde. |
| **Dashboard Web UI** | `src/main/resources/static/index.html` | Page HTML5 légère exploitant **Smoothie.js** pour tracer les courbes de trafic en streaming temps réel. |

---

## 3. Flux de Données & Cycle de Vie des Événements

Le diagramme de séquence ci-dessous illustre l'orchestration complète entre l'ingestion, le stream processing et la restitution graphique :

```mermaid
sequenceDiagram
    autonumber
    actor Dev as Développeur / Client
    participant RestCtrl as PageEventController
    participant Broker as Kafka Broker (Topics)
    participant Consumer as PageEventHandler
    participant KStream as Moteur Kafka Streams
    participant Store as State Store (count-store)
    participant UI as Dashboard Web (Smoothie.js)

    rect rgb(240, 248, 255)
        note over Dev,Consumer: Phase 1 : Publication Manuelle & Consommation
        Dev->>RestCtrl: GET /publish?name=P1&topic=T2
        RestCtrl->>Broker: StreamBridge.send("T2", PageEvent)
        RestCtrl-->>Dev: HTTP 200 (JSON PageEvent)
        Broker->>Consumer: pageEventConsumer(PageEvent)
        Consumer->>Consumer: Log console (****** PageEvent ******)
    end

    rect rgb(255, 250, 240)
        note over Consumer,Store: Phase 2 : Stream Processing Temps Réel
        Consumer->>Broker: pageEventSupplier() produit vers T3 (toutes les 200ms)
        Broker->>KStream: Flux continu KStream<String, PageEvent>
        KStream->>KStream: filter(duration > 100) -> map() -> groupByKey()
        KStream->>KStream: windowedBy(Duration.ofSeconds(5)).count()
        KStream->>Store: Écriture du compte dans count-store
        KStream->>Broker: Émission de l'agrégation vers Topic T4
    end

    rect rgb(245, 255, 250)
        note over RestCtrl,UI: Phase 3 : Requêtes Interactives & Visualisation SSE
        UI->>RestCtrl: Connexion SSE (GET /analytics)
        loop Chaque seconde (Flux.interval)
            RestCtrl->>Store: interactiveQueryService.getQueryableStore("count-store")
            Store-->>RestCtrl: Valeurs agrégées des 5 dernières secondes
            RestCtrl-->>UI: data: {"P1": 15, "P2": 11}
            UI->>UI: smoothieChart.addTimeSeries() -> Rafraîchissement graphique
        end
    end
```

---

## 4. Modèle de Données & Stockage Local

### Structure du Record `PageEvent`
```java
package net.essoktani.kafkaspringcloudstream.events;

import java.util.Date;

public record PageEvent(
    String name,     // Identifiant de la page (ex: P1, P2)
    String user,     // Identifiant de l'utilisateur (ex: U1, U2)
    Date date,       // Timestamp d'occurrence de l'événement
    long duration    // Durée de visite en millisecondes (10 à 10000 ms)
) {}
```

### Table Matérialisée `count-store`
- **Type :** `ReadOnlyWindowStore<String, Long>`
- **Fenêtrage :** 5 secondes (`TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))`)
- **Intervalle de commit :** 1 000 ms (`spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000`)
- **Mécanisme d'interrogation :** `store.fetchAll(Instant.now().minusSeconds(5), Instant.now())` avec agrégation continue via `result.merge(kv.key.key(), kv.value, Long::sum)`.

---

## 5. Déploiement & Configuration Réseau

- **Réseau Docker :** Confluent Kafka (`sdia-kafka-broker`) et Zookeeper (`sdia-zookeeper`) communiquent sur un réseau interne dédié avec protocoles `PLAINTEXT` (port local `9092`, port interne `29092`).
- **Application Spring Boot :** Exécutée sur le port `8080`, elle se connecte directement au broker Kafka local `localhost:9092`.

---
[⬅ Retour au README principal](../README.md)
