# Référence API — Event-Driven Architecture Kafka

> Documentation exhaustive des endpoints HTTP, flux réactifs SSE et contrats d'événements Kafka.

- **URL de base :** `http://localhost:8080`
- **Format d'échange :** `application/json` & `text/event-stream`
- **Port d'écoute :** `8080`

---

## 1. Endpoints HTTP & Réactifs

### 1.1 Publier un événement (`GET /publish`)

Permet d'injecter manuellement un événement de visite `PageEvent` vers un topic Kafka cible en exploitant le `StreamBridge` de Spring Cloud Stream.

- **URL :** `/publish`
- **Méthode :** `GET`
- **Authentification requise :** Non (Accès public de développement)

#### Paramètres de requête (Query Params)

| Paramètre | Type | Requis | Description | Exemple |
| :--- | :--- | :--- | :--- | :--- |
| `name` | `string` | Oui | Identifiant de la page consultée | `P1`, `P2`, `HomePage` |
| `topic` | `string` | Oui | Nom du topic Kafka de destination | `T2` |

#### Exemple d'appel cURL

```bash
curl -X GET "http://localhost:8080/publish?name=P1&topic=T2"
```

#### Réponse de succès (`200 OK`)

```json
{
  "name": "P1",
  "user": "U1",
  "date": "2026-10-21T09:45:12.345+00:00",
  "duration": 4821
}
```

---

### 1.2 Flux d'analytique temps réel (`GET /analytics`)

Émet un flux réactif continu **Server-Sent Events (SSE)** poussant chaque seconde le nombre d'événements comptabilisés par page sur la dernière fenêtre glissante de 5 secondes, calculé par le moteur **Kafka Streams** via son state store local `count-store`.

- **URL :** `/analytics`
- **Méthode :** `GET`
- **En-tête requis :** `Accept: text/event-stream`
- **Authentification requise :** Non

#### Exemple d'appel cURL

```bash
curl -N -X GET "http://localhost:8080/analytics"
```

#### Exemple de flux SSE émis (`200 OK`)

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream;charset=UTF-8
Transfer-Encoding: chunked

data:{"P1":14,"P2":9}

data:{"P1":18,"P2":12}

data:{"P1":15,"P2":14}
```

> [!NOTE]
> Si le state store Kafka Streams n'a pas encore fini de rééquilibrer ses partitions au démarrage, l'endpoint retourne une map vide `{}` sans interrompre le flux (`onErrorContinue`).

---

## 2. Contrats d'Événements Kafka (Event Schemas)

### Structure du Record `PageEvent`

Les topics d'ingestion (`T2` et `T3`) transportent des messages au format JSON standard correspondant au record Java :

```json
{
  "name": "P1",
  "user": "U2",
  "date": "2026-10-21T10:15:30.000Z",
  "duration": 5230
}
```

| Champ | Type | Description |
| :--- | :--- | :--- |
| `name` | `String` | Nom de la page consultée (`P1`, `P2`) |
| `user` | `String` | Utilisateur ayant visité la page (`U1`, `U2`) |
| `date` | `Date / ISO String` | Horodatage précis de la visite |
| `duration` | `Long` | Durée de séjour sur la page en millisecondes |

### Messages du Topic Agrégé `T4`

Le topic `T4` est alimenté par la fonction Kafka Streams `KStreamFunction`. Les messages y sont publiés sous forme de paires clé-valeur :
- **Clé (String) :** Nom de la page (`P1`, `P2`).
- **Valeur (Long) :** Nombre d'occurrences comptabilisées sur la fenêtre active de 5 secondes.

---

## 3. Codes de Statut HTTP

| Code HTTP | Signification | Description |
| :--- | :--- | :--- |
| `200 OK` | Succès | Requête traitée avec succès / Flux SSE ouvert. |
| `400 Bad Request` | Paramètre manquant | Paramètre obligatoire (`name` ou `topic`) absent de la requête. |
| `500 Internal Error` | Erreur serveur | Broker Kafka inaccessible ou erreur de sérialisation. |

---
[⬅ Retour au README principal](../README.md)
