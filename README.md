# 🚀 Data Control Plane: MongoDB → Kafka → ClickHouse (CDC) orchestrated by Dagster

Bienvenido a tu nuevo **Data Control Plane**. Este sistema te permite añadir nuevas entidades de MongoDB a tu data warehouse de ClickHouse en tiempo real usando **CDC (Change Data Capture)** con solo declarar un archivo YAML.

Este manual está diseñado tanto para guiarte en el funcionamiento local como para enseñarte a replicar esta arquitectura exacta en el clúster de **EKS (Elastic Kubernetes Service)** de tu empresa.

---

## 🏗️ Arquitectura de Referencia

La arquitectura del flujo de datos en tiempo real está estructurada de la siguiente manera:

```
+-------------------------------------------------------------------------------+
|                             CONTROL PLANE (DAGSTER)                           |
|  - Sensor: Detecta YAMLs en /datasets/                                        |
|  - Asset Graph: Define la jerarquía de dependencias y lanza onboarding        |
+-------------------------------------------------------------------------------+
                                       │
                                       ▼ (Orquestación & Creación de Recursos)
+-----------------+      +--------------------+      +-------------------------+
|     MONGODB     | ───> |    KAFKA TOPIC     | ───> |       CLICKHOUSE        |
|  (Source DB with|      |  (Message broker)  |      |   (ReplacingMergeTree)  |
|  Replica Set)   |      +--------------------+      +-------------------------+
+-----------------+                 ▲                             ▲
         │                          │                             │
         ▼                          │                             │
+-----------------+                 │                             │ (Piped Stream)
| DEBEZIUM CONNEC. | ────────────────┘                             │
| (Kafka Connect) | (CDC Streams oplog/Change Stream)             │
+-----------------+                                               │
         │                                                        │
         └────────────────────────────────────────────────────────┘
                    (Creates: 1. Kafka Queue Engine Table
                              2. Materialized View Pipe
                              3. ReplacingMergeTree target table)
```

---

## 🎓 Conceptos Fundamentales (¡Aprende mientras montas!)

Para dominar este stack, primero debes entender el rol y comportamiento de cada una de las piezas móviles:

### 1. ¿Qué es Dagster en esta Arquitectura? (El Cerebro)
Dagster es una plataforma de orquestación moderna basada en **activos de datos (declarativos)** en lugar de simples tareas secuenciales (imperativos). En este proyecto:
*   **Sensor de Datasets**: Un sensor monitoriza el directorio `datasets/*.yaml`. Cada vez que creas o modificas un archivo YAML, Dagster detecta el cambio automáticamente, regenera el grafo de dependencias y lanza el **Grafo de Onboarding** para esa nueva tabla.
*   **Asset Graph Declarativo**: Dagster entiende que para que exista ClickHouse en tiempo real, primero debe existir el tópico en Kafka y el conector de Debezium. Las dependencias están modeladas explícitamente.

### 2. ¿Cómo funciona el CDC de MongoDB a Kafka con Debezium?
Debezium se suscribe al **Oplog (Operations Log)** de MongoDB utilizando *Change Streams* (una característica nativa de MongoDB que requiere Replica Sets).
*   Cada insert, update o delete en MongoDB genera un evento con metadatos ricos.
*   **Extractor de Estado (`ExtractNewDocumentState`)**: Los eventos nativos de Debezium MongoDB tienen un formato anidado complejo (`before`, `after`, `op`, `source`). Usamos esta transformación SMT (Single Message Transformation) para **aplanar el JSON** al formato del documento original.
*   **Campos CDC Especiales**: Añadimos campos virtuales al mensaje JSON:
    *   `__op`: `r` (snapshot/lectura), `c` (crear), `u` (actualizar), `d` (eliminar).
    *   `__source_ts_ms`: Marca de tiempo en milisegundos de la base de datos origen. Se usa como número de versión para deduplicar.
    *   `__deleted`: Booleano que indica si el registro fue borrado en origen.

### 3. ClickHouse como Motor de Streaming Deduplicado
ClickHouse destaca por consultas analíticas ultrarrápidas, pero no soporta bien actualizaciones continuas in-place (mutaciones). Para lograr CDC en tiempo real a alta velocidad, implementamos un patrón de 3 objetos:
1.  **ReplacingMergeTree (Tabla Destino)**: Una tabla especial de ClickHouse que deduplica registros asíncronamente basándose en una clave primaria y un número de versión (`_version` mapeado de `__source_ts_ms`). Si llega un cambio con una versión mayor, sobrescribe el registro previo.
2.  **Kafka Engine Table (La Cola)**: Una tabla con el motor `Kafka` que actúa como un consumidor nativo. No almacena datos; simplemente consume mensajes en streaming desde tu tópico de Kafka.
3.  **Materialized View (La Tubería)**: Un proceso continuo en ClickHouse que lee en batch de la tabla de la cola (Kafka Engine), aplica las transformaciones necesarias y escribe los datos en la tabla ReplacingMergeTree destino.

---

## 🚀 Guía Rápida: Uso Local

Para validar el sistema en local, puedes levantar todo el entorno contenerizado en tu máquina:

```bash
# 1. Levantar toda la infraestructura (MongoDB Replica Set, Kafka, Kafka Connect con Debezium, ClickHouse, Dagster)
make up

# 2. Cargar datos de prueba iniciales en MongoDB
make seed

# 3. Acceder a la interfaz de Dagster en tu navegador
# -> http://localhost:3000
# Haz clic en "Materialize All" para ejecutar el Onboarding automático.

# 4. Verificar la carga inicial en ClickHouse
make clickhouse-cli
# Dentro de ClickHouse:
SELECT count() FROM analytics.users;
```

---

## 🛠️ Onboarding de un Nuevo Dataset

Para añadir una nueva tabla de MongoDB al pipeline automatizado, solo sigue este flujo:

### Paso 1: Generar el archivo YAML
Ejecuta el script generador desde el Makefile:
```bash
make add-dataset name=products
```
Esto creará el archivo `datasets/products.yaml` basado en la plantilla de `/templates/dataset_template.yaml`.

### Paso 2: Editar la estructura del YAML
Abre `datasets/products.yaml` y configura los campos de tu colección de MongoDB y tipos de datos en ClickHouse:
```yaml
dataset:
  name: products
  source:
    mongodb:
      database: app_db
      collection: products
  kafka:
    topic_name: "cdc.app_db.products"
    partitions: 3
    replication_factor: 1
  clickhouse:
    database: analytics
    table_name: products
    engine: ReplacingMergeTree
    order_by:
      - _id
    version_column: _version
    columns:
      - name: _id
        type: String
      - name: name
        type: String
      - name: price
        type: Float64
      - name: created_at
        type: DateTime64(3)
      - name: _version
        type: UInt64
        default: "0"
      - name: _deleted
        type: UInt8
        default: "0"
```

### Paso 3: Onboarding Automático
El sensor de Dagster detectará el YAML al instante. Al materializar, Dagster realizará automáticamente:
1.  **Crear el Tópico en Kafka** con las particiones y replicación configuradas.
2.  **Registrar el conector CDC de Debezium** en Kafka Connect. Debezium iniciará un snapshot inicial de la tabla existente en MongoDB y la publicará en Kafka.
3.  **Crear los Objetos de ClickHouse**:
    *   Crea la base de datos destino.
    *   Crea la tabla `ReplacingMergeTree` para la analítica.
    *   Crea la tabla intermedia `Kafka Engine` de ClickHouse apuntando al tópico.
    *   Crea la `Materialized View` que actúa como tubería streaming de Kafka a la tabla destino.
4.  **Carga del Histórico**: Ejecuta una inserción masiva y directa del histórico desde MongoDB a ClickHouse para garantizar velocidad de onboarding, marcando un offset base.
5.  **Streaming en Tiempo Real**: Todo insert/update posterior de MongoDB será capturado por Debezium, publicado en Kafka, recogido por el motor Kafka de ClickHouse e insertado en tiempo real a la tabla destino.

---

## ⚓ Guía de Migración a Producción: Clúster Kubernetes (EKS)

Para migrar esta implementación local al clúster de **Amazon EKS** de tu empresa, debes desplegar los servicios usando manifiestos nativos o charts de Helm y adaptar los recursos del proyecto Dagster.

### 1. MongoDB en EKS
*   **Oplog y Replica Set**: Para habilitar CDC, tu clúster de MongoDB en producción (sea autogestionado en EKS mediante un operador como KubeDB, o administrado como AWS DocumentDB/MongoDB Atlas) **debe tener activado el Replica Set**.
*   Si usas **AWS DocumentDB**, asegúrate de habilitar los *Change Streams* ejecutando:
    ```json
    db.adminCommand({modifyChangeStreams: 1, database: "app_db", collection: "", enable: true});
    ```

### 2. Desplegar Kafka y Kafka Connect (Debezium)
La forma más recomendada para gestionar Kafka en producción sobre Kubernetes es **Strimzi Kafka Operator**.

#### Paso A: Instalar Strimzi mediante Helm
```bash
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi-operator strimzi/strimzi-kafka-operator --namespace kafka --create-namespace
```

#### Paso B: Desplegar tu Clúster de Kafka (Custom Resource)
Crea un archivo llamado `kafka-cluster.yaml`:
```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: production-kafka-cluster
  namespace: kafka
spec:
  kafka:
    version: 3.6.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
    storage:
      type: persistent-claim
      size: 100Gi
      class: gp3 # AWS EBS GP3
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 20Gi
      class: gp3
```
Despliégalo: `kubectl apply -f kafka-cluster.yaml`.

#### Paso C: Construir y Desplegar Kafka Connect con Debezium
Para producción, debes empaquetar los plugins de Debezium MongoDB en una imagen personalizada de Kafka Connect y desplegarla con Strimzi.
Crea un `KafkaConnect` Custom Resource (`kafka-connect.yaml`):
```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
metadata:
  name: debezium-connect-cluster
  namespace: kafka
spec:
  replicas: 2
  bootstrapServers: production-kafka-cluster-kafka-bootstrap:9092
  image: quay.io/strimzi/kafka:0.38.0-kafka-3.6.0 # O tu Dockerfile personalizado con el jar de Debezium Mongo
  config:
    group.id: debezium-connect-cluster
    offset.storage.topic: debezium-connect-offsets
    config.storage.topic: debezium-connect-configs
    status.storage.topic: debezium-connect-status
    key.converter: org.apache.kafka.connect.json.JsonConverter
    value.converter: org.apache.kafka.connect.json.JsonConverter
    key.converter.schemas.enable: false
    value.converter.schemas.enable: false
```
Despliégalo: `kubectl apply -f kafka-connect.yaml`.

### 3. ClickHouse en EKS
En Kubernetes, se recomienda utilizar el **Altinity ClickHouse Operator** para garantizar alta disponibilidad y gestión automática del almacenamiento persistente GP3 de AWS.

#### Paso A: Instalar Altinity Operator
```bash
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml
```

#### Paso B: Crear un Clúster ClickHouse con almacenamiento GP3
Crea `clickhouse-cluster.yaml`:
```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "prod-clickhouse"
  namespace: clickhouse
spec:
  configuration:
    clusters:
      - name: "prod-ch-cluster"
        layout:
          shardsCount: 1
          replicasCount: 2 # Alta Disponibilidad
    users:
      default/password: "PonTuPasswordSeguroAqui"
  defaults:
    templates:
      volumeClaimTemplate: gp3-volume-template
  templates:
    volumeClaimTemplates:
      - name: gp3-volume-template
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 200Gi
          storageClassName: gp3
```
Despliégalo: `kubectl apply -f clickhouse-cluster.yaml`.

### 4. Desplegar Dagster en EKS (Control Plane)
Dagster dispone de un Helm Chart robusto diseñado para separar la plataforma web (Control Tower) del código del desarrollador (User Code) que ejecuta los pipelines.

#### Paso A: Crear tu imagen Docker de User Code
Debes empaquetar el contenido del proyecto (la carpeta `/dagster_project`, tus YAMLs y las dependencias de Python) en una imagen de Docker y subirla a Amazon ECR:
```dockerfile
FROM python:3.11-slim

WORKDIR /opt/dagster/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY dagster_project/ /opt/dagster/app/dagster_project/
COPY datasets/ /opt/dagster/app/datasets/
COPY workspace.yaml .

ENV DAGSTER_CURRENT_REPOSITORY=dagster_project
```
Construye y sube tu imagen:
```bash
docker build -t <tu_cuenta_aws>.dkr.ecr.<region>.amazonaws.com/dagster-user-code:latest .
docker push <tu_cuenta_aws>.dkr.ecr.<region>.amazonaws.com/dagster-user-code:latest
```

#### Paso B: Configurar Helm Chart de Dagster en EKS
Crea un archivo de configuración de Helm (`values-eks.yaml`):
```yaml
dagsterWebserver:
  replicaCount: 1

dagsterDaemon:
  replicaCount: 1

postgresql:
  enabled: true # Utiliza PostgreSQL local en Kubernetes para los metadatos o apunta a Amazon RDS PostgreSQL

# Define tus despliegues de Código de Usuario
userDeployments:
  enabled: true
  deployments:
    - name: "dagster-user-code-prod"
      image:
        repository: "<tu_cuenta_aws>.dkr.ecr.<region>.amazonaws.com/dagster-user-code"
        tag: "latest"
        pullPolicy: "Always"
      env:
        MONGODB_URI: "mongodb://<mongo-credentials>@prod-mongodb:27017/?replicaSet=rs0"
        MONGODB_DATABASE: "app_db"
        KAFKA_BOOTSTRAP_SERVERS: "production-kafka-cluster-kafka-bootstrap.kafka.svc:9092"
        KAFKA_CONNECT_URL: "http://debezium-connect-cluster-connect-api.kafka.svc:8083"
        CLICKHOUSE_HOST: "clickhouse-prod-clickhouse.clickhouse.svc"
        CLICKHOUSE_PORT: "8123"
        CLICKHOUSE_USER: "default"
        CLICKHOUSE_PASSWORD: "PonTuPasswordSeguroAqui"
        DATASETS_DIR: "/opt/dagster/app/datasets"
```

Instala Dagster en el clúster:
```bash
helm repo add dagster https://dagster-io.github.io/helm
helm install dagster-control-plane dagster/dagster --namespace dagster --create-namespace -f values-eks.yaml
```

---

## 🔌 Integración en un Dagster Existente (Clúster de tu Empresa)

Si tu empresa **ya tiene en funcionamiento** un clúster de EKS con Dagster, Kafka/Debezium y Clickhouse operando, no tienes que redesplegar nada desde cero. Solo debes integrar nuestro módulo Python y registrar las definiciones en tu proyecto Dagster actual.

Sigue estos sencillos pasos para integrarlo:

### Paso 1: Copiar la Estructura de Código a tu Repositorio Git
Copia la estructura del Data Control Plane a la raíz o submódulo de código de tu proyecto Dagster existente:
```
tu-repositorio-dagster/
├── datasets/                 # Directorio donde irán los YAMLs de tus nuevas tablas
│   ├── users.yaml
│   └── orders.yaml
└── dagster_project/          # O el nombre de tu paquete Python actual
    ├── assets/
    │   └── dataset_factory.py
    ├── models/
    │   └── dataset_config.py
    ├── resources/            # Si ya tienes recursos definidos, añade estos 4 recursos
    │   ├── clickhouse_resource.py
    │   ├── debezium_resource.py
    │   ├── kafka_resource.py
    │   └── mongodb_resource.py
    ├── sensors/
    │   └── yaml_sensor.py
    └── utils/
        ├── clickhouse_ddl.py
        └── debezium_config.py
```

### Paso 2: Fusionar las Definiciones (`definitions.py` o `__init__.py`)
En el archivo de entrada donde declaras tu objeto `Definitions` actual de Dagster, impórtalo y fusiónalo de la siguiente forma:

```python
import os
import dagster as dg

# 1. Importa nuestro creador dinámico de assets y recursos
from dagster_project.assets.dataset_factory import build_all_dataset_assets
from dagster_project.resources.clickhouse_resource import ClickHouseResource
from dagster_project.resources.debezium_resource import DebeziumResource
from dagster_project.resources.kafka_resource import KafkaAdminResource
from dagster_project.resources.mongodb_resource import MongoDBResource
from dagster_project.sensors.yaml_sensor import dataset_yaml_sensor

# 2. Genera los assets dinámicos a partir de la carpeta de datasets
datasets_dir = os.environ.get("DATASETS_DIR", "/opt/dagster/app/datasets")
dynamic_assets = build_all_dataset_assets(datasets_dir)

# 3. Define los recursos requeridos por el Data Control Plane
control_plane_resources = {
    "kafka_admin": KafkaAdminResource(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    ),
    "clickhouse": ClickHouseResource(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    ),
    "debezium": DebeziumResource(
        connect_url=os.environ["KAFKA_CONNECT_URL"],
    ),
    "mongodb": MongoDBResource(
        uri=os.environ["MONGODB_URI"],
        default_database=os.environ.get("MONGODB_DATABASE", "app_db"),
    ),
}

# 4. Tus assets, recursos y sensores preexistentes en la empresa
existing_assets = [...]
existing_resources = {...}
existing_sensors = [...]

# 5. Fusiónalo todo en un único objeto Definitions
defs = dg.Definitions(
    assets=[*existing_assets, *dynamic_assets],
    resources={**existing_resources, **control_plane_resources},
    sensors=[*existing_sensors, dataset_yaml_sensor],
)
```

### Paso 3: Configurar las Variables de Entorno en tu Helm Chart Existente
Ve al archivo de configuración Helm `values.yaml` de la empresa (o tus Kubernetes Secrets/ConfigMaps) y asegúrate de inyectar las siguientes variables a tu deployment de **User Code** actual:
```yaml
# En tu values.yaml de Dagster existente (sección de env o userDeployments):
env:
  - name: MONGODB_URI
    valueFrom:
      secretKeyRef:
        name: mongodb-credentials # Usa tus secretos de K8s existentes
        key: connection-string
  - name: KAFKA_BOOTSTRAP_SERVERS
    value: "cluster-kafka-bootstrap.kafka.svc:9092"
  - name: KAFKA_CONNECT_URL
    value: "http://cluster-kafka-connect.kafka.svc:8083"
  - name: CLICKHOUSE_HOST
    value: "clickhouse-service.clickhouse.svc"
  - name: CLICKHOUSE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: clickhouse-credentials
        key: password
  - name: DATASETS_DIR
    value: "/opt/dagster/app/datasets"
```

### Paso 4: Flujo de GitOps para Onboarding de Nuevas Tablas de Mongo
Dado que el clúster de EKS ejecuta contenedores inmutables en pods, la mejor práctica en producción es usar **GitOps**:
1. El equipo de datos crea o modifica un archivo YAML de dataset (ej: `datasets/new_table.yaml`) en tu repositorio de Git.
2. Al hacer `push` o `merge` a `main`, tu pipeline de CI/CD (GitHub Actions, GitLab CI, Jenkins, etc.) compila la nueva imagen Docker de tu Dagster User Code que ya incluye la carpeta `datasets/` dentro de `/opt/dagster/app/datasets/`.
3. Tu pipeline de CI/CD actualiza el tag de la imagen del pod de User Code en Kubernetes (vía Helm o ArgoCD).
4. Al redesplegarse el pod de forma automática:
   * El Daemon y Webserver de Dagster leen el nuevo esquema del pod.
   * Aparecen instantáneamente en la interfaz de Dagster los **Assets** correspondientes a `new_table`.
   * El `dataset_yaml_sensor` detecta la nueva configuración y lanza inmediatamente el pipeline de Onboarding (creando el tópico de Kafka, el conector CDC de Debezium, las tablas y MV de Clickhouse, y cargando el histórico).
   * ¡A partir de ese momento, la sincronización fluye de manera automática sin tocar una sola línea de código Python!

---

## 🔁 Transición: Reemplazar el CLI de Go por el Data Control Plane en Dagster

Esta es la sección más relevante para tu trabajo de las próximas semanas. Tu empresa usa actualmente una herramienta CLI escrita en Go para hacer el onboarding de tablas de MongoDB (crear topic Kafka, registrar conector Debezium, crear tablas ClickHouse y cargar el histórico). El objetivo es **reemplazar esa herramienta con este Data Control Plane en tu Dagster corporativo existente**, que ya tiene pipelines funcionando y está conectado a MongoDB Atlas.

La buena noticia: **el 90% del trabajo es copiar ficheros y cambiar nombres de variables**. El código Python ya está probado y funcionando. No tienes que reescribir la lógica.

---

### ¿Qué cambia exactamente? CLI de Go vs. Dagster Control Plane

El CLI de Go es **imperativo y manual**: alguien ejecuta un comando, ocurren las cosas, y si algo falla hay que relanzarlo a mano y recordar en qué paso quedó.

El Data Control Plane en Dagster es **declarativo y auto-reparable**: defines en un YAML *qué* quieres tener, y Dagster sabe *cómo* conseguirlo, en qué orden, y si algo falla puede relanzarse solo o con un clic.

| Tarea de onboarding | CLI de Go (actual) | Dagster Control Plane (nuevo) |
|---|---|---|
| Crear topic Kafka | `go-cli create-topic <name>` | Asset `kafka_topic_<dataset>` |
| Registrar conector Debezium | `go-cli register-connector <json>` | Asset `debezium_connector_<dataset>` |
| Crear tablas ClickHouse | `go-cli create-tables <schema>` | Asset `clickhouse_schema_<dataset>` |
| Cargar histórico | `go-cli load-history --mongo ... --ch ...` | Asset `historical_load_<dataset>` |
| Definir qué tabla añadir | Parámetros del comando en consola | Un fichero `datasets/mi_tabla.yaml` |
| Visibilidad del estado | Logs en terminal | Asset Graph visual en Dagster UI |
| Re-ejecutar si falla | Ejecutar comando de nuevo manualmente | Clic en "Re-materialize" en Dagster UI |
| Auditoría y trazabilidad | Ninguna por defecto | Historial de runs en Dagster automático |

---

### Paso A: Copiar los módulos del Control Plane a tu repositorio corporativo

Tu Dagster corporativo tendrá un repositorio Git con una estructura similar a esta:

```
tu-repo-corporativo/
├── my_dagster_project/       ← Tu paquete Python actual (el nombre puede variar)
│   ├── assets/
│   │   └── mis_assets_existentes.py
│   ├── resources/
│   │   └── mis_recursos_existentes.py
│   └── definitions.py        ← El fichero donde está tu objeto dg.Definitions(...)
├── pyproject.toml
└── Dockerfile
```

Tienes que añadir los siguientes ficheros de este proyecto. **No toques nada de lo que ya existe.**

```
tu-repo-corporativo/
├── datasets/                              ← NUEVO: carpeta para tus YAMLs de tablas
│   └── mi_primera_tabla.yaml             ← Empieza con UNA tabla de prueba
└── my_dagster_project/
    ├── assets/
    │   ├── mis_assets_existentes.py
    │   └── dataset_factory.py             ← NUEVO: copiar de dagster_project/assets/
    ├── models/
    │   └── dataset_config.py             ← NUEVO: copiar de dagster_project/models/
    ├── resources/
    │   ├── mis_recursos_existentes.py
    │   ├── clickhouse_resource.py         ← NUEVO: copiar de dagster_project/resources/
    │   ├── debezium_resource.py           ← NUEVO: copiar de dagster_project/resources/
    │   ├── kafka_resource.py              ← NUEVO: copiar de dagster_project/resources/
    │   └── mongodb_resource.py            ← NUEVO: solo si no tienes uno ya (ver Paso B)
    ├── sensors/
    │   └── yaml_sensor.py                ← NUEVO: copiar de dagster_project/sensors/
    └── utils/
        ├── clickhouse_ddl.py             ← NUEVO: copiar de dagster_project/utils/
        └── debezium_config.py            ← NUEVO: copiar de dagster_project/utils/
```

> **Nota sobre las importaciones**: Los ficheros copiados importan desde `dagster_project.*`. Tienes que hacer un reemplazo global en todos los ficheros copiados: cambia `dagster_project` por el nombre real de tu paquete Python corporativo (ej: `my_dagster_project`). En un editor moderno es `Ctrl+Shift+H` → reemplazar en todos los archivos del proyecto.

---

### Paso B: Reutilizar tu recurso MongoDB existente (si ya tienes uno)

Si tu Dagster corporativo ya tiene un recurso de MongoDB configurado (lo más probable, dado que ya usáis MongoDB), puedes evitar el conflicto de tener dos recursos MongoDB. Tienes dos opciones:

**Opción 1 (Recomendada): Renombrar el recurso nuevo**

En `definitions.py`, registra nuestro `MongoDBResource` con un nombre diferente para que no colisione con el tuyo:

```python
# En tu definitions.py corporativo
from my_dagster_project.resources.mongodb_resource import MongoDBResource as CDPMongoResource

control_plane_resources = {
    # Este nombre "mongodb" es el que usa dataset_factory.py internamente
    "mongodb": CDPMongoResource(
        uri=os.environ["MONGODB_URI"],           # mongodb+srv://... de Atlas
        default_database=os.environ.get("MONGODB_DATABASE", "mi_base_de_datos"),
    ),
    # ...resto de recursos del control plane
}
```

Lo importante es que en el diccionario de recursos uses la clave `"mongodb"`, que es la que buscan los assets del control plane con `context.resources.mongodb`.

**Opción 2: Adaptar `dataset_factory.py` a tu recurso existente**

Si prefieres usar tu recurso MongoDB existente (ej: si se llama `"mi_mongo"`), abre `dataset_factory.py` y busca las líneas donde se usa `context.resources.mongodb`. Cámbialas por el nombre de tu recurso:

```python
# En dataset_factory.py, busca esto:
mongo: MongoDBResource = context.resources.mongodb

# Y cámbialo por el nombre de tu recurso existente:
mongo: TuMongoResource = context.resources.mi_mongo
```

---

### Paso C: Fusionar en tu `definitions.py` sin romper lo existente

Este es el paso más delicado. Debes **añadir** los nuevos assets, recursos y sensores a tu `Definitions` existente sin eliminar lo que ya tienes.

```python
# tu-repo-corporativo/my_dagster_project/definitions.py

import os
import dagster as dg

# ── Tus imports existentes (NO los toques) ──
from my_dagster_project.assets.mis_assets_existentes import mis_assets
from my_dagster_project.resources.mis_recursos_existentes import mi_recurso_existente

# ── Nuevos imports del Data Control Plane ──
from my_dagster_project.assets.dataset_factory import build_all_dataset_assets
from my_dagster_project.resources.clickhouse_resource import ClickHouseResource
from my_dagster_project.resources.debezium_resource import DebeziumResource
from my_dagster_project.resources.kafka_resource import KafkaAdminResource
from my_dagster_project.resources.mongodb_resource import MongoDBResource
from my_dagster_project.sensors.yaml_sensor import dataset_yaml_sensor

# ── Generar assets del Control Plane desde los YAMLs ──
datasets_dir = os.environ.get("DATASETS_DIR", "/opt/dagster/app/datasets")
cdp_assets = build_all_dataset_assets(datasets_dir)

# ── Fusionar TODO en un único Definitions ──
defs = dg.Definitions(
    assets=[
        mis_assets,          # Tus assets existentes
        *cdp_assets,         # Assets del Control Plane (generados dinámicamente)
    ],
    resources={
        # Tus recursos existentes
        "mi_recurso_existente": mi_recurso_existente,

        # Recursos del Control Plane (se añaden, no reemplazan)
        "kafka_admin": KafkaAdminResource(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        ),
        "clickhouse": ClickHouseResource(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        ),
        "debezium": DebeziumResource(
            connect_url=os.environ["KAFKA_CONNECT_URL"],
        ),
        "mongodb": MongoDBResource(
            uri=os.environ["MONGODB_URI"],
            default_database=os.environ.get("MONGODB_DATABASE", "mi_bd"),
        ),
    },
    sensors=[
        # Tus sensores existentes (si los tienes)
        dataset_yaml_sensor,  # Sensor del Control Plane
    ],
)
```

---

### Paso D: Variables de entorno que necesitas añadir a tu deployment

Tu Dagster corporativo ya tendrá `MONGODB_URI` configurada. Solo necesitas añadir las variables nuevas que el Control Plane requiere. Probablemente los valores ya los conoce tu equipo de infraestructura:

```bash
# Variables YA existentes en tu Dagster (no las toques):
MONGODB_URI="mongodb+srv://usuario:pass@cluster.mongodb.net/..."

# Variables NUEVAS que hay que añadir al pod de User Code:
KAFKA_BOOTSTRAP_SERVERS="nombre-de-tu-kafka-bootstrap.namespace.svc:9092"
KAFKA_CONNECT_URL="http://nombre-de-tu-kafka-connect.namespace.svc:8083"
CLICKHOUSE_HOST="nombre-de-tu-clickhouse.namespace.svc"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD="tu-password-de-clickhouse"
MONGODB_DATABASE="nombre_de_tu_base_de_datos_en_atlas"
DATASETS_DIR="/opt/dagster/app/datasets"
```

Para encontrar los valores correctos de Kafka y ClickHouse, pregunta a tu equipo de infraestructura o ejecuta esto en tu clúster EKS:

```bash
# Ver todos los servicios de Kubernetes para encontrar los nombres:
kubectl get svc -A | grep -E "kafka|clickhouse|connect"
```

---

### Paso E: Instalar las dependencias Python nuevas

Añade estas librerías al `requirements.txt` o `pyproject.toml` de tu proyecto Dagster corporativo. Muchas puede que ya las tengas:

```txt
# Nuevas dependencias del Control Plane:
kafka-python>=2.0.0       # Para crear topics Kafka (KafkaAdminResource)
clickhouse-connect>=0.7   # Para conectar a ClickHouse (ClickHouseResource)
pymongo>=4.0              # Para leer de MongoDB Atlas (MongoDBResource)
requests>=2.28            # Para llamar a la API REST de Debezium
pyyaml>=6.0               # Para leer los ficheros YAML de datasets
pydantic>=2.0             # Para validar los modelos de dataset
```

---

### Paso F: Escribir tu primer YAML de prueba

Antes de migrar todas las tablas de MongoDB, prueba con una tabla de baja criticidad. Crea `datasets/mi_primera_tabla.yaml`:

```yaml
dataset:
  name: mi_primera_tabla

  source:
    mongodb:
      database: nombre_de_tu_base_de_datos     # El mismo que MONGODB_DATABASE
      collection: nombre_de_la_coleccion       # La colección MongoDB exacta

  kafka:
    topic_name: "cdc.nombre_de_tu_base_de_datos.nombre_de_la_coleccion"
    partitions: 3
    replication_factor: 3   # En producción usa 3, no 1

  clickhouse:
    database: analytics      # La base de datos destino en ClickHouse
    table_name: mi_primera_tabla
    engine: ReplacingMergeTree
    order_by:
      - _id
    version_column: _version
    columns:
      - name: _id
        type: String
      # Añade aquí los campos de tu colección MongoDB:
      - name: campo_1
        type: String
      - name: campo_2
        type: UInt64
        default: "0"
      - name: fecha_creacion
        type: DateTime64(3)
      # Campos obligatorios de CDC (siempre los últimos):
      - name: _version
        type: UInt64
        default: "0"
      - name: _deleted
        type: UInt8
        default: "0"
```

> **Tip para mapear tipos**: Los tipos de MongoDB se mapean a ClickHouse así:
> - `String` / `ObjectId` → `String`
> - `Number (entero)` → `Int32`, `Int64`, o `UInt64`
> - `Number (decimal)` → `Float64`
> - `Boolean` → `UInt8` (0=false, 1=true)
> - `Date` / `ISODate` → `DateTime64(3)`
> - `Array` / `Object` anidado → `String` (guardar como JSON serializado)

---

### Paso G: ¿Qué pasa con las tablas que el Go CLI ya onboardó?

Esta es una pregunta clave. Si creas un YAML para una tabla que el Go CLI ya configuró previamente, **no hay problema**: el Control Plane es completamente idempotente.

- `kafka_topic_*`: comprueba si el topic ya existe antes de crearlo. Si existe, no hace nada.
- `debezium_connector_*`: hace `GET /connectors/{name}` antes de `POST`. Si el conector ya existe y está en `RUNNING`, no lo toca.
- `clickhouse_schema_*`: usa `CREATE TABLE IF NOT EXISTS` y `CREATE VIEW IF NOT EXISTS`. Si las tablas ya existen, no falla.
- `historical_load_*`: inserta registros con `INSERT INTO`. Como la tabla usa `ReplacingMergeTree`, si ya hay datos, simplemente se añaden versiones más recientes. ClickHouse deduplica automáticamente con `SELECT ... FINAL`.

**Esto significa que puedes migrar tablas del Go CLI a Dagster de una en una, sin downtime y sin riesgo.**

---

### Plan de transición semana a semana

Aquí tienes una hoja de ruta concreta para las próximas semanas:

```
SEMANA 1: Preparar el entorno
  ✓ Copiar ficheros al repositorio corporativo
  ✓ Cambiar importaciones (dagster_project → my_dagster_project)
  ✓ Fusionar definitions.py
  ✓ Añadir dependencias Python
  ✓ Conseguir las variables de entorno de infra (Kafka, ClickHouse)
  ✓ Desplegar en staging y verificar que Dagster arranca sin errores

SEMANA 2: Primera tabla piloto
  ✓ Elegir una colección MongoDB de baja criticidad
  ✓ Escribir su YAML en datasets/
  ✓ Ejecutar "Materialize All" en Dagster UI (staging)
  ✓ Verificar datos en ClickHouse
  ✓ Hacer un UPDATE en MongoDB y confirmar que llega a ClickHouse

SEMANA 3+: Migración progresiva
  ✓ Por cada tabla que el Go CLI ya gestiona:
      → Crear su YAML en datasets/
      → Materializar en Dagster (es idempotente, no rompe nada)
      → Verificar en Dagster UI que el asset aparece como "Materialized"
  ✓ Dejar de ejecutar el Go CLI para esas tablas
  ✓ Para tablas nuevas: solo crear el YAML (nunca más usar el Go CLI)

SEMANA FINAL: Retirar el Go CLI
  ✓ Todas las tablas gestionadas vía YAML + Dagster
  ✓ Documentar internamente el nuevo flujo para el equipo
  ✓ El Go CLI queda deprecado
```

---

## ☁️ Especial: Integración Completa con MongoDB Atlas (Entorno Enterprise)

Sincronizar datos en tiempo real desde **MongoDB Atlas** (el SaaS en la nube de MongoDB) a tu clúster existente de **EKS** e infraestructura analítica requiere resolver tres retos fundamentales: **seguridad de red**, **permisos específicos de Atlas** y la **sintaxis de conexión SRV** en Kafka Connect / Debezium.

A continuación, tienes la guía de ingeniería definitiva para adaptar este Data Control Plane a tu entorno corporativo con Atlas:

```
+-----------------------------------+               +--------------------------------------+
|       AWS EKS CLUSTER (COMPANY)   |               |         MONGODB ATLAS (CLOUD)        |
|                                   |  VPC Peering  |                                      |
|  [Dagster Pods] [Kafka Connect]  | <=============> |  [Primary] [Secondary] [Secondary]  |
|                                   |  PrivateLink  |  (Replica Set autogestionado)        |
+-----------------------------------+               +--------------------------------------+
```

### 1. Conectividad y Seguridad de Red (EKS ⇆ MongoDB Atlas)
Por defecto, MongoDB Atlas tiene un cortafuegos estricto que bloquea todo tráfico entrante. Para que tus pods de **Dagster** (para la carga del histórico) y **Kafka Connect** (para Debezium CDC) puedan conectarse de forma privada y segura, debes implementar una de las siguientes opciones:

*   **Opción A: AWS VPC Peering (La más común)**:
    1. Ve a la consola de **MongoDB Atlas**, selecciona tu proyecto, y en el menú lateral navega a **Network Peering**.
    2. Haz clic en **Add Peering Connection**, selecciona **Amazon Web Services**, e introduce tu **AWS Account ID**, el **VPC ID** de tu clúster de EKS, y la **Región** del VPC.
    3. Ve a tu consola de **AWS VPC**, acepta la solicitud de emparejamiento (Peering Connection) entrante.
    4. **Muy Importante**: Actualiza las **Tablas de Enrutamiento (Route Tables)** de tus subredes privadas de EKS en AWS para dirigir el tráfico del bloque CIDR de Atlas a través de la conexión de Peering.
    5. En Atlas, añade el bloque CIDR del VPC de tu EKS en la pestaña **Network Access (IP Access List)**.
*   **Opción B: AWS PrivateLink / Endpoint Services (La más robusta para grandes empresas)**:
    *   Atlas permite crear un **Private Endpoint** integrado directamente con AWS. Esto expone un host DNS privado dentro del VPC de tu EKS para conectarse a Atlas de forma 100% privada sin emparejar redes directamente ni exponer IPs.
*   **Opción C: Whitelisting de NAT Gateways (La más sencilla sin cambios en VPC)**:
    *   Si tu clúster de EKS sale a Internet a través de un pool de **NAT Gateways** de AWS con IPs elásticas estáticas (Elastic IPs), simplemente añade esas IPs públicas en la sección de **IP Access List** en MongoDB Atlas.

---

### 2. Permisos y Roles de Usuario en MongoDB Atlas para Debezium CDC
Debezium CDC lee los cambios directamente desde el **Oplog** y Change Streams de Atlas. Crear un usuario de base de datos con permisos excesivos (`Atlas Admin`) es un grave riesgo de seguridad. Debes crear un usuario exclusivo para la replicación analítica con los siguientes privilegios estrictos:

1. Ve a **Database Access** en la consola de Atlas y haz clic en **Add New Database User**.
2. Define un método de autenticación por contraseña segura (la inyectarás vía Secrets a Dagster).
3. En la sección **Built-in Roles**, selecciona:
   * **`readAnyDatabase`**: Permite a Debezium y a Dagster realizar el snapshot inicial (lectura de datos) y mapear metadatos de cualquier base de datos configurada en tu YAML.
4. Dado que Debezium lee Change Streams globales y lee colecciones del sistema como `local.oplog.rs` y tokens de reanudación, debes asignarle privilegios adicionales. En Atlas, despliega **Specific Privileges (Custom Roles)** y asógnale un rol personalizado con los siguientes permisos en formato JSON:
   ```json
   {
     "actions": [
       { "action": "find", "resource": { "db": "local", "collection": "oplog.rs" } },
       { "action": "find", "resource": { "db": "config", "collection": "system.sessions" } },
       { "action": "changeStream", "resource": { "db": "", "collection": "" } }
     ],
     "roles": []
   }
   ```
   *Nota: Si estás usando una instancia Atlas dedicada (M10 o superior), Atlas habilita Change Streams nativos a nivel de clúster, facilitando enormemente esta tarea.*

---

### 3. Cadena de Conexión SRV de Atlas en Kubernetes y Debezium
MongoDB Atlas utiliza el formato de cadena de conexión `mongodb+srv://` para balancear el tráfico automáticamente entre los miembros de la réplica (Primary y Secondaries) sin acoplarse a direcciones IP físicas o nombres de host cambiantes.

#### Configuración de la Variable `MONGODB_URI` en Dagster
En tu archivo de Secrets de Kubernetes o en tu `values-eks.yaml`, configura tu URI con el siguiente formato:
```env
MONGODB_URI="mongodb+srv://user_debezium:MiContrasenaSegura123@prod-cluster.xxxx.mongodb.net/app_db?retryWrites=true&w=majority"
```
*   **¿Cómo lo procesa Dagster?**: Nuestro recurso `MongoDBResource` (basado en `pymongo`) acepta este formato SRV de manera nativa sin realizar ningún cambio en el código Python de tu empresa. Realizará las queries de lectura del histórico de forma rápida.

#### Configuración de Debezium Connect para MongoDB Atlas
Debezium soporta el formato de conexión SRV nativo. Cuando Dagster registra el conector en Kafka Connect (a través de la API REST que orquesta nuestro asset `debezium_connector`), envía la configuración generada en `dagster_project/utils/debezium_config.py`.

Asegúrate de que la variable de entorno `MONGODB_URI` que le pasas a Dagster tenga el formato SRV de Atlas. Nuestro generador dinámico de configuración de Debezium mapea este parámetro directamente al campo `"mongodb.connection.string"`, por lo que **no tienes que cambiar el código de Debezium tampoco; es totalmente compatible por defecto**.

---

### 4. Ajustes Finos de Configuración en el Entorno Corporativo
Cuando trabajas con Atlas e inmensos volúmenes de datos, te conviene ajustar estos parámetros menores para optimizar la velocidad y evitar cuellos de botella:

*   **Tamaño del Oplog en Atlas**:
    *   En MongoDB Atlas, el Oplog tiene un almacenamiento rotativo. Si tienes una colección muy grande y el snapshot inicial de onboarding tarda varias horas, es posible que el Oplog rote antes de que termine el snapshot, provocando que Debezium pierda la posición de streaming inicial y falle (`OplogReaderhas fallen behind`).
    *   **Recomendación**: En Atlas, ve a las opciones de tu clúster y asegúrate de configurar un tamaño de Oplog generoso (ej: mínimo 20-50 GB o 24 horas de retención) para el periodo en que vayas a realizar el onboarding de datasets grandes.
*   **Lectura desde Secundarios en ClickHouse Onboarding (Carga Histórica)**:
    *   Para evitar degradar el rendimiento de tu base de datos de producción (el miembro Primary de Atlas) durante la extracción masiva del histórico que realiza Dagster, puedes modificar tu URI de MongoDB en el YAML o variable de entorno para forzar lecturas desde miembros secundarios:
    ```env
    MONGODB_URI="mongodb+srv://.../app_db?retryWrites=true&w=majority&readPreference=secondaryPreferred"
    ```
    *   Dagster leerá los cientos de miles de registros iniciales desde las réplicas secundarias de Atlas sin afectar en absoluto los tiempos de respuesta de tu aplicación principal.

---
¡Mucha suerte con el despliegue del Data Control Plane en tu infraestructura! Si tienes dudas con el código o las APIs de Kubernetes, este manual te servirá como mapa de referencia permanente. 🚀


