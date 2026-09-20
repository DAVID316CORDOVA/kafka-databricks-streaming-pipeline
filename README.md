# Kafka Streaming Pipeline → Databricks (Bronze / Silver / Gold)

Pipeline de streaming en tiempo real: trades de Binance (BTCUSDT, ETHUSDT) vía
WebSocket → Apache Kafka (auto-hospedado en EC2) → Databricks (Structured
Streaming + Delta Live Tables), con arquitectura medallón completa y CI/CD
para dev y producción.

## Arquitectura

```
Binance (WebSocket)
       │
       ▼
Productor local (binance_to_kafka.py)
       │
       ▼
┌─────────────────────────────────────┐
│ EC2 (AWS)                            │
│  Broker Kafka (modo KRaft)           │
│  topic: trades-stream, 3 particiones │
└─────────────────────────────────────┘
       │ readStream
       ▼
┌───────────────────────────────────────────────────┐
│ Databricks (Unity Catalog)                         │
│                                                     │
│  Bronze ──▶ Silver ──▶ Gold                        │
│  (job         (DLT:        (DLT:                   │
│  Structured   watermarking  agregacion OHLC         │
│  Streaming    + dedup +     de 1 minuto,            │
│  clasico)     data quality) por simbolo)            │
└───────────────────────────────────────────────────┘
       ▲
       │ validate + deploy
GitHub Actions (CI/CD) — Databricks Asset Bundles
```

Cada capa vive en su propio schema de Unity Catalog, distinto en `dev` y
`prod` (ver [Configuración por ambiente](#configuración-por-ambiente)).

## Por qué esta arquitectura

- **Kafka auto-hospedado en EC2, no un servicio gestionado**: para esta
  práctica se priorizó entender el mecanismo interno de Kafka (particiones,
  réplicas, offsets, brokers) en vez de delegarlo a un servicio como MSK o
  Event Hub desde el principio. En un entorno de producción real, un
  servicio gestionado sería la opción por defecto.
- **Cluster clásico para Bronze, no serverless**: el conector nativo de
  Kafka para Structured Streaming viene incluido en el Databricks Runtime
  clásico, permitiendo `readStream`/`writeStream` reales con watermarking y
  checkpointing genuinos — a diferencia de un proyecto anterior con Event
  Hub sobre serverless, donde ese conector no estaba disponible y se recurrió
  a un loop manual en Python.
- **Silver y Gold como pipelines DLT (Delta Live Tables), separados del job
  de Bronze**: DLT gestiona automáticamente checkpoints, reintentos y
  linaje entre tablas — más apropiado para transformaciones declarativas
  que un `readStream`/`writeStream` manual adicional.
- **Silver y Gold en pipelines separados, no uno combinado**: cada uno con
  su propio schema (`silver`, `gold`), para que el linaje y los permisos
  de cada capa queden claramente delimitados.

## Conceptos clave demostrados

| Concepto | Dónde se aplica |
|---|---|
| Particiones y paralelismo | Topic `trades-stream`, particionado por `symbol` |
| Checkpointing | `checkpointLocation` explícito en Bronze; automático por tabla en DLT (Silver/Gold) |
| Watermarking | `withWatermark("trade_timestamp", "2 minutes")` en Silver y Gold |
| Deduplicación streaming | `dropDuplicatesWithinWatermark` en Silver |
| Data quality declarativa | `@dlt.expect_or_drop` / `@dlt.expect_or_fail` en Silver |
| Agregación por ventanas | OHLC de 1 minuto por símbolo en Gold |
| CI/CD por ambiente | GitHub Actions + Databricks Asset Bundles, `dev` (push automático) y `prod` (solo en `main`) |

## Estructura del repositorio

```
.
├── databricks.yml                 # Bundle: variables y targets (dev/prod)
├── docker-compose-ec2.yml         # Kafka (KRaft) + Kafka UI, desplegado al EC2
├── setup_kafka.sh                 # Script de arranque local de Kafka + creación del topic
├── binance_to_kafka.py            # Productor: WebSocket Binance → Kafka
├── requirements.txt
├── notebooks/
│   ├── bronze_streaming_kafka_consumer.py   # Job Structured Streaming (Bronze)
│   ├── silver_trades_dlt.py                 # Pipeline DLT (Silver)
│   ├── gold_trades_dlt.py                   # Pipeline DLT (Gold)
│   └── demo_watermarking.py                 # Notebook de práctica, aislado de las tablas reales
├── resources/
│   ├── streaming.yml              # Job de Bronze
│   ├── dlt_pipeline_silver.yml    # Pipeline DLT de Silver
│   └── dlt_pipeline_gold.yml      # Pipeline DLT de Gold
├── terraform/                     # IaC documental -- ver nota abajo
└── .github/workflows/
    ├── ci-cd-dev.yml               # Valida y despliega a dev en cada push
    └── ci-cd-prod.yml              # Valida y despliega a prod solo en push a main
```

## Configuración por ambiente

| Variable | dev | prod |
|---|---|---|
| `bronze_schema` | `kafka_bronze` | `bronze` |
| `silver_schema` | `dev_silver` | `silver` |
| `gold_schema` | `dev_gold` | `gold` |
| `starting_offsets` | `earliest` (reprocesa el historial ya generado) | `latest` (evita reprocesar el backlog completo) |

## Cómo correrlo

```bash
export BUNDLE_VAR_kafka_bootstrap_servers="<ip-del-ec2>:9092"
export BUNDLE_VAR_existing_cluster_id="<cluster-id>"

databricks bundle deploy -t dev
databricks bundle run trades_streaming_job -t dev   # Bronze (job)
databricks bundle run trades_silver_pipeline -t dev  # Silver (DLT)
databricks bundle run trades_gold_pipeline -t dev    # Gold (DLT)
```

Los pipelines DLT usan `continuous: false` (modo "triggered"): corren una
vez y se detienen solos, sin quedar consumiendo cómputo de fondo.

## Infraestructura como código (Terraform)

**Nota de transparencia**: la infraestructura de este proyecto (EC2, Security
Group, Docker) se montó manualmente durante la práctica, no mediante
Terraform. El código en `terraform/` documenta cómo se automatizaría ese
mismo despliegue en un flujo de trabajo real — no fue ejecutado como parte
de este proyecto.

## Pendientes conocidos

- Restringir el Security Group del EC2 (puertos 9092/8090) a la IP de salida
  real de Databricks, en vez de `0.0.0.0/0`.
- Considerar replication factor > 1 si el proyecto migra a múltiples brokers.