# Pipeline ETL — Rinde & Clima (AWS)

Pipeline de ingesta y transformación de datos agronómicos usando arquitectura medallion (Bronze → Silver → Gold) sobre AWS, orquestado con Step Functions y procesado con Glue Jobs.

## Arquitectura

```
EventBridge (schedule / manual)
         │
         ▼
   ┌─────────────┐
   │ Step Function│
   └──────┬──────┘
          │
          ├─── Parallel ──────────────────┐
          │                               │
   ┌──────▼──────┐                ┌───────▼──────┐
   │ Glue Job 1  │                │ Glue Job 1   │
   │ Ingest Rinde│                │ Ingest Clima │
   │ CSV → Bronze│                │ CSV → Bronze │
   └──────┬──────┘                └───────┬──────┘
          │                               │
          ├─── Parallel ──────────────────┐
          │                               │
   ┌──────▼──────┐                ┌───────▼──────┐
   │ Glue Job 2  │                │ Glue Job 2   │
   │ Clean Rinde │                │ Clean Clima  │
   │ Bronze→Silv.│                │ Bronze→Silv. │
   └──────┬──────┘                └───────┬──────┘
          │                               │
          └───────────┬───────────────────┘
                      │
               ┌──────▼──────┐
               │ Glue Job 3  │
               │ Join + Gold │
               └──────┬──────┘
                      │
               ┌──────▼──────┐
               │   Athena    │
               │  (BI/SQL)   │
               └─────────────┘
```

## Capas de datos (Medallion)

| Capa | Ubicación S3 | Formato | Contenido |
|------|-------------|---------|-----------|
| **Raw** | `s3://bucket/raw/` | CSV | Archivos originales sin modificar |
| **Bronze** | `s3://bucket/bronze/` | Parquet | Datos crudos convertidos, particionados por campaña |
| **Silver** | `s3://bucket/silver/` | Parquet | Datos limpios, validados, sin duplicados. Particionados por campaña + lote |
| **Gold** | `s3://bucket/gold/` | Parquet | Join rinde+clima con métricas calculadas. Consumo BI |

## Estructura del proyecto

```
├── contracts/          Data contracts (YAML) — schema + reglas DQ declarativas
├── data/               CSVs de prueba con datos sucios para testing
├── src/                Lógica pura de validación y calidad (testeable sin AWS)
├── glue_jobs/          Scripts PySpark que corren en AWS Glue
├── test/               Tests unitarios (pytest)
├── infra/              Step Function (ASL) + IAM policies
├── requirements.txt
└── README.md
```

## Data Contracts

Los contratos en `contracts/` definen de forma declarativa el schema esperado, tipos, rangos válidos, % de nulos aceptable y claves únicas para cada dataset. La validación en `src/validate.py` y `src/quality.py` consume estos contratos, desacoplando las reglas de negocio del código.

```yaml
# Ejemplo: contracts/rinde_lotes.yaml
schema:
  - name: rinde_kg_ha
    type: numeric
    min: 0
    max: 25000
quality:
  max_null_pct: 5
  unique_keys: ["campaña", "lote", "fecha_cosecha"]
```

## Idempotencia & Backfill

- Todos los Glue Jobs usan `mode("overwrite")` con `partitionOverwriteMode: dynamic`, garantizando que re-ejecutar con los mismos parámetros produce el mismo resultado sin duplicados.
- La Step Function recibe `bucket` y `campana` como input. Para backfill, se ejecuta con la campaña histórica deseada:
  ```json
  { "bucket": "mi-bucket", "campana": "2023" }
  ```
- No hay estado mutable externo ni dependencia de timestamps de ejecución.

## Seguridad (IAM)

Principio de **least privilege** — cada componente tiene su propio IAM Role con permisos mínimos:

| Componente | Permisos S3 | Otros |
|-----------|------------|-------|
| Step Function | Ninguno | `glue:StartJobRun` solo sobre los 3 jobs |
| Glue Job 1 (ingest) | `GetObject` en raw/, `PutObject` en bronze/ | CloudWatch logs |
| Glue Job 2 (clean) | `GetObject` en bronze/, `PutObject` en silver/ | CloudWatch logs |
| Glue Job 3 (join) | `GetObject` en silver/, `PutObject` en gold/ | Glue Catalog read/write |

Configuraciones adicionales:
- S3: block public access, versionado, encriptación SSE-S3
- CloudTrail habilitado para auditoría de ejecuciones
- Sin wildcards (`*`) en resources de IAM policies

## Costos

### Estimación por ejecución diaria (~100 MB de CSVs)

| Servicio | Detalle | Costo estimado |
|---------|---------|---------------|
| **Glue Jobs** | 3 jobs × ~2 DPU × 3 min, Flex pricing ($0.29/DPU-hr) | ~$0.09 |
| **Step Functions** | ~10 transiciones Standard ($0.025/1000) | ~$0.00025 |
| **S3 Storage** | ~500 MB total (todas las capas) | ~$0.012/mes |
| **Athena** | Queries sobre gold/ particionado (~10 MB scan) | ~$0.00005/query |
| **Total diario** | | **~$0.09** |
| **Total mensual** | | **~$2.70** |

### Optimizaciones aplicadas

- **Glue Flex Jobs**: hasta 34% más barato usando capacidad spot
- **Glue Auto Scaling**: DPUs se ajustan al volumen, no se sobre-provisiona
- **Parquet columnar**: reduce scan de Athena ~90% vs CSV
- **Particionamiento**: Athena solo lee particiones relevantes
- **S3 Lifecycle**: raw/ → IA a 30 días, Glacier a 90 días; bronze/ → IA a 60 días
- **Procesamiento incremental**: cada ejecución solo procesa la campaña indicada

### Escalabilidad

Para volúmenes de TB, la arquitectura escala sin cambios: se incrementan DPUs en Glue (Auto Scaling) y se mantiene la misma Step Function. Costo estimado para 10 GB/día: ~$0.72/ejecución.

## Testing

```bash
python -m pytest test/ -v
```

Los tests validan `src/validate.py` y `src/quality.py` contra los CSVs de prueba en `data/`, que incluyen intencionalmente: nulos, fechas inválidas, valores fuera de rango y filas duplicadas.

## Ejecución de la Step Function

Input de ejemplo para ejecución normal:
```json
{
  "bucket": "mi-bucket-datos",
  "campana": "2024"
}
```

Input para backfill de campaña histórica:
```json
{
  "bucket": "mi-bucket-datos",
  "campana": "2023"
}
```

## Stack tecnológico

| Tecnología | Uso |
|-----------|-----|
| AWS Step Functions | Orquestación del pipeline |
| AWS Glue (PySpark) | Procesamiento ETL |
| Amazon S3 | Almacenamiento por capas |
| AWS Glue Data Catalog | Metastore para Athena |
| Amazon Athena | Consultas SQL para BI |
| Great Expectations | Framework de data quality |
| pytest | Testing unitario |
