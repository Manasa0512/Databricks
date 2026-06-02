# Databricks notebook source
# DBTITLE 1,Bronze Layer Overview
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC **Purpose**: Ingest all raw genomic datasets into Bronze Delta tables
# MAGIC
# MAGIC **Datasets**:
# MAGIC 1. VCF Variants (6.4M records)
# MAGIC 2. Gene Annotations GTF (5.8M records)
# MAGIC 3. ClinVar Clinical Variants (9M records)
# MAGIC
# MAGIC **Strategy**: Read as-is, add audit metadata, partition by ingestion_date
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Pipeline Pseudocode
# MAGIC %md
# MAGIC # Bronze Layer Pseudocode
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   START Bronze Layer Ingestion
# MAGIC   
# MAGIC   STEP 1: Configure paths and table names
# MAGIC   {
# MAGIC     SET file_paths = [VCF, GTF, ClinVar]
# MAGIC     SET bronze_tables = [bronze_vcf_variants_raw, bronze_gene_annotations_raw, bronze_clinical_variants_raw]
# MAGIC   }
# MAGIC   
# MAGIC   STEP 2: Optional - Clean existing tables
# MAGIC   {
# MAGIC     FOR EACH bronze_table IN bronze_tables
# MAGIC       DROP TABLE IF EXISTS bronze_table
# MAGIC     END FOR
# MAGIC   }
# MAGIC   
# MAGIC   STEP 3: Ingest VCF Variants
# MAGIC   {
# MAGIC     READ VCF file as text
# MAGIC     ADD audit_metadata (timestamp, source_file, ingestion_id, ingestion_date)
# MAGIC     WRITE to bronze_vcf_variants_raw (partitioned by ingestion_date)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 4: Ingest Gene Annotations (GTF)
# MAGIC   {
# MAGIC     READ GTF file as text
# MAGIC     ADD audit_metadata (timestamp, source_file, ingestion_id, ingestion_date)
# MAGIC     WRITE to bronze_gene_annotations_raw (partitioned by ingestion_date)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 5: Ingest ClinVar Clinical Variants
# MAGIC   {
# MAGIC     READ ClinVar file as CSV (tab-delimited)
# MAGIC     SANITIZE column_names (remove special characters)
# MAGIC     ADD audit_metadata (timestamp, source_file, ingestion_id, ingestion_date)
# MAGIC     WRITE to bronze_clinical_variants_raw (partitioned by ingestion_date)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 6: Verify ingestion
# MAGIC   {
# MAGIC     LIST all bronze tables
# MAGIC     COUNT records in each table
# MAGIC     DISPLAY audit metadata
# MAGIC     SHOW sample records
# MAGIC   }
# MAGIC   
# MAGIC   END Bronze Layer Ingestion
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql.functions import current_timestamp, current_date, lit, col, date_format, expr, regexp_replace

# File paths
VCF_FILE = "/Volumes/workspace/default/genome/ALL.chr1.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"
GTF_FILE = "/Volumes/workspace/default/genome/gencode.v49.basic.annotation.gtf.gz"
CLINVAR_FILE = "/Volumes/workspace/default/genome/variant_summary.txt.gz"

# Table names
BRONZE_VCF = "workspace.genomics_project.bronze_vcf_variants_raw"
BRONZE_GTF = "workspace.genomics_project.bronze_gene_annotations_raw"
BRONZE_CLINVAR = "workspace.genomics_project.bronze_clinical_variants_raw"

print("[OK] Configuration loaded (Pure PySpark)")

# COMMAND ----------

# DBTITLE 1,Clean Bronze Tables (Run Once)
# ============================================================================
# CLEANUP: Drop all Bronze tables to start fresh
# ============================================================================
# Run this cell ONLY when you want to delete all data and start over

print("CLEANING UP BRONZE TABLES")
print("="*70)

tables_to_drop = [
    "workspace.genomics_project.bronze_vcf_variants_raw",
    "workspace.genomics_project.bronze_gene_annotations_raw",
    "workspace.genomics_project.bronze_clinical_variants_raw"
]

for table in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"   [OK] Dropped: {table}")
    except Exception as e:
        print(f"   [WARN] Could not drop {table}: {str(e)}")

print("\n[SUCCESS] CLEANUP COMPLETE! All Bronze tables removed.")
print("Now run the ingestion cells (4, 6, 8) to reload data.")
print("="*70)

# COMMAND ----------

# DBTITLE 1,VCF Ingestion
# MAGIC %md
# MAGIC ## 1. VCF Variants Ingestion
# MAGIC Ingesting chromosome 1 variants from 1000 Genomes Phase 3

# COMMAND ----------

# DBTITLE 1,Ingest VCF to Bronze
print("BRONZE INGESTION: VCF Variants")
print("="*70)

# Step 1: Read VCF as text
print("\nReading VCF file...")
vcf_raw = spark.read.text(VCF_FILE)
vcf_raw = vcf_raw.withColumnRenamed("value", "raw_value")

# Step 2: Add audit metadata (Pure PySpark)
print("Adding audit metadata...")

vcf_bronze = vcf_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ALL.chr1.phase3.vcf.gz")) \
    .withColumn("ingestion_id", expr("uuid()")) \
    .withColumn("ingestion_date", current_date())

record_count = vcf_bronze.count()
print(f"[OK] Total records: {record_count:,}")

# Step 3: Write to Bronze
print("\nWriting to Bronze Delta table...")
vcf_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_VCF)

print(f"\n[SUCCESS] {record_count:,} records written to {BRONZE_VCF}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Gene Ingestion
# MAGIC %md
# MAGIC ## 2. Gene Annotations (GTF) Ingestion
# MAGIC Ingesting GENCODE v49 gene annotations

# COMMAND ----------

# DBTITLE 1,Ingest GTF to Bronze
print("BRONZE INGESTION: Gene Annotations (GTF)")
print("="*70)

# Step 1: Read GTF as text
print("\nReading GTF file...")
gtf_raw = spark.read.text(GTF_FILE)
gtf_raw = gtf_raw.withColumnRenamed("value", "raw_value")

# Step 2: Add audit metadata (Pure PySpark)
print("Adding audit metadata...")

gtf_bronze = gtf_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("gencode.v49.gtf.gz")) \
    .withColumn("ingestion_id", expr("uuid()")) \
    .withColumn("ingestion_date", current_date())

record_count = gtf_bronze.count()
print(f"[OK] Total records: {record_count:,}")

# Step 3: Write to Bronze
print("\nWriting to Bronze Delta table...")
gtf_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_GTF)

print(f"\n[SUCCESS] {record_count:,} records written to {BRONZE_GTF}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,ClinVar Ingestion
# MAGIC %md
# MAGIC ## 3. ClinVar Clinical Variants Ingestion
# MAGIC Ingesting clinical variant annotations with pathogenicity classifications

# COMMAND ----------

# DBTITLE 1,Ingest ClinVar to Bronze
print("BRONZE INGESTION: ClinVar Clinical Variants")
print("="*70)

# Step 1: Read ClinVar as CSV (tab-delimited)
print("\nReading ClinVar file...")
clinvar_raw = spark.read \
    .option("sep", "\t") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(CLINVAR_FILE)

print(f"[OK] Total columns: {len(clinvar_raw.columns)}")

# Step 2: Sanitize column names for Delta Lake (Pure PySpark approach)
print("Sanitizing column names...")

# Function to sanitize column names using only Python string methods (no regex)
def sanitize_column_name(col_name):
    """Replace invalid Delta Lake characters with underscores using string methods."""
    result = ''
    for char in col_name:
        if char.isalnum() or char == '_':
            result += char
        else:
            result += '_'
    return result

# Rename columns with sanitized names
for old_col in clinvar_raw.columns:
    new_col = sanitize_column_name(old_col)
    if old_col != new_col:
        clinvar_raw = clinvar_raw.withColumnRenamed(old_col, new_col)

print("[OK] Column names sanitized")

# Step 3: Add audit metadata (Pure PySpark)
print("Adding audit metadata...")

clinvar_bronze = clinvar_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("variant_summary.txt.gz")) \
    .withColumn("ingestion_id", expr("uuid()")) \
    .withColumn("ingestion_date", current_date())

record_count = clinvar_bronze.count()
print(f"[OK] Total records: {record_count:,}")

# Step 4: Write to Bronze
print("\nWriting to Bronze Delta table...")
clinvar_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_CLINVAR)

print(f"\n[SUCCESS] {record_count:,} records written to {BRONZE_CLINVAR}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Verification
# MAGIC %md
# MAGIC ## Bronze Layer Verification
# MAGIC Verify all 3 Bronze tables are properly ingested

# COMMAND ----------

# DBTITLE 1,Verify Bronze Layer
print("BRONZE LAYER VERIFICATION")
print("="*70)

CATALOG = "workspace"
SCHEMA = "genomics_project"

# Check 1: List tables (Pure PySpark SQL)
print("\nCheck 1: Bronze Tables")
print("-" * 70)
bronze_tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE 'bronze*'").collect()
for tbl in bronze_tables:
    print(f"   [OK] {tbl.tableName}")

# Check 2: Record counts (Pure PySpark SQL)
print("\nCheck 2: Record Counts")
print("-" * 70)

vcf_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_VCF}").first()[0]
gtf_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_GTF}").first()[0]
clinvar_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_CLINVAR}").first()[0]

print(f"   VCF: {vcf_count:,} records")
print(f"   GTF: {gtf_count:,} records")
print(f"   ClinVar: {clinvar_count:,} records")

# Check 3: Audit metadata (Pure PySpark SQL)
print("\nCheck 3: Audit Metadata")
print("-" * 70)

for table_name, display_name in [(BRONZE_VCF, "VCF"), (BRONZE_GTF, "GTF"), (BRONZE_CLINVAR, "ClinVar")]:
    print(f"\n   {display_name}:")
    audit = spark.sql(f"""
        SELECT source_file, ingestion_date, COUNT(*) as count
        FROM {table_name}
        GROUP BY source_file, ingestion_date
    """).collect()
    for row in audit:
        print(f"     • {row.source_file} | {row.ingestion_date} | {row['count']:,}")

# Check 4: Sample data (Pure PySpark SQL)
print("\nCheck 4: Sample Data")
print("-" * 70)

print("\n   VCF Sample:")
spark.sql(f"SELECT raw_value FROM {BRONZE_VCF} WHERE raw_value NOT LIKE '#%' LIMIT 3").show(3, truncate=80)

print("\n   GTF Sample:")
spark.sql(f"SELECT raw_value FROM {BRONZE_GTF} WHERE raw_value NOT LIKE '#%' LIMIT 3").show(3, truncate=80)

print("\n   ClinVar Sample:")
spark.sql(f"SELECT _AlleleID, Type, GeneSymbol, ClinicalSignificance, Chromosome FROM {BRONZE_CLINVAR} LIMIT 3").show(3, truncate=50)

print("\n" + "="*70)
print("[SUCCESS] BRONZE LAYER VERIFICATION COMPLETE!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,ETL Test: Record Count Validation
# MAGIC %md
# MAGIC ## ETL Test: Record Count Validation
# MAGIC
# MAGIC **Purpose**: Verify Bronze layer ingestion completeness
# MAGIC
# MAGIC **Expected Counts**:
# MAGIC * VCF Variants: 6,468,347
# MAGIC * GTF Annotations: 5,868,517
# MAGIC * ClinVar Variants: 8,980,556
# MAGIC
# MAGIC **Pass Criteria**: Counts match expected values within 0.1% tolerance

# COMMAND ----------

# DBTITLE 1,Test Execution: Record Count
print("\n" + "="*70)
print("BRONZE LAYER TEST: RECORD COUNT VALIDATION")
print("="*70)

# Expected values
EXPECTED_VCF = 6468347
EXPECTED_GTF = 5868517
EXPECTED_CLINVAR = 8980556

# Actual counts
actual_vcf = spark.table(BRONZE_VCF).count()
actual_gtf = spark.table(BRONZE_GTF).count()
actual_clinvar = spark.table(BRONZE_CLINVAR).count()

print("\nRecord Counts:")
print(f"  VCF Variants:     {actual_vcf:>10,} (Expected: {EXPECTED_VCF:>10,})")
print(f"  GTF Annotations:  {actual_gtf:>10,} (Expected: {EXPECTED_GTF:>10,})")
print(f"  ClinVar Variants: {actual_clinvar:>10,} (Expected: {EXPECTED_CLINVAR:>10,})")
print(f"  Total:            {(actual_vcf + actual_gtf + actual_clinvar):>10,}")

# Validation (0.1% tolerance)
tolerance = 0.001
vcf_match = abs(actual_vcf - EXPECTED_VCF) / EXPECTED_VCF <= tolerance
gtf_match = abs(actual_gtf - EXPECTED_GTF) / EXPECTED_GTF <= tolerance
clinvar_match = abs(actual_clinvar - EXPECTED_CLINVAR) / EXPECTED_CLINVAR <= tolerance

test_passed = vcf_match and gtf_match and clinvar_match

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Bronze Layer: All record counts match expected values")
else:
    print("[✗ FAIL] Bronze Layer: Record count mismatch detected")
    if not vcf_match:
        print(f"  - VCF variance: {((actual_vcf - EXPECTED_VCF) / EXPECTED_VCF * 100):.2f}%")
    if not gtf_match:
        print(f"  - GTF variance: {((actual_gtf - EXPECTED_GTF) / EXPECTED_GTF * 100):.2f}%")
    if not clinvar_match:
        print(f"  - ClinVar variance: {((actual_clinvar - EXPECTED_CLINVAR) / EXPECTED_CLINVAR * 100):.2f}%")
print("="*70 + "\n")