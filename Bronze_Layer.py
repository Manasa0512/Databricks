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

print("✅ Configuration loaded (Pure PySpark)")

# COMMAND ----------

# DBTITLE 1,Clean Bronze Tables (Run Once)
# ============================================================================
# CLEANUP: Drop all Bronze tables to start fresh
# ============================================================================
# Run this cell ONLY when you want to delete all data and start over

print("🗑️ CLEANING UP BRONZE TABLES")
print("="*70)

tables_to_drop = [
    "workspace.genomics_project.bronze_vcf_variants_raw",
    "workspace.genomics_project.bronze_gene_annotations_raw",
    "workspace.genomics_project.bronze_clinical_variants_raw"
]

for table in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"   ✅ Dropped: {table}")
    except Exception as e:
        print(f"   ⚠️ Could not drop {table}: {str(e)}")

print("\n✅ CLEANUP COMPLETE! All Bronze tables removed.")
print("Now run the ingestion cells (4, 6, 8) to reload data.")
print("="*70)

# COMMAND ----------

# DBTITLE 1,VCF Ingestion
# MAGIC %md
# MAGIC ## 1. VCF Variants Ingestion
# MAGIC Ingesting chromosome 1 variants from 1000 Genomes Phase 3

# COMMAND ----------

# DBTITLE 1,Ingest VCF to Bronze
print("🧬 BRONZE INGESTION: VCF Variants")
print("="*70)

# Step 1: Read VCF as text
print("\n📂 Reading VCF file...")
vcf_raw = spark.read.text(VCF_FILE)
vcf_raw = vcf_raw.withColumnRenamed("value", "raw_value")

# Step 2: Add audit metadata (Pure PySpark)
print("📋 Adding audit metadata...")

vcf_bronze = vcf_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("ALL.chr1.phase3.vcf.gz")) \
    .withColumn("ingestion_id", expr("uuid()")) \
    .withColumn("ingestion_date", current_date())

record_count = vcf_bronze.count()
print(f"✅ Total records: {record_count:,}")

# Step 3: Write to Bronze
print("\n💾 Writing to Bronze Delta table...")
vcf_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_VCF)

print(f"\n✅ SUCCESS: {record_count:,} records written to {BRONZE_VCF}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Gene Ingestion
# MAGIC %md
# MAGIC ## 2. Gene Annotations (GTF) Ingestion
# MAGIC Ingesting GENCODE v49 gene annotations

# COMMAND ----------

# DBTITLE 1,Ingest GTF to Bronze
print("🧠 BRONZE INGESTION: Gene Annotations (GTF)")
print("="*70)

# Step 1: Read GTF as text
print("\n📂 Reading GTF file...")
gtf_raw = spark.read.text(GTF_FILE)
gtf_raw = gtf_raw.withColumnRenamed("value", "raw_value")

# Step 2: Add audit metadata (Pure PySpark)
print("📋 Adding audit metadata...")

gtf_bronze = gtf_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("gencode.v49.gtf.gz")) \
    .withColumn("ingestion_id", expr("uuid()")) \
    .withColumn("ingestion_date", current_date())

record_count = gtf_bronze.count()
print(f"✅ Total records: {record_count:,}")

# Step 3: Write to Bronze
print("\n💾 Writing to Bronze Delta table...")
gtf_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_GTF)

print(f"\n✅ SUCCESS: {record_count:,} records written to {BRONZE_GTF}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,ClinVar Ingestion
# MAGIC %md
# MAGIC ## 3. ClinVar Clinical Variants Ingestion
# MAGIC Ingesting clinical variant annotations with pathogenicity classifications

# COMMAND ----------

# DBTITLE 1,Ingest ClinVar to Bronze
print("⚕️ BRONZE INGESTION: ClinVar Clinical Variants")
print("="*70)

# Step 1: Read ClinVar as CSV (tab-delimited)
print("\n📂 Reading ClinVar file...")
clinvar_raw = spark.read \
    .option("sep", "\t") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(CLINVAR_FILE)

print(f"✅ Total columns: {len(clinvar_raw.columns)}")

# Step 2: Sanitize column names for Delta Lake (Pure PySpark approach)
print("🔧 Sanitizing column names...")

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

print("✅ Column names sanitized")

# Step 3: Add audit metadata (Pure PySpark)
print("📋 Adding audit metadata...")

clinvar_bronze = clinvar_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("variant_summary.txt.gz")) \
    .withColumn("ingestion_id", expr("uuid()")) \
    .withColumn("ingestion_date", current_date())

record_count = clinvar_bronze.count()
print(f"✅ Total records: {record_count:,}")

# Step 4: Write to Bronze
print("\n💾 Writing to Bronze Delta table...")
clinvar_bronze.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ingestion_date") \
    .option("mergeSchema", "true") \
    .saveAsTable(BRONZE_CLINVAR)

print(f"\n✅ SUCCESS: {record_count:,} records written to {BRONZE_CLINVAR}")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Verification
# MAGIC %md
# MAGIC ## Bronze Layer Verification
# MAGIC Verify all 3 Bronze tables are properly ingested

# COMMAND ----------

# DBTITLE 1,Verify Bronze Layer
print("🔍 BRONZE LAYER VERIFICATION")
print("="*70)

CATALOG = "workspace"
SCHEMA = "genomics_project"

# Check 1: List tables (Pure PySpark SQL)
print("\n📋 Check 1: Bronze Tables")
print("-" * 70)
bronze_tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE 'bronze*'").collect()
for tbl in bronze_tables:
    print(f"   ✅ {tbl.tableName}")

# Check 2: Record counts (Pure PySpark SQL)
print("\n📊 Check 2: Record Counts")
print("-" * 70)

vcf_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_VCF}").first()[0]
gtf_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_GTF}").first()[0]
clinvar_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_CLINVAR}").first()[0]

print(f"   VCF: {vcf_count:,} records")
print(f"   GTF: {gtf_count:,} records")
print(f"   ClinVar: {clinvar_count:,} records")

# Check 3: Audit metadata (Pure PySpark SQL)
print("\n📝 Check 3: Audit Metadata")
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
print("\n🔍 Check 4: Sample Data")
print("-" * 70)

print("\n   VCF Sample:")
spark.sql(f"SELECT raw_value FROM {BRONZE_VCF} WHERE raw_value NOT LIKE '#%' LIMIT 3").show(3, truncate=80)

print("\n   GTF Sample:")
spark.sql(f"SELECT raw_value FROM {BRONZE_GTF} WHERE raw_value NOT LIKE '#%' LIMIT 3").show(3, truncate=80)

print("\n   ClinVar Sample:")
spark.sql(f"SELECT _AlleleID, Type, GeneSymbol, ClinicalSignificance, Chromosome FROM {BRONZE_CLINVAR} LIMIT 3").show(3, truncate=50)

print("\n" + "="*70)
print("✅ BRONZE LAYER VERIFICATION COMPLETE!")
print("="*70)