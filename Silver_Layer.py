# Databricks notebook source
# DBTITLE 1,Silver Layer Overview
# MAGIC %md
# MAGIC # Silver Layer: Data Transformation & Cleansing
# MAGIC
# MAGIC **Purpose**: Transform Bronze raw data into structured, typed, analytics-ready Silver tables
# MAGIC
# MAGIC **Transformations**:
# MAGIC 1. VCF Parsing (6.4M variants → structured)
# MAGIC 2. GTF Parsing (5.8M annotations → structured)
# MAGIC 3. ClinVar Type Casting (9M → 4.5M validated records)
# MAGIC
# MAGIC **Features**:
# MAGIC - Type casting (string → int/long/double)
# MAGIC - Data validation & filtering
# MAGIC - Deduplication
# MAGIC - Error handling with automatic rollback
# MAGIC - Partition by chromosome
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql.functions import (
    col, split, current_timestamp, when, length, trim, regexp_extract
)
from pyspark.sql.types import IntegerType, DoubleType, LongType

# Bronze table names
BRONZE_VCF = "workspace.genomics_project.bronze_vcf_variants_raw"
BRONZE_GTF = "workspace.genomics_project.bronze_gene_annotations_raw"
BRONZE_CLINVAR = "workspace.genomics_project.bronze_clinical_variants_raw"

# Silver table names
SILVER_VCF = "workspace.genomics_project.silver_vcf_variants"
SILVER_GTF = "workspace.genomics_project.silver_gene_annotations"
SILVER_CLINVAR = "workspace.genomics_project.silver_clinical_variants"

print("✅ Configuration loaded")

# COMMAND ----------

# DBTITLE 1,VCF Transformation
# MAGIC %md
# MAGIC ## 1. VCF Variants Transformation
# MAGIC Parse VCF text into structured columns with variant classification

# COMMAND ----------

# DBTITLE 1,Transform VCF to Silver
print("🧬 SILVER TRANSFORMATION: VCF Variants")
print("="*70)

try:
    # Read Bronze
    print("\n📂 Reading Bronze VCF...")
    bronze_vcf = spark.table(BRONZE_VCF)
    print(f"✅ Loaded {bronze_vcf.count():,} records")
    
    # Filter data lines
    print("🧹 Filtering data lines...")
    vcf_data = bronze_vcf.filter(~col("raw_value").startswith("#"))
    print(f"✅ Data lines: {vcf_data.count():,}")
    
    # Parse VCF columns
    print("✂️ Parsing VCF format...")
    vcf_parsed = vcf_data.select(
        split(col("raw_value"), "\t").getItem(0).alias("chrom"),
        split(col("raw_value"), "\t").getItem(1).cast(IntegerType()).alias("pos"),
        split(col("raw_value"), "\t").getItem(2).alias("variant_id"),
        split(col("raw_value"), "\t").getItem(3).alias("ref_allele"),
        split(col("raw_value"), "\t").getItem(4).alias("alt_allele"),
        split(col("raw_value"), "\t").getItem(5).cast(DoubleType()).alias("quality_score"),
        split(col("raw_value"), "\t").getItem(6).alias("filter_status"),
        split(col("raw_value"), "\t").getItem(7).alias("info"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        col("source_file"),
        current_timestamp().alias("silver_processing_timestamp")
    )
    
    # Add quality flags and variant type
    print("✅ Adding variant classification...")
    silver_vcf = vcf_parsed \
        .withColumn("is_high_quality", when(col("filter_status") == "PASS", True).otherwise(False)) \
        .withColumn("ref_length", length(col("ref_allele"))) \
        .withColumn("alt_length", length(col("alt_allele"))) \
        .withColumn(
            "variant_type",
            when(col("ref_length") == col("alt_length"), "SNP")
            .when(col("ref_length") < col("alt_length"), "INSERTION")
            .when(col("ref_length") > col("alt_length"), "DELETION")
            .otherwise("COMPLEX")
        )
    
    # Validation
    print("🔍 Validating data...")
    null_check = silver_vcf.filter(col("chrom").isNull() | col("pos").isNull()).count()
    if null_check > 0:
        raise ValueError(f"Validation failed: {null_check} records with null chrom/pos")
    print("✅ Validation passed")
    
    # Write to Silver
    print("\n💾 Writing to Silver...")
    silver_vcf.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("chrom") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_VCF)
    
    final_count = spark.table(SILVER_VCF).count()
    print(f"\n✅ SUCCESS: {final_count:,} records written to {SILVER_VCF}")
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    print("🔄 ROLLBACK: Previous table version preserved")
    print("="*70)
    raise

# COMMAND ----------

# DBTITLE 1,Gene Transformation
# MAGIC %md
# MAGIC ## 2. Gene Annotations (GTF) Transformation
# MAGIC Parse GTF into 9 standard columns with gene attributes

# COMMAND ----------

# DBTITLE 1,Transform GTF to Silver
print("🧠 SILVER TRANSFORMATION: Gene Annotations")
print("="*70)

try:
    # Read Bronze
    print("\n📂 Reading Bronze GTF...")
    bronze_gtf = spark.table(BRONZE_GTF)
    print(f"✅ Loaded {bronze_gtf.count():,} records")
    
    # Filter data lines
    print("🧹 Filtering data lines...")
    gtf_data = bronze_gtf.filter(~col("raw_value").startswith("#"))
    print(f"✅ Data lines: {gtf_data.count():,}")
    
    # Parse GTF columns
    print("✂️ Parsing GTF format...")
    gtf_parsed = gtf_data.select(
        split(col("raw_value"), "\t").getItem(0).alias("seqname"),
        split(col("raw_value"), "\t").getItem(1).alias("source"),
        split(col("raw_value"), "\t").getItem(2).alias("feature"),
        split(col("raw_value"), "\t").getItem(3).cast(IntegerType()).alias("start_pos"),
        split(col("raw_value"), "\t").getItem(4).cast(IntegerType()).alias("end_pos"),
        split(col("raw_value"), "\t").getItem(5).alias("score"),
        split(col("raw_value"), "\t").getItem(6).alias("strand"),
        split(col("raw_value"), "\t").getItem(7).alias("frame"),
        split(col("raw_value"), "\t").getItem(8).alias("attributes"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        col("source_file"),
        current_timestamp().alias("silver_processing_timestamp")
    )
    
    # Extract gene attributes
    print("✅ Extracting gene attributes...")
    silver_gtf = gtf_parsed \
        .withColumn("gene_id", regexp_extract(col("attributes"), r'gene_id "([^"]+)"', 1)) \
        .withColumn("gene_name", regexp_extract(col("attributes"), r'gene_name "([^"]+)"', 1)) \
        .withColumn("gene_type", regexp_extract(col("attributes"), r'gene_type "([^"]+)"', 1)) \
        .withColumn("transcript_id", regexp_extract(col("attributes"), r'transcript_id "([^"]+)"', 1)) \
        .withColumn("length", col("end_pos") - col("start_pos") + 1)
    
    # Validation
    print("🔍 Validating data...")
    null_check = silver_gtf.filter(col("seqname").isNull() | col("start_pos").isNull()).count()
    if null_check > 0:
        raise ValueError(f"Validation failed: {null_check} records with null seqname/positions")
    print("✅ Validation passed")
    
    # Write to Silver
    print("\n💾 Writing to Silver...")
    silver_gtf.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("seqname") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_GTF)
    
    final_count = spark.table(SILVER_GTF).count()
    print(f"\n✅ SUCCESS: {final_count:,} records written to {SILVER_GTF}")
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    print("🔄 ROLLBACK: Previous table version preserved")
    print("="*70)
    raise

# COMMAND ----------

# DBTITLE 1,ClinVar Transformation
# MAGIC %md
# MAGIC ## 3. ClinVar Clinical Variants Transformation
# MAGIC Type cast and validate clinical variant data

# COMMAND ----------

# DBTITLE 1,Transform ClinVar to Silver
print("⚕️ SILVER TRANSFORMATION: ClinVar Clinical Variants")
print("="*70)

try:
    # Read Bronze
    print("\n📂 Reading Bronze ClinVar...")
    bronze_clinvar = spark.table(BRONZE_CLINVAR)
    print(f"✅ Loaded {bronze_clinvar.count():,} records")
    
    # Helper function for numeric cleaning
    def clean_numeric(column_name, cast_type):
        return when(
            (col(column_name) == "-1") | (col(column_name) == "") | col(column_name).isNull(),
            None
        ).otherwise(col(column_name).cast(cast_type))
    
    # Type cast columns
    print("🔄 Type casting columns...")
    silver_clinvar = bronze_clinvar.select(
        # Primary identifiers
        clean_numeric("_AlleleID", IntegerType()).alias("allele_id"),
        clean_numeric("VariationID", IntegerType()).alias("variation_id"),
        trim(col("Type")).alias("variant_type"),
        
        # Gene information
        trim(col("GeneSymbol")).alias("gene_symbol"),
        clean_numeric("GeneID", IntegerType()).alias("gene_id"),
        
        # Clinical significance
        trim(col("ClinicalSignificance")).alias("clinical_significance"),
        trim(col("ReviewStatus")).alias("review_status"),
        
        # Genomic location
        trim(col("Chromosome")).alias("chromosome"),
        clean_numeric("Start", LongType()).alias("start_pos"),
        clean_numeric("Stop", LongType()).alias("stop_pos"),
        trim(col("ReferenceAllele")).alias("ref_allele"),
        trim(col("AlternateAllele")).alias("alt_allele"),
        
        # Assembly
        trim(col("Assembly")).alias("assembly"),
        
        # Phenotypes
        trim(col("PhenotypeIDS")).alias("phenotype_ids"),
        trim(col("PhenotypeList")).alias("phenotype_list"),
        
        # Metadata
        trim(col("Origin")).alias("origin"),
        clean_numeric("NumberSubmitters", IntegerType()).alias("number_submitters"),
        trim(col("LastEvaluated")).alias("last_evaluated"),
        
        # Audit
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        col("source_file"),
        current_timestamp().alias("silver_processing_timestamp")
    )
    
    # Validation and filtering
    print("✅ Validating and filtering...")
    valid_chromosomes = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]
    
    silver_clinvar_validated = silver_clinvar \
        .filter(col("chromosome").isin(valid_chromosomes)) \
        .filter(col("allele_id").isNotNull()) \
        .dropDuplicates(["allele_id"])
    
    validated_count = silver_clinvar_validated.count()
    print(f"✅ Valid records: {validated_count:,}")
    
    # Write to Silver
    print("\n💾 Writing to Silver...")
    silver_clinvar_validated.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("chromosome") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_CLINVAR)
    
    final_count = spark.table(SILVER_CLINVAR).count()
    print(f"\n✅ SUCCESS: {final_count:,} records written to {SILVER_CLINVAR}")
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    print("🔄 ROLLBACK: Previous table version preserved")
    print("="*70)
    raise

# COMMAND ----------

# DBTITLE 1,Verification
# MAGIC %md
# MAGIC ## Silver Layer Verification
# MAGIC Verify all 3 Silver tables with data quality checks

# COMMAND ----------

# DBTITLE 1,Verify Silver Layer
print("🔍 SILVER LAYER VERIFICATION")
print("="*70)

CATALOG = "workspace"
SCHEMA = "genomics_project"

# Check 1: List tables
print("\n📋 Check 1: Silver Tables")
print("-" * 70)
silver_tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE 'silver*'").collect()
for tbl in silver_tables:
    print(f"   ✅ {tbl.tableName}")

# Check 2: Record counts
print("\n📊 Check 2: Record Counts")
print("-" * 70)

vcf_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SILVER_VCF}").first()[0]
gtf_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SILVER_GTF}").first()[0]
clinvar_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SILVER_CLINVAR}").first()[0]

print(f"   VCF Variants: {vcf_count:,}")
print(f"   Gene Annotations: {gtf_count:,}")
print(f"   Clinical Variants: {clinvar_count:,}")

# Check 3: Schema types
print("\n🔍 Check 3: Schema Verification (First 8 Fields)")
print("-" * 70)

print("\n   VCF Schema:")
for field in spark.table(SILVER_VCF).schema.fields[:8]:
    print(f"     • {field.name}: {field.dataType}")

print("\n   GTF Schema:")
for field in spark.table(SILVER_GTF).schema.fields[:8]:
    print(f"     • {field.name}: {field.dataType}")

print("\n   ClinVar Schema:")
for field in spark.table(SILVER_CLINVAR).schema.fields[:8]:
    print(f"     • {field.name}: {field.dataType}")

# Check 4: Data quality metrics
print("\n✅ Check 4: Data Quality Metrics")
print("-" * 70)

print("\n   VCF Variant Types:")
variant_types = spark.sql(f"SELECT variant_type, COUNT(*) as count FROM {SILVER_VCF} GROUP BY variant_type ORDER BY count DESC").collect()
for row in variant_types:
    print(f"     • {row.variant_type}: {row['count']:,}")

print("\n   ClinVar Clinical Significance (Top 5):")
clin_sig = spark.sql(f"SELECT clinical_significance, COUNT(*) as count FROM {SILVER_CLINVAR} GROUP BY clinical_significance ORDER BY count DESC LIMIT 5").collect()
for row in clin_sig:
    print(f"     • {row.clinical_significance}: {row['count']:,}")

print("\n   GTF Feature Types (Top 5):")
features = spark.sql(f"SELECT feature, COUNT(*) as count FROM {SILVER_GTF} GROUP BY feature ORDER BY count DESC LIMIT 5").collect()
for row in features:
    print(f"     • {row.feature}: {row['count']:,}")

# Check 5: Sample data
print("\n🔍 Check 5: Sample Data (3 records each)")
print("-" * 70)

print("\n   VCF Sample:")
spark.sql(f"SELECT chrom, pos, variant_id, ref_allele, alt_allele, variant_type FROM {SILVER_VCF} LIMIT 3").show(3, truncate=False)

print("\n   Gene Sample:")
spark.sql(f"SELECT seqname, feature, start_pos, end_pos, gene_name, strand FROM {SILVER_GTF} WHERE gene_name != '' LIMIT 3").show(3, truncate=False)

print("\n   ClinVar Sample:")
spark.sql(f"SELECT allele_id, gene_symbol, clinical_significance, chromosome FROM {SILVER_CLINVAR} LIMIT 3").show(3, truncate=False)

print("\n" + "="*70)
print("✅ SILVER LAYER VERIFICATION COMPLETE!")
print("="*70)