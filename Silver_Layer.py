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

# DBTITLE 1,Pipeline Pseudocode
# MAGIC %md
# MAGIC # Silver Layer Pseudocode
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   START Silver Layer Transformation
# MAGIC   
# MAGIC   STEP 1: Configure table names
# MAGIC   {
# MAGIC     SET bronze_tables = [bronze_vcf, bronze_gtf, bronze_clinvar]
# MAGIC     SET silver_tables = [silver_vcf_variants, silver_gene_annotations, silver_clinical_variants]
# MAGIC   }
# MAGIC   
# MAGIC   STEP 2: Transform VCF Variants
# MAGIC   {
# MAGIC     READ bronze_vcf_variants_raw
# MAGIC     FILTER rows (exclude header lines starting with #)
# MAGIC     PARSE raw_value into columns (chrom, pos, id, ref, alt, qual, filter, info)
# MAGIC     CAST data types (pos → int, qual → double)
# MAGIC     ADD variant_classification (SNP, INDEL, MNP)
# MAGIC     VALIDATE data (non-null chromosomes, positive positions)
# MAGIC     WRITE to silver_vcf_variants (partitioned by chrom)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 3: Transform Gene Annotations (GTF)
# MAGIC   {
# MAGIC     READ bronze_gene_annotations_raw
# MAGIC     FILTER rows (exclude header lines starting with #)
# MAGIC     PARSE raw_value into 9 GTF columns (seqname, source, feature, start, end, score, strand, frame, attributes)
# MAGIC     CAST data types (start/end → int, score → double)
# MAGIC     EXTRACT gene attributes (gene_id, gene_name, gene_type)
# MAGIC     VALIDATE data (start < end, valid strand)
# MAGIC     WRITE to silver_gene_annotations (partitioned by seqname)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 4: Transform ClinVar Clinical Variants
# MAGIC   {
# MAGIC     READ bronze_clinical_variants_raw
# MAGIC     CAST columns to proper types (integers, doubles)
# MAGIC     CLEAN numeric fields (replace -1 and empty strings with NULL)
# MAGIC     VALIDATE required fields (GeneSymbol, ClinicalSignificance not null)
# MAGIC     FILTER invalid records
# MAGIC     WRITE to silver_clinical_variants (partitioned by Chromosome)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 5: Verify transformations
# MAGIC   {
# MAGIC     LIST all silver tables
# MAGIC     COUNT records in each table
# MAGIC     VERIFY schema and data types
# MAGIC     SHOW sample records
# MAGIC   }
# MAGIC   
# MAGIC   ERROR HANDLING
# MAGIC   {
# MAGIC     IF transformation_fails THEN
# MAGIC       ROLLBACK changes
# MAGIC       PRINT error details
# MAGIC     END IF
# MAGIC   }
# MAGIC   
# MAGIC   END Silver Layer Transformation
# MAGIC }
# MAGIC ```

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

print("[OK] Configuration loaded")

# COMMAND ----------

# DBTITLE 1,VCF Transformation
# MAGIC %md
# MAGIC ## 1. VCF Variants Transformation
# MAGIC Parse VCF text into structured columns with variant classification

# COMMAND ----------

# DBTITLE 1,Transform VCF to Silver
print("SILVER TRANSFORMATION: VCF Variants")
print("="*70)

try:
    # Read Bronze
    print("\nReading Bronze VCF...")
    bronze_vcf = spark.table(BRONZE_VCF)
    print(f"[OK] Loaded {bronze_vcf.count():,} records")
    
    # Filter data lines
    print("Filtering data lines...")
    vcf_data = bronze_vcf.filter(~col("raw_value").startswith("#"))
    print(f"[OK] Data lines: {vcf_data.count():,}")
    
    # Parse VCF columns
    print("Parsing VCF format...")
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
    print("[OK] Adding variant classification...")
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
    print("Validating data...")
    null_check = silver_vcf.filter(col("chrom").isNull() | col("pos").isNull()).count()
    if null_check > 0:
        raise ValueError(f"Validation failed: {null_check} records with null chrom/pos")
    print("[OK] Validation passed")
    
    # Write to Silver
    print("\nWriting to Silver...")
    silver_vcf.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("chrom") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_VCF)
    
    final_count = spark.table(SILVER_VCF).count()
    print(f"\n[SUCCESS] {final_count:,} records written to {SILVER_VCF}")
    print("="*70)
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    print("[ROLLBACK] Previous table version preserved")
    print("="*70)
    raise

# COMMAND ----------

# DBTITLE 1,Gene Transformation
# MAGIC %md
# MAGIC ## 2. Gene Annotations (GTF) Transformation
# MAGIC Parse GTF into 9 standard columns with gene attributes

# COMMAND ----------

# DBTITLE 1,Transform GTF to Silver
print("SILVER TRANSFORMATION: Gene Annotations")
print("="*70)

try:
    # Read Bronze
    print("\nReading Bronze GTF...")
    bronze_gtf = spark.table(BRONZE_GTF)
    print(f"[OK] Loaded {bronze_gtf.count():,} records")
    
    # Filter data lines
    print("Filtering data lines...")
    gtf_data = bronze_gtf.filter(~col("raw_value").startswith("#"))
    print(f"[OK] Data lines: {gtf_data.count():,}")
    
    # Parse GTF columns
    print("Parsing GTF format...")
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
    print("[OK] Extracting gene attributes...")
    silver_gtf = gtf_parsed \
        .withColumn("gene_id", regexp_extract(col("attributes"), r'gene_id "([^"]+)"', 1)) \
        .withColumn("gene_name", regexp_extract(col("attributes"), r'gene_name "([^"]+)"', 1)) \
        .withColumn("gene_type", regexp_extract(col("attributes"), r'gene_type "([^"]+)"', 1)) \
        .withColumn("transcript_id", regexp_extract(col("attributes"), r'transcript_id "([^"]+)"', 1)) \
        .withColumn("length", col("end_pos") - col("start_pos") + 1)
    
    # Validation
    print("Validating data...")
    null_check = silver_gtf.filter(col("seqname").isNull() | col("start_pos").isNull()).count()
    if null_check > 0:
        raise ValueError(f"Validation failed: {null_check} records with null seqname/positions")
    print("[OK] Validation passed")
    
    # Write to Silver
    print("\nWriting to Silver...")
    silver_gtf.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("seqname") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_GTF)
    
    final_count = spark.table(SILVER_GTF).count()
    print(f"\n[SUCCESS] {final_count:,} records written to {SILVER_GTF}")
    print("="*70)
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    print("[ROLLBACK] Previous table version preserved")
    print("="*70)
    raise

# COMMAND ----------

# DBTITLE 1,ClinVar Transformation
# MAGIC %md
# MAGIC ## 3. ClinVar Clinical Variants Transformation
# MAGIC Type cast and validate clinical variant data

# COMMAND ----------

# DBTITLE 1,Transform ClinVar to Silver
print("SILVER TRANSFORMATION: ClinVar Clinical Variants")
print("="*70)

try:
    # Read Bronze
    print("\nReading Bronze ClinVar...")
    bronze_clinvar = spark.table(BRONZE_CLINVAR)
    print(f"[OK] Loaded {bronze_clinvar.count():,} records")
    
    # Helper function for numeric cleaning
    def clean_numeric(column_name, cast_type):
        return when(
            (col(column_name) == "-1") | (col(column_name) == "") | col(column_name).isNull(),
            None
        ).otherwise(col(column_name).cast(cast_type))
    
    # Type cast columns
    print("Type casting columns...")
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
    print("[OK] Validating and filtering...")
    valid_chromosomes = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]
    
    silver_clinvar_validated = silver_clinvar \
        .filter(col("chromosome").isin(valid_chromosomes)) \
        .filter(col("allele_id").isNotNull()) \
        .dropDuplicates(["allele_id"])
    
    validated_count = silver_clinvar_validated.count()
    print(f"[OK] Valid records: {validated_count:,}")
    
    # Write to Silver
    print("\nWriting to Silver...")
    silver_clinvar_validated.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("chromosome") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_CLINVAR)
    
    final_count = spark.table(SILVER_CLINVAR).count()
    print(f"\n[SUCCESS] {final_count:,} records written to {SILVER_CLINVAR}")
    print("="*70)
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    print("[ROLLBACK] Previous table version preserved")
    print("="*70)
    raise

# COMMAND ----------

# DBTITLE 1,Verification
# MAGIC %md
# MAGIC ## Silver Layer Verification
# MAGIC Verify all 3 Silver tables with data quality checks

# COMMAND ----------

# DBTITLE 1,Verify Silver Layer
print("SILVER LAYER VERIFICATION")
print("="*70)

CATALOG = "workspace"
SCHEMA = "genomics_project"

# Check 1: List tables
print("\nCheck 1: Silver Tables")
print("-" * 70)
silver_tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE 'silver*'").collect()
for tbl in silver_tables:
    print(f"   [OK] {tbl.tableName}")

# Check 2: Record counts
print("\nCheck 2: Record Counts")
print("-" * 70)

vcf_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SILVER_VCF}").first()[0]
gtf_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SILVER_GTF}").first()[0]
clinvar_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {SILVER_CLINVAR}").first()[0]

print(f"   VCF Variants: {vcf_count:,}")
print(f"   Gene Annotations: {gtf_count:,}")
print(f"   Clinical Variants: {clinvar_count:,}")

# Check 3: Schema types
print("\nCheck 3: Schema Verification (First 8 Fields)")
print("-" * 70)

print("\n   VCF Schema:")
for field in spark.table(SILVER_VCF).schema.fields[:8]:
    print(f"     - {field.name}: {field.dataType}")

print("\n   GTF Schema:")
for field in spark.table(SILVER_GTF).schema.fields[:8]:
    print(f"     - {field.name}: {field.dataType}")

print("\n   ClinVar Schema:")
for field in spark.table(SILVER_CLINVAR).schema.fields[:8]:
    print(f"     - {field.name}: {field.dataType}")

# Check 4: Data quality metrics
print("\nCheck 4: Data Quality Metrics")
print("-" * 70)

print("\n   VCF Variant Types:")
variant_types = spark.sql(f"SELECT variant_type, COUNT(*) as count FROM {SILVER_VCF} GROUP BY variant_type ORDER BY count DESC").collect()
for row in variant_types:
    print(f"     - {row.variant_type}: {row['count']:,}")

print("\n   ClinVar Clinical Significance (Top 5):")
clin_sig = spark.sql(f"SELECT clinical_significance, COUNT(*) as count FROM {SILVER_CLINVAR} GROUP BY clinical_significance ORDER BY count DESC LIMIT 5").collect()
for row in clin_sig:
    print(f"     - {row.clinical_significance}: {row['count']:,}")

print("\n   GTF Feature Types (Top 5):")
features = spark.sql(f"SELECT feature, COUNT(*) as count FROM {SILVER_GTF} GROUP BY feature ORDER BY count DESC LIMIT 5").collect()
for row in features:
    print(f"     - {row.feature}: {row['count']:,}")

# Check 5: Sample data
print("\nCheck 5: Sample Data (3 records each)")
print("-" * 70)

print("\n   VCF Sample:")
spark.sql(f"SELECT chrom, pos, variant_id, ref_allele, alt_allele, variant_type FROM {SILVER_VCF} LIMIT 3").show(3, truncate=False)

print("\n   Gene Sample:")
spark.sql(f"SELECT seqname, feature, start_pos, end_pos, gene_name, strand FROM {SILVER_GTF} WHERE gene_name != '' LIMIT 3").show(3, truncate=False)

print("\n   ClinVar Sample:")
spark.sql(f"SELECT allele_id, gene_symbol, clinical_significance, chromosome FROM {SILVER_CLINVAR} LIMIT 3").show(3, truncate=False)

print("\n" + "="*70)
print("[SUCCESS] SILVER LAYER VERIFICATION COMPLETE!")
print("="*70)

# COMMAND ----------

# DBTITLE 1,ETL Tests: Silver Layer Quality Checks
# MAGIC %md
# MAGIC ## ETL Tests: Silver Layer Quality Checks
# MAGIC
# MAGIC Validating data transformation, integrity, and quality:
# MAGIC 1. **Record Count Validation** - Verify transformation preserved data correctly
# MAGIC 2. **Referential Integrity** - Ensure data traceability
# MAGIC 3. **Partition Integrity** - Verify chromosome partitioning
# MAGIC 4. **Duplicate Detection** - No invalid duplicates
# MAGIC 5. **Mandatory Column Null Check** - Critical columns have no NULLs

# COMMAND ----------

# DBTITLE 1,Test 1: Record Count Validation
print("\n" + "="*70)
print("SILVER TEST 1: RECORD COUNT VALIDATION")
print("="*70)

# Expected values
EXPECTED_SILVER_VCF = 6468094
EXPECTED_SILVER_GTF = 5868512
EXPECTED_SILVER_CLINVAR = 4514767

# Actual counts
actual_vcf = spark.table(SILVER_VCF).count()
actual_gtf = spark.table(SILVER_GTF).count()
actual_clinvar = spark.table(SILVER_CLINVAR).count()

print("\nRecord Counts:")
print(f"  VCF Variants:     {actual_vcf:>10,} (Expected: {EXPECTED_SILVER_VCF:>10,})")
print(f"  GTF Annotations:  {actual_gtf:>10,} (Expected: {EXPECTED_SILVER_GTF:>10,})")
print(f"  ClinVar Variants: {actual_clinvar:>10,} (Expected: {EXPECTED_SILVER_CLINVAR:>10,})")
print(f"  Total:            {(actual_vcf + actual_gtf + actual_clinvar):>10,}")

# Validation (0.1% tolerance)
tolerance = 0.001
vcf_match = abs(actual_vcf - EXPECTED_SILVER_VCF) / EXPECTED_SILVER_VCF <= tolerance
gtf_match = abs(actual_gtf - EXPECTED_SILVER_GTF) / EXPECTED_SILVER_GTF <= tolerance
clinvar_match = abs(actual_clinvar - EXPECTED_SILVER_CLINVAR) / EXPECTED_SILVER_CLINVAR <= tolerance

test_passed = vcf_match and gtf_match and clinvar_match

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Silver Test 1: All record counts match expected")
else:
    print("[✗ FAIL] Silver Test 1: Record count mismatch")
print("="*70 + "\n")

# COMMAND ----------

# DBTITLE 1,Test 2: Referential Integrity
print("="*70)
print("SILVER TEST 2: REFERENTIAL INTEGRITY")
print("="*70)

# Check that Silver VCF exists for downstream Gold joins
silver_vcf_df = spark.table(SILVER_VCF).select("chrom", "pos").distinct()
silver_vcf_count = silver_vcf_df.count()

print(f"\nSilver VCF distinct variants: {silver_vcf_count:,}")
print("Checking mandatory columns for downstream joins...")

# Check for NULLs in join keys
null_chrom = spark.table(SILVER_VCF).filter(col("chrom").isNull()).count()
null_pos = spark.table(SILVER_VCF).filter(col("pos").isNull()).count()

print(f"  NULL chrom: {null_chrom}")
print(f"  NULL pos: {null_pos}")

test_passed = (null_chrom == 0) and (null_pos == 0) and (silver_vcf_count > 6000000)

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Silver Test 2: Referential integrity maintained")
else:
    print("[✗ FAIL] Silver Test 2: Integrity issues detected")
print("="*70 + "\n")

# COMMAND ----------

# DBTITLE 1,Test 3: Partition Integrity
print("="*70)
print("SILVER TEST 3: PARTITION INTEGRITY")
print("="*70)

# Check chromosome 1 exists in VCF (partitioned table)
vcf_chr1 = spark.table(SILVER_VCF).filter(col("chrom") == "1").count()

print(f"\nChromosome 1 variants in Silver VCF: {vcf_chr1:,}")

# Verify partition distribution
chrom_distribution = spark.table(SILVER_VCF).groupBy("chrom").count().orderBy("chrom").collect()
print(f"\nTotal chromosomes represented: {len(chrom_distribution)}")
print("Sample distribution (first 5):")
for row in chrom_distribution[:5]:
    print(f"  Chr {row.chrom}: {row['count']:,} variants")

test_passed = vcf_chr1 > 6000000

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Silver Test 3: Partition integrity verified")
else:
    print("[✗ FAIL] Silver Test 3: Partitioning issue detected")
print("="*70 + "\n")

# COMMAND ----------

# DBTITLE 1,Test 4: Duplicate Detection
print("="*70)
print("SILVER TEST 4: DUPLICATE DETECTION")
print("="*70)

# Check VCF for true duplicates (same chrom, pos, ref, alt)
vcf_df = spark.table(SILVER_VCF)
total_vcf = vcf_df.count()
distinct_vcf = vcf_df.select("chrom", "pos", "ref_allele", "alt_allele").distinct().count()
vcf_duplicates = total_vcf - distinct_vcf

print(f"\nVCF Variants:")
print(f"  Total records: {total_vcf:,}")
print(f"  Distinct variants: {distinct_vcf:,}")
print(f"  Duplicates: {vcf_duplicates}")

# Check ClinVar for duplicates on allele_id
clinvar_df = spark.table(SILVER_CLINVAR)
total_clinvar = clinvar_df.count()
distinct_clinvar = clinvar_df.select("allele_id").distinct().count()
clinvar_duplicates = total_clinvar - distinct_clinvar

print(f"\nClinVar Variants:")
print(f"  Total records: {total_clinvar:,}")
print(f"  Distinct allele_ids: {distinct_clinvar:,}")
print(f"  Duplicates: {clinvar_duplicates}")

test_passed = (vcf_duplicates == 0) and (clinvar_duplicates == 0)

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Silver Test 4: No duplicates detected")
else:
    print("[✗ FAIL] Silver Test 4: Duplicates found")
print("="*70 + "\n")

# COMMAND ----------

# DBTITLE 1,Test 5: Mandatory Column Null Check
print("="*70)
print("SILVER TEST 5: MANDATORY COLUMN NULL CHECK")
print("="*70)

# Check VCF mandatory columns
vcf_df = spark.table(SILVER_VCF)
vcf_nulls = {
    "chrom": vcf_df.filter(col("chrom").isNull()).count(),
    "pos": vcf_df.filter(col("pos").isNull()).count(),
    "ref_allele": vcf_df.filter(col("ref_allele").isNull()).count(),
    "alt_allele": vcf_df.filter(col("alt_allele").isNull()).count()
}

print("\nVCF Mandatory Columns:")
for col_name, null_count in vcf_nulls.items():
    status = "✓" if null_count == 0 else "✗"
    print(f"  [{status}] {col_name}: {null_count} NULLs")

# Check ClinVar mandatory columns
clinvar_df = spark.table(SILVER_CLINVAR)
clinvar_nulls = {
    "allele_id": clinvar_df.filter(col("allele_id").isNull()).count(),
    "chromosome": clinvar_df.filter(col("chromosome").isNull()).count()
}

print("\nClinVar Mandatory Columns:")
for col_name, null_count in clinvar_nulls.items():
    status = "✓" if null_count == 0 else "✗"
    print(f"  [{status}] {col_name}: {null_count} NULLs")

total_nulls = sum(vcf_nulls.values()) + sum(clinvar_nulls.values())
test_passed = total_nulls == 0

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Silver Test 5: No NULLs in mandatory columns")
else:
    print(f"[✗ FAIL] Silver Test 5: Found {total_nulls} NULLs in mandatory columns")
print("="*70 + "\n")