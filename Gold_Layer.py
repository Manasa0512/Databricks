# Databricks notebook source
# DBTITLE 1,Gold Layer Overview
# MAGIC %md
# MAGIC # Gold Layer: Analytics-Ready Genomics Intelligence
# MAGIC
# MAGIC ## Simple Gold Layer with 3 Tables
# MAGIC
# MAGIC ```
# MAGIC Silver Layer                    Gold Layer (Analytics-Ready)
# MAGIC ├─ silver_vcf_variants    →    ├─ gold_variant_summary
# MAGIC ├─ silver_gene_annotations →   ├─ gold_clinical_significance  
# MAGIC └─ silver_clinical_variants →  └─ gold_gene_hotspots
# MAGIC ```
# MAGIC
# MAGIC ### Gold Tables:
# MAGIC
# MAGIC 1. **gold_variant_summary**: Complete variant profile (VCF + Gene + ClinVar)
# MAGIC 2. **gold_clinical_significance**: Clinical pathogenicity aggregations
# MAGIC 3. **gold_gene_hotspots**: Gene-level variant burden rankings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Storage
# MAGIC - **Format**: Delta Lake
# MAGIC - **Write Mode**: Overwrite
# MAGIC - **Partitioning**: By chromosome

# COMMAND ----------

# DBTITLE 1,Pipeline Pseudocode
# MAGIC %md
# MAGIC # Gold Layer Pseudocode
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   START Gold Layer Analytics
# MAGIC   
# MAGIC   STEP 1: Load Silver tables
# MAGIC   {
# MAGIC     LOAD silver_vcf_variants (6.4M variants)
# MAGIC     LOAD silver_gene_annotations (filter to feature='gene' only → 78K genes)
# MAGIC     LOAD silver_clinical_variants (4.5M clinical annotations)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 2: Create gold_variant_summary (Complete variant profile)
# MAGIC   {
# MAGIC     JOIN vcf with genes (range join)
# MAGIC     {
# MAGIC       CONDITION: vcf.chrom = gene.seqname AND vcf.pos BETWEEN gene.start_pos AND gene.end_pos
# MAGIC       RESULT: Map variants to their containing genes
# MAGIC     }
# MAGIC     
# MAGIC     JOIN result with clinical_variants (position join)
# MAGIC     {
# MAGIC       CONDITION: chrom = chromosome AND pos = start_pos
# MAGIC       RESULT: Add clinical annotations to variants
# MAGIC     }
# MAGIC     
# MAGIC     SELECT final columns (chrom, pos, ref, alt, variant_type, gene_id, gene_name, gene_type, clinical_significance, quality)
# MAGIC     WRITE to gold_variant_summary (partitioned by chrom)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 3: Create gold_clinical_significance (Pathogenicity aggregations)
# MAGIC   {
# MAGIC     SOURCE: gold_variant_summary WHERE clinical_significance IS NOT NULL
# MAGIC     
# MAGIC     AGGREGATE by clinical_significance
# MAGIC     {
# MAGIC       COUNT total_variants
# MAGIC       COUNT DISTINCT unique_genes
# MAGIC       COUNT DISTINCT unique_positions
# MAGIC       COMPUTE avg_quality
# MAGIC     }
# MAGIC     
# MAGIC     AGGREGATE by gene + clinical_significance
# MAGIC     {
# MAGIC       COUNT variants per gene per significance level
# MAGIC       RANK genes by pathogenic variant burden
# MAGIC     }
# MAGIC     
# MAGIC     WRITE to gold_clinical_significance (partitioned by clinical_significance)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 4: Create gold_gene_hotspots (Gene variant burden)
# MAGIC   {
# MAGIC     SOURCE: gold_variant_summary WHERE gene_id IS NOT NULL
# MAGIC     
# MAGIC     GROUP BY gene_id, gene_name, gene_type
# MAGIC     {
# MAGIC       COUNT total_variants per gene
# MAGIC       COUNT unique_variant_types per gene
# MAGIC       COMPUTE avg_quality per gene
# MAGIC       RANK genes by variant count (dense_rank)
# MAGIC     }
# MAGIC     
# MAGIC     ORDER BY total_variants DESC
# MAGIC     WRITE to gold_gene_hotspots (no partitioning - small table)
# MAGIC   }
# MAGIC   
# MAGIC   STEP 5: Verification
# MAGIC   {
# MAGIC     VERIFY all 3 gold tables exist
# MAGIC     COUNT records in each table
# MAGIC     SHOW sample data
# MAGIC     DISPLAY summary statistics
# MAGIC   }
# MAGIC   
# MAGIC   END Gold Layer Analytics
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Configuration & Imports
# ============================================================================
# GOLD LAYER CONFIGURATION
# ============================================================================

from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    when, lit, concat_ws, current_timestamp, broadcast, 
    countDistinct, dense_rank, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType

# Silver table names
SILVER_VCF = "workspace.genomics_project.silver_vcf_variants"
SILVER_GTF = "workspace.genomics_project.silver_gene_annotations"
SILVER_CLINVAR = "workspace.genomics_project.silver_clinical_variants"

# Gold table names (3 tables only)
GOLD_VARIANT_SUMMARY = "workspace.genomics_project.gold_variant_summary"
GOLD_CLINICAL_SIG = "workspace.genomics_project.gold_clinical_significance"
GOLD_GENE_HOTSPOTS = "workspace.genomics_project.gold_gene_hotspots"

print("Gold Layer Configuration Loaded")
print(f"   Silver sources: 3 tables")
print(f"   Gold targets: 3 tables")

# COMMAND ----------

# DBTITLE 1,Section 1: Genomic Coordinate Join Strategy
# MAGIC %md
# MAGIC # Genomic Join Strategy
# MAGIC
# MAGIC ## VCF ↔ GTF (Range Join)
# MAGIC Find which gene contains each variant:
# MAGIC ```sql
# MAGIC vcf.chrom = gtf.seqname
# MAGIC AND vcf.pos BETWEEN gtf.start_pos AND gtf.end_pos
# MAGIC ```
# MAGIC
# MAGIC ## VCF ↔ ClinVar (Position Join)
# MAGIC Add clinical annotations:
# MAGIC ```sql
# MAGIC vcf.chrom = clinvar.chromosome
# MAGIC AND vcf.pos = clinvar.start_pos
# MAGIC ```
# MAGIC
# MAGIC **Note**: Position-based join (not allele-based) because ClinVar has 99% 'na' values in alleles.

# COMMAND ----------

# DBTITLE 1,Load Silver Tables
# ============================================================================
# STEP 1: Load Silver Tables with Optimization
# ============================================================================

print("Loading Silver Tables...")
print("="*70)

# Load VCF Variants
print("\n[1] Loading VCF Variants...")
vcf_silver = spark.table(SILVER_VCF)
vcf_count = vcf_silver.count()
print(f"   [OK] VCF: {vcf_count:,} variants loaded")

# Load Gene Annotations (filter to genes only for efficiency)
print("\n[2] Loading Gene Annotations (filtering to 'gene' features)...")
gtf_silver = spark.table(SILVER_GTF) \
    .filter(col("feature") == "gene") \
    .filter(col("gene_name").isNotNull()) \
    .filter(col("gene_name") != "")
gtf_count = gtf_silver.count()
print(f"   [OK] Genes: {gtf_count:,} gene records loaded (filtered from 5.8M annotations)")
print(f"   [INFO] Reduction: {5868512 - gtf_count:,} non-gene records filtered out")

# Load Clinical Variants
print("\n[3] Loading Clinical Variants...")
clinvar_silver = spark.table(SILVER_CLINVAR)
clinvar_count = clinvar_silver.count()
print(f"   [OK] ClinVar: {clinvar_count:,} clinical variants loaded")

print("\n" + "="*70)
print("[OK] All Silver tables loaded and ready for Gold transformations")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Gold Table 1: Variant Summary
# MAGIC %md
# MAGIC # Gold Table 1: gold_variant_summary
# MAGIC
# MAGIC **Complete variant profile** with genomic coordinates, gene annotation, clinical significance, quality metrics, and **population frequencies**.
# MAGIC
# MAGIC **New in this version**: Added single `population_frequencies` struct column containing regional frequencies from 1000 Genomes Project Phase 3:
# MAGIC - `african` - African populations (AFR_AF)
# MAGIC - `american` - American/Latino populations (AMR_AF)
# MAGIC - `east_asian` - East Asian populations (EAS_AF)
# MAGIC - `european` - European populations (EUR_AF)
# MAGIC - `south_asian` - South Asian populations (SAS_AF)
# MAGIC - `global` - Global allele frequency (AF)
# MAGIC
# MAGIC **Data source**: 2,504 individuals across 5 super-populations
# MAGIC
# MAGIC **Partitioned by**: chrom

# COMMAND ----------

# DBTITLE 1,Create Gold: Variant Summary
# ============================================================================
# GOLD TABLE 1: gold_variant_summary
# Complete variant profile with gene + clinical annotations + population frequencies
# ============================================================================

from pyspark.sql.functions import regexp_extract

print("GOLD TABLE 1: Variant Summary (Enriched with Population Frequencies)")
print("="*70)

try:
    # Step 0: Extract population frequencies from VCF INFO field as 6 separate columns
    print("\nStep 0: Extracting population frequencies from INFO field...")
    vcf_with_pop = vcf_silver \
        .withColumn("african_freq", regexp_extract(col("info"), r"AFR_AF=([0-9.]+)", 1).cast("double")) \
        .withColumn("american_freq", regexp_extract(col("info"), r"AMR_AF=([0-9.]+)", 1).cast("double")) \
        .withColumn("east_asian_freq", regexp_extract(col("info"), r"EAS_AF=([0-9.]+)", 1).cast("double")) \
        .withColumn("european_freq", regexp_extract(col("info"), r"EUR_AF=([0-9.]+)", 1).cast("double")) \
        .withColumn("south_asian_freq", regexp_extract(col("info"), r"SAS_AF=([0-9.]+)", 1).cast("double")) \
        .withColumn("global_freq", regexp_extract(col("info"), r"AF=([0-9.]+)", 1).cast("double"))
    print("   [OK] Population frequency columns added (6 separate columns: african_freq, american_freq, east_asian_freq, european_freq, south_asian_freq, global_freq)")
    
    # Step 1: VCF ↔ GTF Range Join (variant to gene mapping)
    print("\nStep 1: Joining VCF with Gene Annotations (range join)...")
    print("   Join condition: vcf.chrom = gene.seqname AND vcf.pos BETWEEN gene.start_pos AND gene.end_pos")
    
    # Normalize chromosome format for GTF join (VCF: "1" → "chr1" to match GTF: "chr1")
    vcf_normalized = vcf_with_pop.withColumn(
        "chrom_for_gtf", 
        when(col("chrom").startswith("chr"), col("chrom")).otherwise(concat_ws("", lit("chr"), col("chrom")))
    )
    
    # Range join: variant position falls within gene boundaries
    vcf_gene_join = vcf_normalized.alias("vcf").join(
        broadcast(gtf_silver.alias("gene")),  # Broadcast smaller gene table (~60K genes)
        (col("vcf.chrom_for_gtf") == col("gene.seqname")) &
        (col("vcf.pos") >= col("gene.start_pos")) &
        (col("vcf.pos") <= col("gene.end_pos")),
        "left"  # LEFT join to keep variants without gene annotation
    ).select(
        col("vcf.chrom"),  # Keep original format "1" for ClinVar join
        col("vcf.pos"),
        col("vcf.variant_id"),
        col("vcf.ref_allele"),
        col("vcf.alt_allele"),
        col("vcf.quality_score"),
        col("vcf.filter_status"),
        col("vcf.is_high_quality"),
        col("vcf.variant_type"),
        col("gene.gene_id"),
        col("gene.gene_name"),
        col("gene.gene_type"),
        col("gene.strand"),
        col("vcf.african_freq"),
        col("vcf.american_freq"),
        col("vcf.east_asian_freq"),
        col("vcf.european_freq"),
        col("vcf.south_asian_freq"),
        col("vcf.global_freq")
    )
    
    join1_count = vcf_gene_join.count()
    genes_found = vcf_gene_join.filter(col("gene_name").isNotNull()).count()
    print(f"   [OK] Range join complete: {join1_count:,} variants processed")
    print(f"   [INFO] Variants mapped to genes: {genes_found:,} ({(genes_found/join1_count*100):.1f}%)")
    
    # Step 2: Result ↔ ClinVar Position-based Join
    print("\nStep 2: Joining with Clinical Variants (position-based)...")
    print("   Join condition: chrom + pos match")
    print("   Note: ClinVar ref/alt alleles are 99% 'na' - using position-based join instead")
    
    # Use position-based join since ClinVar alleles are mostly 'na'
    variant_summary = vcf_gene_join.alias("v").join(
        clinvar_silver.alias("c"),
        (col("v.chrom") == col("c.chromosome")) &  # Both are "1" format
        (col("v.pos") == col("c.start_pos")),
        "left"  # LEFT join to keep all variants
    ).select(
        # Genomic coordinates
        col("v.chrom"),
        col("v.pos"),
        col("v.variant_id"),
        col("v.ref_allele"),
        col("v.alt_allele"),
        
        # Gene information
        col("v.gene_id"),
        col("v.gene_name"),
        col("v.gene_type"),
        col("v.strand"),
        
        # Clinical information
        col("c.allele_id").alias("clinvar_allele_id"),
        col("c.clinical_significance"),
        col("c.review_status"),
        col("c.phenotype_list"),
        col("c.gene_symbol").alias("clinvar_gene_symbol"),
        
        # Quality metrics
        col("v.quality_score"),
        col("v.is_high_quality"),
        col("v.variant_type"),
        col("v.filter_status"),
        
        # Population frequencies (6 separate columns - 1000 Genomes Project Phase 3)
        col("v.african_freq"),
        col("v.american_freq"),
        col("v.east_asian_freq"),
        col("v.european_freq"),
        col("v.south_asian_freq"),
        col("v.global_freq"),
        
        # Flags
        when(col("c.allele_id").isNotNull(), lit(True)).otherwise(lit(False)).alias("has_clinical_annotation"),
        when(col("v.gene_name").isNotNull(), lit(True)).otherwise(lit(False)).alias("has_gene_annotation"),
        
        # Metadata
        current_timestamp().alias("gold_processing_timestamp")
    )
    
    clinical_matched = variant_summary.filter(col("has_clinical_annotation") == True).count()
    print(f"   [OK] Clinical match complete: {clinical_matched:,} variants have clinical annotations")
    print(f"   [INFO] Match rate: {(clinical_matched/join1_count*100):.2f}%")
    
    # Step 3: Write to Gold Delta table
    print("\nStep 3: Writing to Gold Delta table...")
    variant_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("chrom") \
        .option("overwriteSchema", "true") \
        .saveAsTable(GOLD_VARIANT_SUMMARY)
    
    # Step 4: Optimize with Z-ORDER
    print("\nStep 4: Optimizing with Z-ORDER...")
    spark.sql(f"OPTIMIZE {GOLD_VARIANT_SUMMARY} ZORDER BY (gene_name, clinical_significance)")
    
    final_count = spark.table(GOLD_VARIANT_SUMMARY).count()
    gene_annotated_count = spark.table(GOLD_VARIANT_SUMMARY).filter(col("has_gene_annotation") == True).count()
    clinical_annotated_count = spark.table(GOLD_VARIANT_SUMMARY).filter(col("has_clinical_annotation") == True).count()
    
    print(f"\n[SUCCESS] gold_variant_summary created with {final_count:,} variants")
    print(f"   [INFO] Gene annotations: {gene_annotated_count:,} variants ({(gene_annotated_count/final_count*100):.1f}%)")
    print(f"   [INFO] Clinical annotations: {clinical_annotated_count:,} variants ({(clinical_annotated_count/final_count*100):.2f}%)")
    print(f"   [INFO] Population frequencies: 6 separate columns (african_freq, american_freq, east_asian_freq, european_freq, south_asian_freq, global_freq)")
    print(f"   [INFO] Total columns: 27")
    print(f"   [INFO] Partitioned by: chrom")
    print(f"   [INFO] Optimized with: Z-ORDER (gene_name, clinical_significance)")
    print(f"\n[NOTE] ClinVar join is position-based (chrom+pos) due to 99% 'na' values in ref/alt allele columns")
    print(f"[NOTE] Population frequencies from 1000 Genomes Project Phase 3 (2,504 individuals)")
    print("="*70)
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Gold Table 3: Clinical Significance
# MAGIC %md
# MAGIC # Gold Table 2: gold_clinical_significance
# MAGIC
# MAGIC **Clinical pathogenicity aggregations** by clinical significance category and by gene.
# MAGIC
# MAGIC **Partitioned by**: clinical_significance

# COMMAND ----------

# DBTITLE 1,Create Gold: Clinical Significance
# ============================================================================
# GOLD TABLE 3: gold_clinical_significance
# Clinical pathogenicity aggregations
# ============================================================================

# Imports (inline for job execution)
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, when, lit, 
    countDistinct, dense_rank, round as spark_round, current_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType

# Table names (inline for job execution)
GOLD_VARIANT_SUMMARY = "workspace.genomics_project.gold_variant_summary"
GOLD_CLINICAL_SIG = "workspace.genomics_project.gold_clinical_significance"

print("GOLD TABLE 3: Clinical Significance (Aggregation)")
print("="*70)

try:
    print("\nAggregating clinical significance metrics...")
    
    # Read variant summary and filter to clinically annotated variants only
    variant_summary_df = spark.table(GOLD_VARIANT_SUMMARY)
    clinical_variants = variant_summary_df.filter(col("has_clinical_annotation") == True)
    
    clinical_count = clinical_variants.count()
    print(f"   [OK] Processing {clinical_count:,} clinically annotated variants")
    
    # Aggregation 1: By Clinical Significance Category
    print("\nAggregation 1: By Clinical Significance Category...")
    by_significance = clinical_variants.groupBy("clinical_significance").agg(
        count("*").alias("variant_count"),
        countDistinct("gene_name").alias("unique_genes"),
        countDistinct("chrom").alias("chromosomes_affected"),
        _sum(when(col("variant_type") == "SNP", 1).otherwise(0)).alias("snp_count"),
        _sum(when(col("variant_type") == "INSERTION", 1).otherwise(0)).alias("insertion_count"),
        _sum(when(col("variant_type") == "DELETION", 1).otherwise(0)).alias("deletion_count"),
        spark_round(avg("quality_score"), 2).alias("avg_quality_score")
    ).withColumn(
        "pct_of_clinical_variants",
        spark_round((col("variant_count") / lit(clinical_count) * 100), 2)
    )
    
    # Aggregation 2: By Gene + Clinical Significance (top genes per category)
    print("\nAggregation 2: By Gene + Clinical Significance...")
    by_gene_significance = clinical_variants.groupBy("gene_name", "clinical_significance").agg(
        count("*").alias("variant_count"),
        countDistinct("pos").alias("unique_positions")
    ).filter(col("gene_name").isNotNull())
    
    # Add rank within each clinical significance category
    window_spec = Window.partitionBy("clinical_significance").orderBy(col("variant_count").desc())
    by_gene_significance_ranked = by_gene_significance.withColumn(
        "rank_in_category",
        dense_rank().over(window_spec)
    ).filter(col("rank_in_category") <= 100)  # Top 100 genes per category
    
    # Combine both aggregations
    clinical_sig_final = by_significance.withColumn(
        "aggregation_type",
        lit("by_clinical_significance")
    ).withColumn(
        "gene_name",
        lit(None).cast(StringType())
    ).withColumn(
        "rank_in_category",
        lit(None).cast(IntegerType())
    ).withColumn(
        "unique_positions",
        lit(None).cast(LongType())
    ).select(
        "aggregation_type",
        "clinical_significance",
        "gene_name",
        "variant_count",
        "unique_genes",
        "chromosomes_affected",
        "snp_count",
        "insertion_count",
        "deletion_count",
        "avg_quality_score",
        "pct_of_clinical_variants",
        "rank_in_category",
        "unique_positions"
    ).union(
        by_gene_significance_ranked.withColumn(
            "aggregation_type",
            lit("by_gene_and_significance")
        ).withColumn(
            "unique_genes",
            lit(None).cast(LongType())
        ).withColumn(
            "chromosomes_affected",
            lit(None).cast(LongType())
        ).withColumn(
            "snp_count",
            lit(None).cast(LongType())
        ).withColumn(
            "insertion_count",
            lit(None).cast(LongType())
        ).withColumn(
            "deletion_count",
            lit(None).cast(LongType())
        ).withColumn(
            "avg_quality_score",
            lit(None).cast(DoubleType())
        ).withColumn(
            "pct_of_clinical_variants",
            lit(None).cast(DoubleType())
        ).select(
            "aggregation_type",
            "clinical_significance",
            "gene_name",
            "variant_count",
            "unique_genes",
            "chromosomes_affected",
            "snp_count",
            "insertion_count",
            "deletion_count",
            "avg_quality_score",
            "pct_of_clinical_variants",
            "rank_in_category",
            "unique_positions"
        )
    ).withColumn(
        "processing_timestamp",
        current_timestamp()
    )
    
    # Write to Gold
    print("\nWriting to Gold Delta table...")
    clinical_sig_final.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("clinical_significance") \
        .option("overwriteSchema", "true") \
        .saveAsTable(GOLD_CLINICAL_SIG)
    
    final_count = spark.table(GOLD_CLINICAL_SIG).count()
    print(f"\n[SUCCESS] gold_clinical_significance created with {final_count:,} records")
    print("\nClinical Significance Distribution:")
    spark.table(GOLD_CLINICAL_SIG).filter(
        col("aggregation_type") == "by_clinical_significance"
    ).select(
        "clinical_significance", "variant_count", "unique_genes", "pct_of_clinical_variants"
    ).orderBy(col("variant_count").desc()).show(10, truncate=False)
    print("="*70)
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Gold Table 4: Gene Hotspots
# MAGIC %md
# MAGIC # Gold Table 3: gold_gene_hotspots
# MAGIC
# MAGIC **Gene-level variant burden** showing which genes have the most variants.
# MAGIC
# MAGIC **No partitioning** (small table with ~7K genes)

# COMMAND ----------

# DBTITLE 1,Create Gold: Gene Hotspots
# ============================================================================
# GOLD TABLE 3: gold_gene_hotspots
# Gene-level variant burden analysis
# ============================================================================

# Imports (inline for job execution)
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, when, 
    round as spark_round
)

# Table names (inline for job execution)
GOLD_VARIANT_SUMMARY = "workspace.genomics_project.gold_variant_summary"
GOLD_GENE_HOTSPOTS = "workspace.genomics_project.gold_gene_hotspots"

print("GOLD TABLE 3: Gene Hotspots (Variant Burden)")
print("="*70)

try:
    print("\nAggregating variant counts by gene...")
    
    # Load variant summary and filter to gene-annotated variants
    variant_summary_df = spark.table(GOLD_VARIANT_SUMMARY)
    gene_variants = variant_summary_df.filter(col("has_gene_annotation") == True)
    
    # Aggregate by gene
    gene_hotspots = gene_variants.groupBy("gene_name").agg(
        count("*").alias("total_variants"),
        _sum(when(col("variant_type") == "SNP", 1).otherwise(0)).alias("snp_count"),
        _sum(when(col("variant_type") == "INSERTION", 1).otherwise(0)).alias("insertion_count"),
        _sum(when(col("variant_type") == "DELETION", 1).otherwise(0)).alias("deletion_count"),
        _sum(when(col("has_clinical_annotation") == True, 1).otherwise(0)).alias("clinical_variants"),
        spark_round(avg("quality_score"), 2).alias("avg_quality_score")
    ).orderBy(col("total_variants").desc())
    
    # Write to Gold
    print("\nWriting to Gold Delta table...")
    gene_hotspots.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(GOLD_GENE_HOTSPOTS)
    
    final_count = spark.table(GOLD_GENE_HOTSPOTS).count()
    print(f"\n[SUCCESS] gold_gene_hotspots created with {final_count:,} genes")
    print("\nTop 10 Gene Hotspots:")
    spark.table(GOLD_GENE_HOTSPOTS).select(
        "gene_name", "total_variants", "snp_count", "clinical_variants"
    ).show(10, truncate=False)
    print("="*70)
    
except Exception as e:
    print(f"\n[ERROR] {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Final Summary
# ============================================================================
# FINAL SUMMARY
# ============================================================================

# Table names (inline for job execution)
GOLD_VARIANT_SUMMARY = "workspace.genomics_project.gold_variant_summary"
GOLD_CLINICAL_SIG = "workspace.genomics_project.gold_clinical_significance"
GOLD_GENE_HOTSPOTS = "workspace.genomics_project.gold_gene_hotspots"

print("\n" + "="*70)
print("GOLD LAYER PIPELINE COMPLETE!")
print("="*70)

print("\nGOLD TABLES CREATED (3 tables):\n")

gold_tables = [
    ("1", "gold_variant_summary", GOLD_VARIANT_SUMMARY),
    ("2", "gold_clinical_significance", GOLD_CLINICAL_SIG),
    ("3", "gold_gene_hotspots", GOLD_GENE_HOTSPOTS)
]

for num, name, full_name in gold_tables:
    count = spark.table(full_name).count()
    print(f"{num}. {name}")
    print(f"   Records: {count:,}")
    print()

print("="*70)
print("[SUCCESS] Gold Layer is ready for analysis.")
print("="*70)

# COMMAND ----------

# DBTITLE 1,ETL Tests: Gold Layer Analytics Validation
# MAGIC %md
# MAGIC ## ETL Tests: Gold Layer Analytics Validation
# MAGIC
# MAGIC Validating analytics quality, join accuracy, and aggregations:
# MAGIC 1. **Record Count Validation** - Verify join expansion is correct
# MAGIC 2. **Join Accuracy** - VCF→GTF (77.7%) and VCF→ClinVar (0.99%)
# MAGIC 3. **Aggregation Accuracy** - Gene hotspots match variant counts
# MAGIC 4. **Referential Integrity** - Gold variants traceable to Silver
# MAGIC 5. **Partition Integrity** - Chromosome partitioning verified

# COMMAND ----------

# DBTITLE 1,Test 1: Record Count Validation
print("\n" + "="*70)
print("GOLD TEST 1: RECORD COUNT VALIDATION")
print("="*70)

# Expected values (updated May 29, 2026 after adding population frequency columns)
EXPECTED_GOLD_VARIANT_SUMMARY = 7405220
EXPECTED_GOLD_CLINICAL_SIG = 7323
EXPECTED_GOLD_GENE_HOTSPOTS = 6722

# Actual counts
actual_variant_summary = spark.table(GOLD_VARIANT_SUMMARY).count()
actual_clinical_sig = spark.table(GOLD_CLINICAL_SIG).count()
actual_gene_hotspots = spark.table(GOLD_GENE_HOTSPOTS).count()

print("\nGold Layer Record Counts:")
print(f"  Variant Summary:        {actual_variant_summary:>10,} (Expected: {EXPECTED_GOLD_VARIANT_SUMMARY:>10,})")
print(f"  Clinical Significance:  {actual_clinical_sig:>10,} (Expected: {EXPECTED_GOLD_CLINICAL_SIG:>10,})")
print(f"  Gene Hotspots:          {actual_gene_hotspots:>10,} (Expected: {EXPECTED_GOLD_GENE_HOTSPOTS:>10,})")

# Validation (0.5% tolerance for Gold due to join variations)
tolerance = 0.005
variant_match = abs(actual_variant_summary - EXPECTED_GOLD_VARIANT_SUMMARY) / EXPECTED_GOLD_VARIANT_SUMMARY <= tolerance
clinical_match = abs(actual_clinical_sig - EXPECTED_GOLD_CLINICAL_SIG) / EXPECTED_GOLD_CLINICAL_SIG <= tolerance
hotspots_match = abs(actual_gene_hotspots - EXPECTED_GOLD_GENE_HOTSPOTS) / EXPECTED_GOLD_GENE_HOTSPOTS <= tolerance

test_passed = variant_match and clinical_match and hotspots_match

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Gold Test 1: All record counts match expected")
else:
    print("[✗ FAIL] Gold Test 1: Record count mismatch")
print("="*70 + "\n")

# COMMAND ----------

# DBTITLE 1,Test 2: Join Accuracy
print("="*70)
print("GOLD TEST 2: JOIN ACCURACY")
print("="*70)

# Test VCF → GTF join (gene annotation rate)
gold_df = spark.table(GOLD_VARIANT_SUMMARY)
total_variants = gold_df.count()
variants_with_genes = gold_df.filter(col("has_gene_annotation") == True).count()
gene_mapping_rate = (variants_with_genes / total_variants) * 100

print("\nVCF → GTF Join (Range Join):")
print(f"  Total variants: {total_variants:,}")
print(f"  With gene annotation: {variants_with_genes:,}")
print(f"  Mapping rate: {gene_mapping_rate:.2f}%")
print(f"  Expected: 77.7%")

# Test VCF → ClinVar join (clinical annotation rate)
variants_with_clinical = gold_df.filter(col("has_clinical_annotation") == True).count()
clinical_rate = (variants_with_clinical / total_variants) * 100

print("\nVCF → ClinVar Join (Position Join):")
print(f"  Total variants: {total_variants:,}")
print(f"  With clinical annotation: {variants_with_clinical:,}")
print(f"  Annotation rate: {clinical_rate:.2f}%")
print(f"  Expected: 0.99%")

# Validation
gene_join_ok = 77.0 <= gene_mapping_rate <= 78.5
clinical_join_ok = 0.9 <= clinical_rate <= 1.1

test_passed = gene_join_ok and clinical_join_ok

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Gold Test 2: Join accuracy within expected ranges")
else:
    print("[✗ FAIL] Gold Test 2: Join accuracy outside expected ranges")
    if not gene_join_ok:
        print(f"  - Gene mapping rate {gene_mapping_rate:.2f}% not in [77.0%, 78.5%]")
    if not clinical_join_ok:
        print(f"  - Clinical rate {clinical_rate:.2f}% not in [0.9%, 1.1%]")
print("="*70 + "\n")

# COMMAND ----------

# DBTITLE 1,Test 3: Aggregation Accuracy
print("="*70)
print("GOLD TEST 3: AGGREGATION ACCURACY")
print("="*70)

# Check that gene hotspots aggregation matches source data
gold_variant = spark.table(GOLD_VARIANT_SUMMARY)
gold_hotspots = spark.table(GOLD_GENE_HOTSPOTS)

variants_with_genes = gold_variant.filter(col("gene_name").isNotNull()).count()
hotspots_sum = gold_hotspots.agg(_sum("total_variants")).collect()[0][0]

print("\nGene Hotspots Aggregation:")
print(f"  Variants with genes (source): {variants_with_genes:,}")
print(f"  Hotspots sum (aggregated):    {hotspots_sum:,}")
print(f"  Difference:                   {abs(variants_with_genes - hotspots_sum):,}")

# Note: Small differences expected due to NULL gene handling
tolerance = 100  # Allow small variance
test_passed = abs(variants_with_genes - hotspots_sum) <= tolerance

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Gold Test 3: Aggregation matches source data")
else:
    print(f"[✗ FAIL] Gold Test 3: Aggregation mismatch exceeds tolerance ({tolerance})")
print("="*70 + "\n")

# COMMAND ----------

# DBTITLE 1,Test 4: Referential Integrity
print("="*70)
print("GOLD TEST 4: REFERENTIAL INTEGRITY")
print("="*70)

# Check that all Gold variants exist in Silver VCF
gold_df = spark.table(GOLD_VARIANT_SUMMARY).select("chrom", "pos").distinct()
silver_vcf = spark.table(SILVER_VCF).select("chrom", "pos").distinct()

gold_count = gold_df.count()
matched = gold_df.join(silver_vcf, ["chrom", "pos"], "inner").count()
match_rate = (matched / gold_count) * 100

print("\nReferential Integrity Check:")
print(f"  Gold unique variants: {gold_count:,}")
print(f"  Matched in Silver VCF: {matched:,}")
print(f"  Match rate: {match_rate:.2f}%")

test_passed = match_rate >= 99.9

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Gold Test 4: All variants traceable to Silver")
else:
    print(f"[✗ FAIL] Gold Test 4: Match rate {match_rate:.2f}% below 99.9%")
print("="*70 + "\n")

# COMMAND ----------

# DBTITLE 1,Test 5: Partition Integrity
print("="*70)
print("GOLD TEST 5: PARTITION INTEGRITY")
print("="*70)

# Check chromosome 1 exists and partitions are complete
gold_df = spark.table(GOLD_VARIANT_SUMMARY)
chr1_count = gold_df.filter(col("chrom") == "1").count()

print("\nPartition Validation:")
print(f"  Chromosome 1 variants: {chr1_count:,}")

# Check partition distribution
chrom_distribution = gold_df.groupBy("chrom").count().orderBy("chrom").collect()
print(f"\nTotal chromosomes: {len(chrom_distribution)}")
print("Sample distribution (first 5):")
for row in chrom_distribution[:5]:
    print(f"  Chr {row.chrom}: {row['count']:,} variants")

# Verify chr1 has majority of variants (chr1 is the only chromosome in dataset)
test_passed = chr1_count == gold_df.count()

print("\n" + "="*70)
if test_passed:
    print("[✓ PASS] Gold Test 5: Partition integrity verified")
else:
    print(f"[✗ FAIL] Gold Test 5: Partitioning issue detected")
print("="*70 + "\n")

# COMMAND ----------

