# Databricks notebook source
# DBTITLE 1,Gold Layer Overview
# MAGIC %md
# MAGIC # Gold Layer: Analytics-Ready Genomics Intelligence
# MAGIC
# MAGIC ## 🎯 Simple Gold Layer with 3 Tables
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
# MAGIC ## 💾 Storage
# MAGIC - **Format**: Delta Lake
# MAGIC - **Write Mode**: Overwrite
# MAGIC - **Partitioning**: By chromosome

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

print("✅ Gold Layer Configuration Loaded")
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

print("📂 Loading Silver Tables...")
print("="*70)

# Load VCF Variants
print("\n1️⃣ Loading VCF Variants...")
vcf_silver = spark.table(SILVER_VCF)
vcf_count = vcf_silver.count()
print(f"   ✅ VCF: {vcf_count:,} variants loaded")

# Load Gene Annotations (filter to genes only for efficiency)
print("\n2️⃣ Loading Gene Annotations (filtering to 'gene' features)...")
gtf_silver = spark.table(SILVER_GTF) \
    .filter(col("feature") == "gene") \
    .filter(col("gene_name").isNotNull()) \
    .filter(col("gene_name") != "")
gtf_count = gtf_silver.count()
print(f"   ✅ Genes: {gtf_count:,} gene records loaded (filtered from 5.8M annotations)")
print(f"   📊 Reduction: {5868512 - gtf_count:,} non-gene records filtered out")

# Load Clinical Variants
print("\n3️⃣ Loading Clinical Variants...")
clinvar_silver = spark.table(SILVER_CLINVAR)
clinvar_count = clinvar_silver.count()
print(f"   ✅ ClinVar: {clinvar_count:,} clinical variants loaded")

print("\n" + "="*70)
print("✅ All Silver tables loaded and ready for Gold transformations")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Gold Table 1: Variant Summary
# MAGIC %md
# MAGIC # Gold Table 1: gold_variant_summary
# MAGIC
# MAGIC **Complete variant profile** with genomic coordinates, gene annotation, clinical significance, and quality metrics.
# MAGIC
# MAGIC **Partitioned by**: chrom

# COMMAND ----------

# DBTITLE 1,Create Gold: Variant Summary
# ============================================================================
# GOLD TABLE 1: gold_variant_summary
# Complete variant profile with gene + clinical annotations
# ============================================================================

print("🧬 GOLD TABLE 1: Variant Summary (Enriched)")
print("="*70)

try:
    # Step 1: VCF ↔ GTF Range Join (variant to gene mapping)
    print("\n🔗 Step 1: Joining VCF with Gene Annotations (range join)...")
    print("   Join condition: vcf.chrom = gene.seqname AND vcf.pos BETWEEN gene.start_pos AND gene.end_pos")
    
    # Normalize chromosome format for GTF join (VCF: "1" → "chr1" to match GTF: "chr1")
    vcf_normalized = vcf_silver.withColumn(
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
        col("gene.strand")
    )
    
    join1_count = vcf_gene_join.count()
    genes_found = vcf_gene_join.filter(col("gene_name").isNotNull()).count()
    print(f"   ✅ Range join complete: {join1_count:,} variants processed")
    print(f"   🧬 Variants mapped to genes: {genes_found:,} ({(genes_found/join1_count*100):.1f}%)")
    
    # Step 2: Result ↔ ClinVar Position-based Join
    print("\n🔗 Step 2: Joining with Clinical Variants (position-based)...")
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
        
        # Flags
        when(col("c.allele_id").isNotNull(), lit(True)).otherwise(lit(False)).alias("has_clinical_annotation"),
        when(col("v.gene_name").isNotNull(), lit(True)).otherwise(lit(False)).alias("has_gene_annotation"),
        
        # Metadata
        current_timestamp().alias("gold_processing_timestamp")
    )
    
    clinical_matched = variant_summary.filter(col("has_clinical_annotation") == True).count()
    print(f"   ✅ Clinical match complete: {clinical_matched:,} variants have clinical annotations")
    print(f"   📊 Match rate: {(clinical_matched/join1_count*100):.2f}%")
    
    # Step 3: Write to Gold Delta table
    print("\n💾 Step 3: Writing to Gold Delta table...")
    variant_summary.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("chrom") \
        .option("overwriteSchema", "true") \
        .saveAsTable(GOLD_VARIANT_SUMMARY)
    
    # Step 4: Optimize with Z-ORDER
    print("\n⚡ Step 4: Optimizing with Z-ORDER...")
    spark.sql(f"OPTIMIZE {GOLD_VARIANT_SUMMARY} ZORDER BY (gene_name, clinical_significance)")
    
    final_count = spark.table(GOLD_VARIANT_SUMMARY).count()
    gene_annotated_count = spark.table(GOLD_VARIANT_SUMMARY).filter(col("has_gene_annotation") == True).count()
    clinical_annotated_count = spark.table(GOLD_VARIANT_SUMMARY).filter(col("has_clinical_annotation") == True).count()
    
    print(f"\n✅ SUCCESS: gold_variant_summary created with {final_count:,} variants")
    print(f"   📊 Gene annotations: {gene_annotated_count:,} variants ({(gene_annotated_count/final_count*100):.1f}%)")
    print(f"   🏥 Clinical annotations: {clinical_annotated_count:,} variants ({(clinical_annotated_count/final_count*100):.2f}%)")
    print(f"   📁 Partitioned by: chrom")
    print(f"   ⚡ Optimized with: Z-ORDER (gene_name, clinical_significance)")
    print(f"\n📌 Note: ClinVar join is position-based (chrom+pos) due to 99% 'na' values in ref/alt allele columns")
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
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

print("🏥 GOLD TABLE 3: Clinical Significance (Aggregation)")
print("="*70)

try:
    print("\n📈 Aggregating clinical significance metrics...")
    
    # Read variant summary and filter to clinically annotated variants only
    variant_summary_df = spark.table(GOLD_VARIANT_SUMMARY)
    clinical_variants = variant_summary_df.filter(col("has_clinical_annotation") == True)
    
    clinical_count = clinical_variants.count()
    print(f"   ✅ Processing {clinical_count:,} clinically annotated variants")
    
    # Aggregation 1: By Clinical Significance Category
    print("\n📊 Aggregation 1: By Clinical Significance Category...")
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
    print("\n🧬 Aggregation 2: By Gene + Clinical Significance...")
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
    print("\n💾 Writing to Gold Delta table...")
    clinical_sig_final.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("clinical_significance") \
        .option("overwriteSchema", "true") \
        .saveAsTable(GOLD_CLINICAL_SIG)
    
    final_count = spark.table(GOLD_CLINICAL_SIG).count()
    print(f"\n✅ SUCCESS: gold_clinical_significance created with {final_count:,} records")
    print("\n🏥 Clinical Significance Distribution:")
    spark.table(GOLD_CLINICAL_SIG).filter(
        col("aggregation_type") == "by_clinical_significance"
    ).select(
        "clinical_significance", "variant_count", "unique_genes", "pct_of_clinical_variants"
    ).orderBy(col("variant_count").desc()).show(10, truncate=False)
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
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

print("🧬 GOLD TABLE 3: Gene Hotspots (Variant Burden)")
print("="*70)

try:
    print("\n📊 Aggregating variant counts by gene...")
    
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
    print("\n💾 Writing to Gold Delta table...")
    gene_hotspots.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(GOLD_GENE_HOTSPOTS)
    
    final_count = spark.table(GOLD_GENE_HOTSPOTS).count()
    print(f"\n✅ SUCCESS: gold_gene_hotspots created with {final_count:,} genes")
    print("\n🔥 Top 10 Gene Hotspots:")
    spark.table(GOLD_GENE_HOTSPOTS).select(
        "gene_name", "total_variants", "snp_count", "clinical_variants"
    ).show(10, truncate=False)
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
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
print("🎉 GOLD LAYER PIPELINE COMPLETE!")
print("="*70)

print("\n📁 GOLD TABLES CREATED (3 tables):\n")

gold_tables = [
    ("1", "gold_variant_summary", GOLD_VARIANT_SUMMARY),
    ("2", "gold_clinical_significance", GOLD_CLINICAL_SIG),
    ("3", "gold_gene_hotspots", GOLD_GENE_HOTSPOTS)
]

for num, name, full_name in gold_tables:
    count = spark.table(full_name).count()
    print(f"{num}. {name}")
    print(f"   📊 Records: {count:,}")
    print()

print("="*70)
print("✅ SUCCESS! Gold Layer is ready for analysis.")
print("="*70)

# COMMAND ----------

