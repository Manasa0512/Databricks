# Genomics ETL Pipeline Diagram

## Updated Data Model

This document now reflects the latest Databricks pipeline design and presents:
- A refreshed medallion pipeline diagram
- Separate dimension table definitions
- Separate fact table definitions
- Clear mapping from Bronze ? Silver ? Gold

---

## Pipeline Flow

```mermaid
flowchart TD
    subgraph Bronze[Bronze Layer]
      B1[VCF Raw Text]
      B2[GTF Raw Text]
      B3[ClinVar Raw CSV]
      B1 -->|ingest + audit metadata| BRONZE_VCF[bronze_vcf_variants_raw]
      B2 -->|ingest + audit metadata| BRONZE_GTF[bronze_gene_annotations_raw]
      B3 -->|ingest + audit metadata| BRONZE_CLINVAR[bronze_clinical_variants_raw]
    end

    subgraph Silver[Silver Layer]
      S1[Parse VCF into structured columns]
      S2[Parse GTF into structured annotations]
      S3[Sanitize ClinVar + type cast]
      S1 --> SILVER_VCF[silver_vcf_variants]
      S2 --> SILVER_GTF[silver_gene_annotations]
      S3 --> SILVER_CLINVAR[silver_clinical_variants]
    end

    subgraph Gold[Gold Layer]
      D1[Create dimension tables]
      F1[Build fact table]
      A1[Generate analytics aggregates]
      D1 --> DIM_VARIANT[dim_variant]
      D1 --> DIM_GENE[dim_gene]
      D1 --> DIM_CLINVAR[dim_clinvar_annotation]
      S1 --> D1
      S2 --> D1
      S3 --> D1

      DIM_VARIANT --> FACT_VARIANT[fact_variant_annotation]
      DIM_GENE --> FACT_VARIANT
      DIM_CLINVAR --> FACT_VARIANT
      FACT_VARIANT --> A1
      A1 --> GOLD_CLINICAL[gold_clinical_significance]
      A1 --> GOLD_HOTSPOTS[gold_gene_hotspots]
    end

    BRONZE_VCF --> S1
    BRONZE_GTF --> S2
    BRONZE_CLINVAR --> S3

    style Bronze fill:#f8fafc,stroke:#94a3b8
    style Silver fill:#eff6ff,stroke:#2563eb
    style Gold fill:#ecfdf5,stroke:#15803d
    style BRONZE_VCF fill:#ffffff,stroke:#475569
    style BRONZE_GTF fill:#ffffff,stroke:#475569
    style BRONZE_CLINVAR fill:#ffffff,stroke:#475569
    style SILVER_VCF fill:#ffffff,stroke:#2563eb
    style SILVER_GTF fill:#ffffff,stroke:#2563eb
    style SILVER_CLINVAR fill:#ffffff,stroke:#2563eb
    style DIM_VARIANT fill:#f0fdf4,stroke:#15803d
    style DIM_GENE fill:#f0fdf4,stroke:#15803d
    style DIM_CLINVAR fill:#f0fdf4,stroke:#15803d
    style FACT_VARIANT fill:#ffffff,stroke:#15803d
    style GOLD_CLINICAL fill:#ffffff,stroke:#15803d
    style GOLD_HOTSPOTS fill:#ffffff,stroke:#15803d
```

---

## Key Notes
- Bronze layer preserves raw source data from VCF, GTF, and ClinVar.
- Silver layer produces typed, cleaned, and partitioned Delta tables.
- Gold layer separates dimensions from facts for analytics-ready modeling.
- The main fact table is `fact_variant_annotation`, supported by `dim_variant`, `dim_gene`, and `dim_clinvar_annotation`.

---

## Dimension Tables

### dim_variant
| Column | Type | Description |
|---|---|---|
| variant_key | string | Surrogate key for each unique variant |
| chrom | string | Chromosome code |
| pos | int | Variant genomic position |
| variant_id | string | VCF ID or generated identifier |
| ref_allele | string | Reference allele |
| alt_allele | string | Alternate allele |
| variant_type | string | SNP, INSERTION, DELETION, COMPLEX |
| quality_score | double | VCF QUAL score |
| filter_status | string | VCF FILTER field |
| is_high_quality | boolean | `filter_status == 'PASS'` |
| bronze_ingestion_timestamp | timestamp | Bronze ingestion metadata |
| source_file | string | Original source filename |

### dim_gene
| Column | Type | Description |
|---|---|---|
| gene_key | string | Surrogate key for each gene |
| gene_id | string | Gene identifier from GTF |
| gene_name | string | Standard gene symbol |
| gene_type | string | Gene biotype |
| seqname | string | Chromosome or contig |
| start_pos | int | Gene start coordinate |
| end_pos | int | Gene end coordinate |
| strand | string | `+` or `-` |
| length | int | End minus start + 1 |
| annotation_source | string | Source system, e.g. GENCODE v49 |

### dim_clinvar_annotation
| Column | Type | Description |
|---|---|---|
| clinvar_key | string | Surrogate key for ClinVar annotation |
| allele_id | int | ClinVar AlleleID |
| variation_id | int | ClinVar VariationID |
| chromosome | string | Chromosome code |
| start_pos | long | Variant start position |
| gene_symbol | string | Reported gene symbol |
| gene_id | int | Gene ID if available |
| clinical_significance | string | Pathogenicity label |
| review_status | string | ClinVar review status |
| phenotype_list | string | Associated phenotypes |
| pathogenicity_group | string | Normalized clinical class |

### dim_region
| Column | Type | Description |
|---|---|---|
| region_key | string | Surrogate key for a geopolitical/analysis region |
| region_name | string | Name (e.g. Africa, Europe, Asia, Americas, Oceania) |
| region_code | string | Short code (e.g. AF, EU, AS, AM, OC) |
| description | string | Optional human-readable description |
| source_population | string | Source of region mapping (metadata source)

---

## Fact Tables

### fact_variant_annotation
| Column | Type | Description |
|---|---|---|
| fact_variant_key | string | Surrogate fact row key |
| variant_key | string | FK to `dim_variant` |
| gene_key | string | FK to `dim_gene` |
| clinvar_key | string | FK to `dim_clinvar_annotation` |
| chrom | string | Chromosome code |
| pos | int | Variant position |
| variant_id | string | VCF variant identifier |
| gene_id | string | Gene identifier mapped via range join |
| gene_name | string | Gene symbol mapped via gene annotation |
| clinical_significance | string | ClinVar annotation captured from the join |
| has_gene_annotation | boolean | Whether gene mapping exists |
| has_clinical_annotation | boolean | Whether ClinVar annotation exists |
| num_clinvar_evidence | int | Count of ClinVar records per variant |
| avg_quality_score | double | Quality score for the variant |
| processed_timestamp | timestamp | Gold processing event time |
| region_key | string | FK to `dim_region` to indicate sample/analysis region |
| region_name | string | Denormalized region name (for query convenience) |
| sample_population | string | Population or cohort label mapped to region |

### gold_clinical_significance
| Column | Type | Description |
|---|---|---|
| clinical_significance | string | ClinVar grouping |
| total_variants | long | Number of variants with this label |
| unique_genes | long | Distinct genes impacted |
| distinct_positions | long | Distinct genomic positions |
| avg_quality_score | double | Average VCF quality for this group |
| pathogenic_variant_ratio | double | Percent of variants labeled pathogenic |
| rank_by_burden | int | Rank within clinical class |

### gold_clinical_significance_by_region
| Column | Type | Description |
|---|---|---|
| region_key | string | FK to `dim_region` |
| region_name | string | Denormalized region name |
| clinical_significance | string | ClinVar grouping |
| total_variants | long | Number of variants with this label in region |
| unique_genes | long | Distinct genes impacted in region |
| avg_quality_score | double | Average VCF quality for region/class |
| pathogenic_variant_ratio | double | Percent pathogenic in region |
| rank_by_burden | int | Rank within region and clinical class |

### gold_gene_hotspots
| Column | Type | Description |
|---|---|---|
| gene_key | string | FK to `dim_gene` |
| gene_name | string | Gene symbol |
| gene_type | string | Biotype |
| total_variants | long | Number of variants mapped to gene |
| clinical_variants | long | Variants with clinical annotation |
| snp_count | long | Count of SNPs |
| indel_count | long | Count of insertions/deletions |
| avg_quality_score | double | Mean VCF quality score |
| hotspot_rank | int | Dense rank by variant burden |

### gold_gene_hotspots_by_region
| Column | Type | Description |
|---|---|---|
| region_key | string | FK to `dim_region` |
| region_name | string | Denormalized region name |
| gene_key | string | FK to `dim_gene` |
| gene_name | string | Gene symbol |
| total_variants | long | Number of variants mapped to gene in region |
| clinical_variants | long | Variants with clinical annotation in region |
| avg_quality_score | double | Mean VCF quality for gene in region |
| hotspot_rank | int | Dense rank by variant burden within region |

---

## Gold Layer Data Flow

1. `silver_vcf_variants` provides core variant details.
2. `silver_gene_annotations` maps variants to genes using range join: `chrom = seqname AND pos BETWEEN start_pos AND end_pos`.
3. `silver_clinical_variants` enriches variants using position join: `chromosome = chrom AND start_pos = pos`.
4. `dim_variant`, `dim_gene`, and `dim_clinvar_annotation` are generated from Silver tables.
5. `fact_variant_annotation` joins all dimensions into a single analytics row.
6. Aggregate tables `gold_clinical_significance` and `gold_gene_hotspots` are computed from the fact table.

---

## Compact Pipeline Diagram

```mermaid
flowchart LR
  Bronze((Bronze)) -->|ingest| Silver((Silver))
  Silver -->|transform & enrich| Gold((Gold))

  subgraph BronzeSrc [Bronze sources]
    b_vcf[VCF raw]
    b_gtf[GTF raw]
    b_clin[ClinVar raw]
  end

  subgraph SilverOps [Silver tables]
    s_vcf[silver_vcf_variants]
    s_gtf[silver_gene_annotations]
    s_clin[silver_clinical_variants]
  end

  subgraph GoldOps [Gold tables]
    dim[Dimensions]
    fact[fact_variant_annotation]
    agg[Regional Aggregates]
  end

  b_vcf --> s_vcf
  b_gtf --> s_gtf
  b_clin --> s_clin
  s_vcf --> dim
  s_gtf --> dim
  s_clin --> dim
  dim --> fact
  fact --> agg
```

---

## Schema ER Diagram (compact)

```mermaid
erDiagram
  DIM_VARIANT ||--o{ FACT_VARIANT : "variant_key -> variant_key"
  DIM_GENE ||--o{ FACT_VARIANT : "gene_key -> gene_key"
  DIM_CLINVAR ||--o{ FACT_VARIANT : "clinvar_key -> clinvar_key"
  DIM_REGION ||--o{ FACT_VARIANT : "region_key -> region_key"

  DIM_VARIANT {
    string variant_key PK
    string chrom
    int pos
    string variant_id
    string ref_allele
    string alt_allele
    string variant_type
    double quality_score
  }

  DIM_GENE {
    string gene_key PK
    string gene_id
    string gene_name
    string seqname
    int start_pos
    int end_pos
  }

  DIM_CLINVAR {
    string clinvar_key PK
    int allele_id
    int variation_id
    string chromosome
    long start_pos
    string clinical_significance
  }

  DIM_REGION {
    string region_key PK
    string region_name
    string region_code
  }

  FACT_VARIANT {
    string fact_variant_key PK
    string variant_key FK
    string gene_key FK
    string clinvar_key FK
    string region_key FK
    string chrom
    int pos
    string clinical_significance
    boolean has_gene_annotation
    boolean has_clinical_annotation
    int num_clinvar_evidence
    double avg_quality_score
    timestamp processed_timestamp
  }

  GOLD_CLINICAL_BY_REGION {
    string region_key FK
    string region_name
    string clinical_significance
    long total_variants
    long unique_genes
    double avg_quality_score
  }

  GOLD_HOTSPOTS_BY_REGION {
    string region_key FK
    string region_name
    string gene_key FK
    string gene_name
    long total_variants
    long clinical_variants
    double avg_quality_score
  }
```
