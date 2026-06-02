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
%%{init: {"theme":"dark"}}%%
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
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| variant_key | string | Surrogate Key |
| chrom | string | - |
| pos | int | - |
| variant_id | string | - |
| ref_allele | string | - |
| alt_allele | string | - |
| variant_type | string | - |
| quality_score | double | - |
| filter_status | string | - |
| is_high_quality | boolean | - |
| bronze_ingestion_timestamp | timestamp | - |
| source_file | string | - |

### dim_gene
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| gene_key | string | Surrogate Key |
| gene_id | string | - |
| gene_name | string | - |
| gene_type | string | - |
| seqname | string | - |
| start_pos | int | - |
| end_pos | int | - |
| strand | string | - |
| length | int | - |
| annotation_source | string | - |

### dim_clinvar_annotation
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| clinvar_key | string | Surrogate Key |
| allele_id | int | - |
| variation_id | int | - |
| chromosome | string | - |
| start_pos | long | - |
| gene_symbol | string | - |
| gene_id | int | - |
| clinical_significance | string | - |
| review_status | string | - |
| phenotype_list | string | - |
| pathogenicity_group | string | - |

### dim_region
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| region_key | string | Surrogate Key |
| region_name | string | - |
| region_code | string | - |
| description | string | - |
| source_population | string | - |

---

## Fact Tables

### fact_variant_annotation
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| fact_variant_key | string | Surrogate Key |
| variant_key | string | FK |
| gene_key | string | FK |
| clinvar_key | string | FK |
| chrom | string | - |
| pos | int | - |
| variant_id | string | - |
| gene_id | string | - |
| gene_name | string | - |
| clinical_significance | string | - |
| has_gene_annotation | boolean | - |
| has_clinical_annotation | boolean | - |
| num_clinvar_evidence | int | - |
| avg_quality_score | double | - |
| processed_timestamp | timestamp | - |
| region_key | string | FK |
| region_name | string | - |
| sample_population | string | - |

### gold_clinical_significance
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| clinical_significance | string | - |
| total_variants | long | - |
| unique_genes | long | - |
| distinct_positions | long | - |
| avg_quality_score | double | - |
| pathogenic_variant_ratio | double | - |
| rank_by_burden | int | - |

### gold_clinical_significance_by_region
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| region_key | string | FK |
| region_name | string | - |
| clinical_significance | string | - |
| total_variants | long | - |
| unique_genes | long | - |
| avg_quality_score | double | - |
| pathogenic_variant_ratio | double | - |
| rank_by_burden | int | - |

### gold_gene_hotspots
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| gene_key | string | FK |
| gene_name | string | - |
| gene_type | string | - |
| total_variants | long | - |
| clinical_variants | long | - |
| snp_count | long | - |
| indel_count | long | - |
| avg_quality_score | double | - |
| hotspot_rank | int | - |

### gold_gene_hotspots_by_region
| Column Name | Type Cast | Key Type |
|---|---:|---:|
| region_key | string | FK |
| region_name | string | - |
| gene_key | string | FK |
| gene_name | string | - |
| total_variants | long | - |
| clinical_variants | long | - |
| avg_quality_score | double | - |
| hotspot_rank | int | - |

---

## Gold Layer Data Flow

1. `silver_vcf_variants` provides core variant details.
2. `silver_gene_annotations` maps variants to genes using range join: `chrom = seqname AND pos BETWEEN start_pos AND end_pos`.
3. `silver_clinical_variants` enriches variants using position join: `chromosome = chrom AND start_pos = pos`.
4. `dim_variant`, `dim_gene`, and `dim_clinvar_annotation` are generated from Silver tables.
5. `fact_variant_annotation` joins all dimensions into a single analytics row.
6. Aggregate tables `gold_clinical_significance` and `gold_gene_hotspots` are computed from the fact table.

---

## Joins (explicit)

- VCF → GTF (range join):

```sql
-- Map variants to genes
SELECT v.*, g.gene_key, g.gene_name
FROM silver_vcf_variants v
JOIN silver_gene_annotations g
  ON v.chrom = g.seqname
  AND v.pos BETWEEN g.start_pos AND g.end_pos
```

- VCF → ClinVar (position join):

```sql
-- Attach clinical annotations by position
SELECT v.*, c.clinvar_key, c.clinical_significance
FROM silver_vcf_variants v
LEFT JOIN silver_clinical_variants c
  ON v.chrom = c.chromosome
  AND v.pos = c.start_pos
```

- Fact → Dimensions (FK joins):

```sql
SELECT f.*, dv.*, dg.*, dc.*, dr.region_name
FROM fact_variant_annotation f
LEFT JOIN dim_variant dv ON f.variant_key = dv.variant_key
LEFT JOIN dim_gene dg ON f.gene_key = dg.gene_key
LEFT JOIN dim_clinvar_annotation dc ON f.clinvar_key = dc.clinvar_key
LEFT JOIN dim_region dr ON f.region_key = dr.region_key
```

---

## Compact Pipeline Diagram

```mermaid
%%{init: {"theme":"dark"}}%%
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
%%{init: {"theme":"dark"}}%%
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
