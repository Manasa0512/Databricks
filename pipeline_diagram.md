# Genomics ETL Pipeline Diagram

```mermaid
flowchart TD
    subgraph Bronze[Bronze Layer]
      B1[VCF Raw Text]
      B2[GTF Raw Text]
      B3[ClinVar Raw CSV]
      B1 -->|Ingest + audit metadata| BRONZE_VCF[bronze_vcf_variants_raw]
      B2 -->|Ingest + audit metadata| BRONZE_GTF[bronze_gene_annotations_raw]
      B3 -->|Ingest + audit metadata| BRONZE_CLINVAR[bronze_clinical_variants_raw]
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
      G1[Variant summary enrichment]
      G2[Clinical significance aggregation]
      G3[Gene hotspot ranking]
      G1 --> GOLD_VARIANT[gold_variant_summary]
      G2 --> GOLD_CLINICAL[gold_clinical_significance]
      G3 --> GOLD_HOTSPOTS[gold_gene_hotspots]
    end

    BRONZE_VCF --> S1
    BRONZE_GTF --> S2
    BRONZE_CLINVAR --> S3

    S1 --> G1
    S2 --> G1
    S3 --> G1
    G1 --> G2
    G1 --> G3

    style Bronze fill:#f3f4f6,stroke:#9ca3af
    style Silver fill:#e0f2fe,stroke:#0284c7
    style Gold fill:#dcfce7,stroke:#16a34a
    style BRONZE_VCF fill:#fff,stroke:#6b7280
    style BRONZE_GTF fill:#fff,stroke:#6b7280
    style BRONZE_CLINVAR fill:#fff,stroke:#6b7280
    style SILVER_VCF fill:#fff,stroke:#0284c7
    style SILVER_GTF fill:#fff,stroke:#0284c7
    style SILVER_CLINVAR fill:#fff,stroke:#0284c7
    style GOLD_VARIANT fill:#fff,stroke:#16a34a
    style GOLD_CLINICAL fill:#fff,stroke:#16a34a
    style GOLD_HOTSPOTS fill:#fff,stroke:#16a34a
```

## Notes
- Bronze layer ingests raw genomic files with audit metadata and partitions by `ingestion_date`.
- Silver layer parses and validates raw text into structured Delta tables.
- Gold layer enriches variants, aggregates clinical significance, and ranks gene hotspots.

## Schema Diagram (ER - Dark Mode Optimized)

```mermaid
erDiagram
    BRONZE_VCF ||--o{ SILVER_VCF : ingests
    BRONZE_GTF ||--o{ SILVER_GTF : ingests
    BRONZE_CLINVAR ||--o{ SILVER_CLINVAR : ingests
    SILVER_VCF }o--|| SILVER_GTF : "range join (chrom, start_pos, end_pos)"
    SILVER_VCF }o--|| SILVER_CLINVAR : "position join (chromosome, start_pos)"
    SILVER_VCF ||--o{ GOLD_VARIANT_SUMMARY : enriches
    SILVER_GTF ||--o{ GOLD_VARIANT_SUMMARY : enriches
    SILVER_CLINVAR ||--o{ GOLD_VARIANT_SUMMARY : enriches
    GOLD_VARIANT_SUMMARY ||--o{ GOLD_CLINICAL_SIG : aggregates
    GOLD_VARIANT_SUMMARY ||--o{ GOLD_GENE_HOTSPOTS : aggregates

    BRONZE_VCF {
        string raw_value
        timestamp ingestion_timestamp
        string source_file
        string ingestion_id
        date ingestion_date
    }
    BRONZE_GTF {
        string raw_value
        timestamp ingestion_timestamp
        string source_file
        string ingestion_id
        date ingestion_date
    }
    BRONZE_CLINVAR {
        int _AlleleID
        int VariationID
        string Type
        string GeneSymbol
        int GeneID
        string ClinicalSignificance
        string ReviewStatus
        string Chromosome
        long Start
        long Stop
        timestamp ingestion_timestamp
    }
    SILVER_VCF {
        string chrom "PK"
        int pos "PK"
        string variant_id "PK"
        string ref_allele
        string alt_allele
        double quality_score
        string filter_status
        boolean is_high_quality
        string variant_type
    }
    SILVER_GTF {
        string gene_id "PK"
        string gene_name
        string gene_type
        string transcript_id
        string seqname
        int start_pos
        int end_pos
        string strand
        int length
    }
    SILVER_CLINVAR {
        int allele_id "PK"
        string chromosome "FK"
        long start_pos "FK"
        string gene_symbol
        int gene_id "FK"
        string clinical_significance
        string review_status
        string phenotype_list
    }
    GOLD_VARIANT_SUMMARY {
        string chrom "PK"
        int pos "PK"
        string variant_id "PK"
        string gene_id "FK"
        int clinvar_allele_id "FK"
        string gene_name
        string clinical_significance
        boolean has_clinical_annotation
        boolean has_gene_annotation
    }
    GOLD_CLINICAL_SIG {
        string aggregation_type
        string clinical_significance
        string gene_name
        long variant_count
        long unique_genes
        long snp_count
        int rank_in_category
    }
    GOLD_GENE_HOTSPOTS {
        string gene_name "PK"
        long total_variants
        long snp_count
        long clinical_variants
        double avg_quality_score
    }
```

---

## Detailed Schema Tables

### **BRONZE LAYER**

#### bronze_vcf_variants_raw
| Column Name | Type Cast | Key Type |
|---|---|---|
| raw_value | string | - |
| ingestion_timestamp | timestamp | - |
| source_file | string | - |
| ingestion_id | string | - |
| ingestion_date | date | Partition |

#### bronze_gene_annotations_raw
| Column Name | Type Cast | Key Type |
|---|---|---|
| raw_value | string | - |
| ingestion_timestamp | timestamp | - |
| source_file | string | - |
| ingestion_id | string | - |
| ingestion_date | date | Partition |

#### bronze_clinical_variants_raw
| Column Name | Type Cast | Key Type |
|---|---|---|
| _AlleleID | int | - |
| VariationID | int | - |
| Type | string | - |
| GeneSymbol | string | - |
| GeneID | int | - |
| ClinicalSignificance | string | - |
| ReviewStatus | string | - |
| Chromosome | string | - |
| Start | long | - |
| Stop | long | - |
| ReferenceAllele | string | - |
| AlternateAllele | string | - |
| Assembly | string | - |
| PhenotypeList | string | - |
| Origin | string | - |
| NumberSubmitters | int | - |
| ingestion_timestamp | timestamp | - |
| source_file | string | - |
| ingestion_id | string | - |
| ingestion_date | date | Partition |

---

### **SILVER LAYER**

#### silver_vcf_variants
| Column Name | Type Cast | Key Type |
|---|---|---|
| chrom | string | PK |
| pos | int | PK |
| variant_id | string | PK |
| ref_allele | string | - |
| alt_allele | string | - |
| quality_score | double | - |
| filter_status | string | - |
| info | string | - |
| is_high_quality | boolean | - |
| ref_length | int | - |
| alt_length | int | - |
| variant_type | string | - |
| bronze_ingestion_timestamp | timestamp | - |
| source_file | string | - |
| silver_processing_timestamp | timestamp | - |

**Partition By:** chrom

**Joins To:**
- `silver_gene_annotations` via **range join**: `chrom = seqname AND pos BETWEEN start_pos AND end_pos`
- `silver_clinical_variants` via **position join**: `chrom = chromosome AND pos = start_pos`

---

#### silver_gene_annotations
| Column Name | Type Cast | Key Type |
|---|---|---|
| gene_id | string | PK |
| gene_name | string | - |
| gene_type | string | - |
| transcript_id | string | - |
| seqname | string | - |
| source | string | - |
| feature | string | - |
| start_pos | int | FK (to VCF) |
| end_pos | int | - |
| score | string | - |
| strand | string | - |
| frame | string | - |
| length | int | - |
| bronze_ingestion_timestamp | timestamp | - |
| source_file | string | - |
| silver_processing_timestamp | timestamp | - |

**Partition By:** seqname

**Joins To:**
- `silver_vcf_variants` via **range join**: Referenced by VCF (chrom, pos)

---

#### silver_clinical_variants
| Column Name | Type Cast | Key Type |
|---|---|---|
| allele_id | int | PK |
| variation_id | int | - |
| variant_type | string | - |
| gene_symbol | string | - |
| gene_id | int | FK (to Gene) |
| clinical_significance | string | - |
| review_status | string | - |
| chromosome | string | FK (to VCF) |
| start_pos | long | FK (to VCF) |
| stop_pos | long | - |
| ref_allele | string | - |
| alt_allele | string | - |
| assembly | string | - |
| phenotype_ids | string | - |
| phenotype_list | string | - |
| origin | string | - |
| number_submitters | int | - |
| last_evaluated | string | - |
| bronze_ingestion_timestamp | timestamp | - |
| source_file | string | - |
| silver_processing_timestamp | timestamp | - |

**Partition By:** chromosome

**Joins To:**
- `silver_vcf_variants` via **position join**: `chromosome = chrom AND start_pos = pos`

---

### **GOLD LAYER**

#### gold_variant_summary
| Column Name | Type Cast | Key Type |
|---|---|---|
| chrom | string | PK |
| pos | int | PK |
| variant_id | string | PK |
| ref_allele | string | - |
| alt_allele | string | - |
| gene_id | string | FK (to Gene) |
| gene_name | string | - |
| gene_type | string | - |
| strand | string | - |
| clinvar_allele_id | int | FK (to ClinVar) |
| clinical_significance | string | - |
| review_status | string | - |
| phenotype_list | string | - |
| clinvar_gene_symbol | string | - |
| quality_score | double | - |
| is_high_quality | boolean | - |
| variant_type | string | - |
| filter_status | string | - |
| has_clinical_annotation | boolean | - |
| has_gene_annotation | boolean | - |
| gold_processing_timestamp | timestamp | - |

**Partition By:** chrom

**Optimization:** Z-ORDER BY (gene_name, clinical_significance)

**Join Strategy:**
- **VCF ↔ Gene (Range Join)**: `chrom = seqname AND pos BETWEEN start_pos AND end_pos`
- **VCF ↔ ClinVar (Position Join)**: `chrom = chromosome AND pos = start_pos`

---

#### gold_clinical_significance
| Column Name | Type Cast | Key Type |
|---|---|---|
| aggregation_type | string | - |
| clinical_significance | string | - |
| gene_name | string | - |
| variant_count | long | - |
| unique_genes | long | - |
| chromosomes_affected | long | - |
| snp_count | long | - |
| insertion_count | long | - |
| deletion_count | long | - |
| avg_quality_score | double | - |
| pct_of_clinical_variants | double | - |
| rank_in_category | int | - |
| unique_positions | long | - |
| processing_timestamp | timestamp | - |

**Partition By:** clinical_significance

**Derived From:** gold_variant_summary (aggregation with GROUP BY)

---

#### gold_gene_hotspots
| Column Name | Type Cast | Key Type |
|---|---|---|
| gene_name | string | PK |
| total_variants | long | - |
| snp_count | long | - |
| insertion_count | long | - |
| deletion_count | long | - |
| clinical_variants | long | - |
| avg_quality_score | double | - |

**Partition By:** None (small table ~7K genes)

**Derived From:** gold_variant_summary (aggregation by gene_name)

---

### Schema Notes
- Bronze tables store raw content plus ingestion audit metadata.
- Silver tables are structured and enriched with parsed fields + processing timestamps.
- `gold_variant_summary` is the core joined fact table.
- **FK = Foreign Key** (join reference), **PK = Primary Key** (unique identifier)
- **Range Join**: Variant position falls within gene boundaries
- **Position Join**: Variant chromosome + position matches clinical variant records

### Schema Notes
- Bronze tables store raw content plus ingestion audit metadata.
- Silver tables are structured and enriched with parsed fields + processing timestamps.
- `gold_variant_summary` is the core joined fact table.
- Aggregation tables derive from `gold_variant_summary` for clinical and gene hotspot analytics.
