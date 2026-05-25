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

## Schema Diagram

```mermaid
classDiagram
    class bronze_vcf_variants_raw {
        +string raw_value
        +timestamp ingestion_timestamp
        +string source_file
        +string ingestion_id
        +date ingestion_date
    }
    class bronze_gene_annotations_raw {
        +string raw_value
        +timestamp ingestion_timestamp
        +string source_file
        +string ingestion_id
        +date ingestion_date
    }
    class bronze_clinical_variants_raw {
        +int _AlleleID
        +int VariationID
        +string Type
        +string GeneSymbol
        +int GeneID
        +string ClinicalSignificance
        +string ReviewStatus
        +string Chromosome
        +long Start
        +long Stop
        +string ReferenceAllele
        +string AlternateAllele
        +string Assembly
        +string PhenotypeIDS
        +string PhenotypeList
        +string Origin
        +int NumberSubmitters
        +string LastEvaluated
        +timestamp ingestion_timestamp
        +string source_file
        +string ingestion_id
        +date ingestion_date
    }
    class silver_vcf_variants {
        +string chrom
        +int pos
        +string variant_id
        +string ref_allele
        +string alt_allele
        +double quality_score
        +string filter_status
        +string info
        +timestamp bronze_ingestion_timestamp
        +string source_file
        +timestamp silver_processing_timestamp
        +boolean is_high_quality
        +int ref_length
        +int alt_length
        +string variant_type
    }
    class silver_gene_annotations {
        +string seqname
        +string source
        +string feature
        +int start_pos
        +int end_pos
        +string score
        +string strand
        +string frame
        +string attributes
        +timestamp bronze_ingestion_timestamp
        +string source_file
        +timestamp silver_processing_timestamp
        +string gene_id
        +string gene_name
        +string gene_type
        +string transcript_id
        +int length
    }
    class silver_clinical_variants {
        +int allele_id
        +int variation_id
        +string variant_type
        +string gene_symbol
        +int gene_id
        +string clinical_significance
        +string review_status
        +string chromosome
        +long start_pos
        +long stop_pos
        +string ref_allele
        +string alt_allele
        +string assembly
        +string phenotype_ids
        +string phenotype_list
        +string origin
        +int number_submitters
        +string last_evaluated
        +timestamp bronze_ingestion_timestamp
        +string source_file
        +timestamp silver_processing_timestamp
    }
    class gold_variant_summary {
        +string chrom
        +int pos
        +string variant_id
        +string ref_allele
        +string alt_allele
        +string gene_id
        +string gene_name
        +string gene_type
        +string strand
        +int clinvar_allele_id
        +string clinical_significance
        +string review_status
        +string phenotype_list
        +string clinvar_gene_symbol
        +double quality_score
        +boolean is_high_quality
        +string variant_type
        +string filter_status
        +boolean has_clinical_annotation
        +boolean has_gene_annotation
        +timestamp gold_processing_timestamp
    }
    class gold_clinical_significance {
        +string aggregation_type
        +string clinical_significance
        +string gene_name
        +long variant_count
        +long unique_genes
        +long chromosomes_affected
        +long snp_count
        +long insertion_count
        +long deletion_count
        +double avg_quality_score
        +double pct_of_clinical_variants
        +int rank_in_category
        +long unique_positions
        +timestamp processing_timestamp
    }
    class gold_gene_hotspots {
        +string gene_name
        +long total_variants
        +long snp_count
        +long insertion_count
        +long deletion_count
        +long clinical_variants
        +double avg_quality_score
    }

    bronze_vcf_variants_raw --> silver_vcf_variants
    bronze_gene_annotations_raw --> silver_gene_annotations
    bronze_clinical_variants_raw --> silver_clinical_variants
    silver_vcf_variants --> gold_variant_summary
    silver_gene_annotations --> gold_variant_summary
    silver_clinical_variants --> gold_variant_summary
    gold_variant_summary --> gold_clinical_significance
    gold_variant_summary --> gold_gene_hotspots
```

### Schema Notes
- Bronze tables store raw content plus ingestion audit metadata.
- Silver tables are structured and enriched with parsed fields + processing timestamps.
- `gold_variant_summary` is the core joined fact table.
- Aggregation tables derive from `gold_variant_summary` for clinical and gene hotspot analytics.
