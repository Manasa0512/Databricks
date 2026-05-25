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
