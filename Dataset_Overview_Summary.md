# Genomic Data Analysis Pipeline — Dataset Overview Summary

## Project
- Student: Manasa Vundela
- Status: COMPLETE
- Last Updated: May 29, 2026
- Implementation: 100% PySpark, Delta Lake on Databricks

## Purpose
Production-ready medallion pipeline (Bronze → Silver → Gold) integrating:
- VCF population variants (1000 Genomes, chr1)
- GTF gene annotations (GENCODE v49)
- ClinVar clinical variant summaries

## Key Metrics
- Input variants (VCF, chr1): 6,468,094
- Genes (GTF): 78,691
- ClinVar records (post-dedup): 4,514,767
- Gold variant summary (integrated): 7,405,220
- Variants mapped to genes: 5,750,770 (77.7%)
- Variants with clinical annotations: 73,319 (0.99%)
- ETL tests: 11/11 passed

## Architecture
- Bronze: raw ingestion to Delta (append, partition by ingestion_date)
- Silver: parsing, type-casting, validation, deduplication, partition by chromosome
- Gold: range join (VCF↔GTF) + position join (VCF↔ClinVar), denormalized analytics tables
- Job: Daily at 02:00 AM (Asia/Calcutta)

## Join Strategy (summary)
- Stage 1 (VCF ↔ GTF): Normalize chromosome naming ("1" → "chr1"), filter GTF to `feature='gene'`, broadcast join on `pos BETWEEN start AND end` → maps variants to genes.
- Stage 2 ((VCF+GTF) ↔ ClinVar): Join on `(chrom, pos)` only (ClinVar allele fields largely missing) → attach clinical annotations.

## Gold Tables (high level)
- `gold_variant_summary` (27 cols): genomic info, gene fields, clinical fields, 6 population frequency columns, metadata
- `gold_clinical_significance` (14 cols): aggregated clinical stats
- `gold_gene_hotspots` (7 cols): gene-level variant burden

## Notable Findings & Decisions
- ClinVar allele fields are mostly "na" → position-only joins used
- Broadcasted GTF genes (~78K) for performance (6x speedup)
- 22.3% intergenic variants are expected biologically
- Population frequencies extracted from VCF INFO (6 regional cols)

## Sample Queries
- High-quality SNPs, gene hotspots, pathogenic variants, population-specific filters are included as ready SQL examples in the notebook.

## Next Steps (options)
- Export detailed Silver/Gold schemas into this README
- Add example Power BI connection instructions
- Scale pipeline to additional chromosomes (multi-chromosome run)

---

File generated from `layers/Dataset_Overview_Summary.py` notes.
