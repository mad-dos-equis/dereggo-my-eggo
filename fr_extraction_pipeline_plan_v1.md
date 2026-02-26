# Federal Register Extraction Pipeline: Implementation Plan & Working Context

**Project owner:** MC  
**Last updated:** 2026-02-26  
**Document version:** 1.3  
**Purpose:** This document carries full project context across Claude conversations. Upload it at the start of any session where we're working on this pipeline.

---

## 0. How to Use This Document

When starting a new conversation with Claude about this project, upload this file and say something like "Here's the FR pipeline plan. I'm working on Phase X" or "Pick up where we left off." This document contains everything needed to understand the project architecture, what's been built, what's in progress, and what decisions have been made and why.

**Update discipline:** After each working session that produces meaningful progress, ask Claude to generate an updated version of this document reflecting what changed. The STATUS TRACKER in Section 8 is the single source of truth for what's done.

---

## 1. Project Summary

### 1.1 What we're building

A production pipeline that processes the entire corpus of Federal Register documents (1936–present, 1M+ PDFs) through AWS Bedrock to produce a structured relational dataset suitable for estimating the economic impact of each regulation. The pipeline operates in two modes: a **backfill mode** that processes the historical corpus, and a **steady-state mode** that processes new documents daily as they are published and ingested by the existing FR scraper Lambda. Both modes share identical extraction logic — the difference is scale and trigger mechanism.

The pipeline has two processing tiers: a high-throughput batch extraction path for straightforward documents, and an agentic enrichment path for documents that require multi-step reasoning, cross-document lookups, or derived calculations.

### 1.2 What we're NOT building

- A RAG/knowledge base for querying FR documents conversationally (wrong tool for structured extraction at scale)
- An economic impact estimation model (that's downstream econometric work that consumes the structured dataset this pipeline produces)

### 1.3 End-state output

A set of relational tables (stored as partitioned Parquet on S3, queryable via Athena) containing per-document structured records with:
- Tier 1 metadata: document type, date, agency, CFR parts, RIN, docket number, page count, volume/page
- Tier 2 classification flags: economically significant (Y/N), contains RIA, references specific statute, industry codes
- Tier 3 economic content: stated cost/benefit estimates, discount rates, time horizons, entity counts, compliance costs, regulatory instruments used
- Tier 4 agent-enriched fields: cross-referenced estimates from related documents, derived cost calculations, NAICS mappings from descriptions, confidence scores and provenance chains

The dataset grows daily as new FR documents are published. Downstream consumers (R analysis, Quarto reports) always see a current, complete view of all processed documents via `arrow::open_dataset()` over date-partitioned Parquet on S3.

### 1.4 Downstream analytical use

The structured dataset feeds econometric analysis and Quarto-generated reports for regulation-related publications. Understanding the analytical intent shapes extraction priorities:

**Primary estimation goal:** Regulation-by-regulation economic impact estimates in dollar terms. For rules with explicit RIAs, the pipeline extracts the agency's own cost/benefit numbers. For rules without RIAs, the pipeline extracts enough structural information (affected industries, entity counts, regulatory instrument type, stringency direction) to support estimation using standard approaches from the regulatory economics literature (cost-per-entity extrapolation, industry-level compliance cost benchmarks, etc.).

**Why specific fields matter for the econometrics:**
- `total_cost_annualized` / `total_benefit_annualized` — These are the headline estimates. When the agency reports them, they are the primary outcome variable. When they're missing, the rest of the schema provides inputs for estimation.
- `discount_rate_low` / `discount_rate_high` — Essential for converting between annualized and present-value figures. Agencies use 3% and 7% per OMB Circular A-4; extracting which rates were used lets us recompute on a consistent basis.
- `entities_affected_count` + `compliance_cost_per_entity` — The building blocks for bottom-up cost estimation when aggregate figures aren't reported.
- `naics_codes` — Links regulations to industry-level data (employment, output, trade exposure) from Census and BLS, enabling sector-level impact analysis.
- `statutory_authority` — Enables grouping regulations by authorizing statute, which matters for analyzing regulatory burden by policy domain.
- `is_economically_significant` + `has_ria` — The triage variables. Only ~3-5% of FR documents are economically significant rules with formal RIAs. Identifying these cheaply (Haiku triage) is what makes the pipeline affordable.
- `cross_references` (RIN, docket) — Links NPRMs to final rules, enabling analysis of how proposed cost estimates evolve through the rulemaking process.
- `regulatory_instruments` — Categorizes the type of regulation (performance standard, technology mandate, reporting requirement, etc.), which is a key covariate in cost estimation models.

**What can be approximate vs. what must be precise:**
- Document type classification must be precise (>95% accuracy): it determines which documents enter the estimation sample
- Cost/benefit dollar figures should be within an order of magnitude; exact-to-the-dollar precision is not expected given variation in how agencies report
- NAICS codes can tolerate some noise: being in the right 3-digit sector is often sufficient for the industry-level analysis
- Dates (publication, effective) must be exact: they define the temporal structure of the regulatory panel

### 1.5 R workflow and data access patterns

The R pipeline accesses the structured data through two paths:

**Primary path: `arrow` + `dplyr` on Parquet files directly.** For analytical work, R reads partitioned Parquet from S3 using the `arrow` package with lazy evaluation — predicates are pushed down to the file scan, so only relevant partitions are read. This is fast and avoids Athena query costs for routine analysis. Typical pattern: filter to final rules, a specific agency or time period, join economic estimates, analyze in `dplyr` or `data.table`. Because the output is date-partitioned, new daily extractions are automatically visible without any R code changes — `open_dataset()` picks up new partition files on each call.

**Secondary path: Athena via `DBI` + `noctua`.** For ad hoc exploration, cross-table joins, or when the full dataset needs to be scanned with complex predicates. Also used in Quarto reports that run SQL for reproducibility.

**Quarto reports** run ad hoc (not scheduled). They pull from the structured dataset, compute summary statistics and time series, and render to HTML/PDF for the Substack publication.

**Implication for Parquet partitioning:** Partition by `year/document_type/` rather than `year/agency/document_type/` — the most common filter is time period + document type, and agency filtering happens in-memory after partition pruning. Too many partition levels create too many small files. See Section 2.6 for the daily append strategy that prevents small-file proliferation.

### 1.6 Key constraints

- **Budget:** Low thousands of dollars for LLM inference, not tens of thousands. This drives the two-tier architecture — Haiku for triage, Sonnet for extraction, agentic only where needed.
- **AWS region:** us-east-1 (existing infrastructure)
- **Existing assets:** FR scraper Lambda already running daily with SQLite manifest synced to S3 (see Section 2.2 for details)
- **Downstream consumer:** R analysis pipeline and Quarto reports for regulation-related publications
- **Latency tolerance:** Daily is sufficient. New documents published today should be extracted and available in the structured dataset by the next morning. Sub-hour latency is not needed.

### 1.7 Development philosophy: test small, scale later

Every phase is designed to be built and validated against a **500-document test sample** before scaling to the full corpus. The test sample is drawn randomly from the existing S3 PDF inventory, stratified to cover the range of document types, eras, and agencies. All infrastructure is structurally capable of handling 1M+ documents, but nothing runs at full scale until the test sample produces validated results. Phase 0 (Section 4.0) establishes this test sample before any other work begins.

**The two-track workflow:**

Development proceeds on the test sample across ALL phases before any full-corpus processing occurs. Specifically:

1. Phase 0: Draw the 500-doc sample
2. Phase 1: Run text extraction on the 500 docs
3. Phase 2: Run triage + full extraction on the 500 docs' extracted text
4. Phase 3: Run agent enrichment on the ~20-50 low-confidence cases from Phase 2's test output
5. Phase 4: Write the Parquet tables from Phase 2+3 test output, verify Athena + R access
6. Phase 5: Validate accuracy against ground truth, go/no-go decision

Each phase's test output is a valid input for the next phase. There is no need to run any phase on the full corpus before developing the next phase. The full-corpus backfill is a single operational event that happens AFTER Phase 5's decision gate, when accuracy is validated and all prompts are stable. At that point, scaling up is a configuration change (point the pipeline at the full S3 prefix instead of the test prefix), not new development.

**Why this matters:** Running Phase 1 on the full corpus before starting Phase 2 would cost weeks of compute time and provide no development value — you'd just be processing documents whose extracted text sits idle until the extraction prompts are written. The test-hop approach lets you iterate on the entire pipeline end-to-end in days, not months, at ~$7 per full pass through the 500-doc sample.

**The one exception:** Phase 3's `search_related_docs` tool requires a cross-document index (RIN → document_ids) that is sparse with only 500 documents. For testing, manually seed the index with 3-5 known rulemaking chains (NPRM → final rule → correction) from the sample. Full-fidelity cross-document search only becomes possible after the full-corpus Tier 1 extraction populates the index.

### 1.8 Two operational modes: backfill and steady-state

The pipeline is designed to serve two purposes with shared logic:

**Backfill mode** processes the historical corpus (1936–present) in large batches. This runs once after Phase 5's validation gate passes. It uses Bedrock Batch API for cost efficiency and processes documents in batches of ~10K. Backfill is a one-time operational event (with possible reruns for schema version upgrades).

**Steady-state mode** processes new documents daily as they arrive from the FR scraper Lambda. This is the pipeline's long-term operating mode — once backfill is complete, steady-state is all that runs. It uses the same extraction logic but is triggered by new PDFs appearing in S3, processes a much smaller daily batch (~100-300 documents), and appends results to the existing partitioned Parquet dataset.

The key design principle: **every component must work in both modes.** The text extractor, triage prompt, extraction prompt, confidence router, agent, and Parquet writer all process one document at a time — the difference between backfill and steady-state is only the trigger mechanism and batch size, not the processing logic. This is why the pipeline is built as a Step Functions state machine with per-document tracking, not as a monolithic batch script.

---

## 2. Architecture Overview

### 2.1 High-level data flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  EXISTING INFRASTRUCTURE                                            │
│  S3: Raw PDFs in federal_register_pdfs/YYYY/MM/DD/agency/type/     │
│  SQLite manifest (fr_manifest.db) synced to S3 each scraper run    │
│  Lambda scraper triggered daily by EventBridge                      │
└──────────┬──────────────────────────────────────────────────────────┘
           │
           │  ┌──────────────────────────────────────────────────┐
           ├──┤  DAILY TRIGGER (steady-state mode)               │
           │  │  Scraper completes → EventBridge rule or         │
           │  │  scraper invokes extraction pipeline directly    │
           │  │  Input: new document_ids from today's scrape     │
           │  └──────────────────────────────────────────────────┘
           │
           │  ┌──────────────────────────────────────────────────┐
           ├──┤  BACKFILL TRIGGER (one-time, post-validation)    │
           │  │  Manual invocation or scheduled batches          │
           │  │  Input: all unprocessed document_ids             │
           │  └──────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 0: TEST SAMPLE (500 documents)                               │
│  Draw stratified random sample from manifest                        │
│  Copy to test prefix in S3                                          │
│  All subsequent phases validate here first                          │
└──────────┬──────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 1: TEXT EXTRACTION                                           │
│  AWS Batch / EC2 (backfill) or Lambda (daily)                       │
│  PyMuPDF with section-aware parsing                                 │
│  Output: S3 extracted_text/ (JSON with section boundaries)          │
│  TEST: Run on 500-doc sample, manually verify section parsing       │
└──────────┬──────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PHASE 2: TIER 1 BATCH EXTRACTION                                   │
│  Step Functions orchestration                                       │
│                                                                     │
│  Stage A ─ Triage (Haiku):                                          │
│    Reads first 2 pages + headers                                    │
│    Outputs: doc_type, agency, is_economically_significant           │
│    Filters corpus to ~150K–200K documents for full extraction       │
│                                                                     │
│  Stage B ─ Full extraction (Sonnet, Bedrock Batch API):             │
│    Processes SUPPLEMENTARY INFORMATION + preamble                   │
│    Outputs: nested JSON per extraction schema                       │
│    Includes per-field confidence scores                             │
│                                                                     │
│  Stage C ─ Confidence routing:                                      │
│    High confidence → write to output tables                         │
│    Low confidence on critical fields → queue for Tier 2             │
│                                                                     │
│  Steady-state: same stages, but uses real-time Bedrock API          │
│  (not Batch) for daily volumes (~100-300 docs, cost delta minimal)  │
│                                                                     │
│  TEST: Run full pipeline on 500-doc sample, measure accuracy        │
└──────────┬──────────────────┬───────────────────────────────────────┘
           │                  │
           │ (high conf)      │ (low conf)
           ▼                  ▼
┌─────────────────┐  ┌───────────────────────────────────────────────┐
│  OUTPUT TABLES   │  │  PHASE 3: TIER 2 AGENTIC ENRICHMENT          │
│  S3 Parquet      │  │  Step Functions → Lambda agent runner         │
│  + Glue Catalog  │  │                                               │
│  + Athena        │  │  Agent receives Tier 1 output + full text     │
│                  │  │  Agent tools: read_section, search_related,   │
│  Date-partitioned│  │    extract_table, compute_estimate,            │
│  daily appends   │  │    lookup_naics, lookup_cfr,                   │
│                  │  │    flag_for_human_review, write_output         │
│                  │  │                                               │
│                  │  │  Max 15 steps per document cluster             │
│                  │  │  Output: enriched JSON + provenance trace      │
│                  │  │                                               │
│                  │  │  TEST: Run on ~50 hard cases from sample       │
│                  │◄─┤                                               │
└─────────────────┘  └───────────────────────────────────────────────┘
```

### 2.2 Existing infrastructure: FR scraper details

The scraper (`fr_scraper_lambda.py`) runs as an AWS Lambda function triggered daily by EventBridge. It uses GovInfo sitemaps to discover Federal Register packages, downloads individual granule PDFs and full-issue PDFs, and uploads them to S3. It includes smart-resumption logic: a short-circuit mechanism that skips older packages once it encounters a configurable number of consecutively completed packages, making daily incremental runs efficient. The scraper also retries previously failed downloads at the end of each run.

**S3 layout:**
```
s3://<BUCKET>/
├── federal_register_pdfs/          ← PDF storage
│   └── <year>/<month>/<day>/<agency_normalized>/<type>/<granule_id>.pdf
├── fr_manifest.db                  ← SQLite manifest (synced each run)
```

**SQLite manifest schema (`fr_manifest.db`):**
The manifest is the authoritative inventory of all downloaded documents. It is NOT a DynamoDB table — it's a SQLite database that the Lambda syncs to/from S3 at the start and end of each invocation.

```sql
documents (
    document_id TEXT PRIMARY KEY,    -- granule_id or package_id_full_issue
    package_id TEXT NOT NULL,        -- e.g. "FR-2024-03-15"
    granule_id TEXT,                 -- individual document ID within package
    title TEXT,                      -- document title from MODS metadata
    agency TEXT,                     -- issuing agency name
    section TEXT,                    -- RULE, PRORULE, NOTICE, PRESDOCU, etc.
    publish_date DATE,               -- publication date
    first_page INTEGER,              -- starting FR page number
    last_page INTEGER,               -- ending FR page number
    filepath TEXT,                   -- relative path within S3 prefix
    file_size INTEGER,               -- PDF size in bytes
    checksum TEXT,                   -- SHA-256 of PDF content
    downloaded_at TIMESTAMP,
    download_url TEXT,
    is_full_issue BOOLEAN DEFAULT FALSE
)

failed_downloads (document_id, package_id, download_url, error_message, retry_count, last_attempt)
scan_metadata (key, value, updated_at)
processed_sitemaps (sitemap_url, last_modified, processed_at, fully_walked)
completed_packages (package_id, granule_count, completed_at)
```

**What the manifest gives us for free:**
- `document_id` — unique key for every document
- `section` — document type classification (RULE, PRORULE, NOTICE, PRESDOCU, etc.)
- `agency` — issuing agency (raw name, not normalized code)
- `publish_date` — publication date
- `first_page` / `last_page` — page range (useful for page count proxy)
- `filepath` — S3 key suffix to locate the PDF
- `file_size` — for filtering out very small/large documents
- `downloaded_at` — timestamp that enables identifying newly ingested documents for daily extraction

**What the manifest does NOT have:**
- RIN (Regulation Identifier Number)
- Docket number
- CFR parts affected
- Whether the rule is economically significant
- Any content-derived fields

These must all be extracted by the pipeline.

**Environment variables for the scraper Lambda:**
- `S3_BUCKET` — target bucket name (required; to be confirmed in working session)
- `S3_PDF_PREFIX` — default `federal_register_pdfs/`
- `S3_MANIFEST_KEY` — default `fr_manifest.db`

### 2.3 Manifest observables (populated after Phase 0)

This section records what we learn from querying the manifest. It prevents re-querying in every session.

**Corpus size:** [TO BE FILLED — Phase 0]
- Total documents: ___
- Individual granules: ___
- Full-issue PDFs: ___
- Total size (GB): ___
- Date range: ___ to ___

**Document type distribution (`section` field values):**
[TO BE FILLED — Phase 0. Expected values include RULE, PRORULE, NOTICE, PRESDOCU, CORRECT, but there may be others. Record exact values and counts.]

| section value | count | % of corpus |
|---------------|-------|-------------|
| | | |

**Agency distribution (top 15):**
[TO BE FILLED — Phase 0. The `agency` field contains raw agency names, not standardized codes. Record the top agencies and note any null/missing values.]

| agency | count |
|--------|-------|
| | |

**Null/missing field prevalence:**
[TO BE FILLED — Phase 0]
- `agency` is NULL: ___% of documents
- `title` is NULL: ___% of documents  
- `section` is NULL: ___% of documents
- `first_page` / `last_page` is NULL: ___% of documents

**Decade distribution:**
[TO BE FILLED — Phase 0]

| Decade | count |
|--------|-------|
| 1930s | |
| 1940s | |
| ... | |
| 2020s | |

**File size distribution:**
[TO BE FILLED — Phase 0]
- Median file size: ___
- 90th percentile: ___
- Files > 1 MB: ___
- Files < 10 KB: ___

**Scanned vs. machine-readable estimate:**
[TO BE FILLED — Phase 1 quality report. Based on extraction quality scores from the 500-doc sample, estimate what fraction of the corpus is scanned/OCR-needed.]

**Daily ingestion rate:**
[TO BE FILLED — Phase 0. Query manifest for `downloaded_at` distribution to establish typical daily volume.]
- Average documents per day (recent 30 days): ___
- Max documents in a single day: ___
- Average documents per weekday (FR publishes Mon–Fri): ___

### 2.4 Key architectural decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Orchestration | Step Functions, not Lambda fan-out | Per-document state tracking, retry logic, conditional branching, concurrency limits |
| Bulk inference | Bedrock Batch API (backfill), real-time API (daily) | 50% cost reduction for backfill; real-time API for daily is cheap enough at ~200 docs/day and avoids batch job latency |
| Text extraction | PyMuPDF on EC2/Batch (backfill), Lambda (daily) | Backfill: PDFs are large, extraction is CPU-bound, Lambda timeout/memory limits are binding. Daily: volume is low enough for Lambda |
| Output storage | S3 Parquet + Glue + Athena | Columnar format for analytical queries, schema evolution via Glue, SQL access for R |
| Tracking database | DynamoDB (new, purpose-built) | Per-document extraction state — separate from the scraper's SQLite manifest, which tracks download state only |
| Agent framework | Standalone module with typed tool classes | Testable independently, tools are classes not closures, clean interface with pipeline |
| Section parsing | Custom FR-aware parser | FR documents have predictable section structure that generic chunking ignores |
| Development approach | Test on 500-doc sample, scale after validation | Every phase runs on small sample first; scale-up is a configuration change, not a rewrite |
| Daily processing trigger | Scraper completion event, not S3 object notifications | Scraper already knows which documents are new; piggybacking on its completion avoids scanning S3 for changes |
| Daily vs. batch API | Real-time Bedrock API for daily, Batch API for backfill | Daily volume (~200 docs) doesn't justify Batch API overhead and latency; cost difference is ~$0.05/day |

### 2.5 Why agentic enrichment and when it applies

Most FR documents (~80%) can be processed with a single-pass extraction prompt. The Tier 2 agentic path exists for documents where:

- The cost/benefit estimate is referenced but lives in a separate RIA document filed under a different docket number (requires cross-document lookup)
- Economic impact is implicit — embedded in tables of compliance deadlines and entity counts that require arithmetic to convert to cost estimates (requires compute_estimate tool)
- The document is a correction or amendment that only makes sense in context of the prior rule it modifies (requires search_related_docs)
- Text extraction produced garbled output from a multi-column layout or complex table (requires table re-extraction)
- The model reports low confidence on critical extraction fields after the Tier 1 pass

The agent receives the Tier 1 extraction output (with gaps flagged), reasons about what's missing, and iteratively calls tools to fill those gaps — analogous to how a research assistant would approach a difficult document by cross-referencing related filings, re-reading specific sections, and performing calculations.

### 2.6 Daily append strategy and small-file management

Daily processing appends a small number of extraction results to the Parquet dataset each day. Unmanaged, this creates a proliferation of tiny Parquet files (one per day per partition) that degrades read performance.

**Append pattern:**
- Daily extraction results are written to a staging prefix: `s3://<BUCKET>/output_tables_staging/<table>/dt=YYYY-MM-DD/batch.parquet`
- Each day's output is a single Parquet file per table (typically a few hundred rows).

**Compaction schedule:**
- A weekly compaction job (EventBridge + Lambda) merges daily staging files into the main output prefix, grouped into larger partition files: `s3://<BUCKET>/output_tables/<table>/year=YYYY/document_type=<type>/part-NNNN.parquet`
- Compaction rewrites the affected partitions with optimally-sized files (~128 MB target) and deletes consumed staging files.
- Between compaction runs, Athena and `arrow::open_dataset()` read from BOTH prefixes via a Glue table that unions them. Alternatively, R code can simply read both paths: `open_dataset(c("s3://bucket/output_tables/", "s3://bucket/output_tables_staging/"))`.

**Why not just append to the main partitions daily:**
- Rewriting entire year-level partition files every day is wasteful for the backfill-era partitions (which are static).
- Tiny daily files within the main partitions degrade Athena scan performance over time.
- The staging + compaction pattern keeps the main dataset optimally organized while ensuring daily freshness.

**Simplification for the test phase:** During development on the 500-doc sample, skip the staging/compaction split — just write directly to the output prefix. The staging pattern is only needed once daily processing begins at steady state.

---

## 3. Extraction Schema

### 3.1 Schema version: 1.0 (DRAFT)

This is the target structure for the Bedrock extraction output. Each document produces one JSON object with this shape. The ETL layer flattens nested arrays into relational tables.

```json
{
  "_meta": {
    "schema_version": "1.0",
    "extraction_tier": 1,
    "extraction_model": "anthropic.claude-3-5-sonnet-...",
    "extraction_timestamp": "2026-03-15T14:22:00Z",
    "processing_time_ms": 2340,
    "processing_mode": "batch|daily",
    "field_confidence": {
      "document_type": "high",
      "total_cost_annualized": "medium",
      "naics_codes": "low"
    }
  },

  "identification": {
    "fr_citation": "89 FR 12345",
    "document_number": "2024-01234",
    "rin": "2060-AU87",
    "docket_number": "EPA-HQ-OAR-2024-0001",
    "document_type": "final_rule",
    "title": "...",
    "agency_code": "EPA",
    "sub_agency": "Office of Air and Radiation",
    "publication_date": "2024-03-15",
    "effective_date": "2024-09-15",
    "cfr_parts_affected": ["40 CFR 60", "40 CFR 63"],
    "page_count": 47,
    "fr_volume": 89,
    "fr_start_page": 12345
  },

  "classification": {
    "is_economically_significant": true,
    "eo_12866_significant": true,
    "eo_14094_significant": false,
    "has_ria": true,
    "has_rfa_analysis": true,
    "has_sbrefa_panel": false,
    "is_major_cra": true,
    "statutory_authority": ["42 USC 7411", "42 USC 7412"],
    "is_deregulatory": false,
    "action_type": "new_requirement"
  },

  "economic_estimates": {
    "total_cost_annualized": 450000000,
    "total_cost_present_value": 3200000000,
    "total_benefit_annualized": 890000000,
    "total_benefit_present_value": 6400000000,
    "net_benefit_annualized": 440000000,
    "discount_rate_low": 0.03,
    "discount_rate_high": 0.07,
    "time_horizon_years": 20,
    "base_year": 2023,
    "cost_source": "agency_ria",
    "benefit_source": "agency_ria",
    "cost_methodology_notes": "...",
    "entities_affected_count": 2300,
    "compliance_cost_per_entity": 195000,
    "first_year_cost": 680000000,
    "cost_category_breakdown": [
      {"category": "capital", "amount": 280000000},
      {"category": "operating", "amount": 170000000}
    ]
  },

  "affected_industries": [
    {
      "naics_code": "325110",
      "naics_description": "Petrochemical manufacturing",
      "naics_source": "explicit",
      "entity_count": 2300,
      "entity_type": "facilities"
    }
  ],

  "regulatory_instruments": [
    {
      "instrument_type": "performance_standard",
      "stringency_direction": "tightening",
      "compliance_deadline": "2026-01-01",
      "phased_implementation": true,
      "description": "..."
    }
  ],

  "cross_references": [
    {
      "ref_type": "nprm",
      "fr_citation": "88 FR 45678",
      "document_number": "2023-05678",
      "relationship": "proposed_rule_for_this_final"
    }
  ],

  "text_excerpts": {
    "summary_section": "...",
    "cost_benefit_paragraph": "...",
    "rfa_paragraph": "..."
  }
}
```

### 3.2 Schema evolution policy

The `schema_version` field in `_meta` is critical. When we change the schema:
1. Increment version number
2. Update extraction prompts
3. DynamoDB tracking records include `extraction_schema_version`
4. Reprocessing query: find all documents where `extraction_schema_version < current_version`
5. Never delete old extractions — write new versions alongside

### 3.3 Confidence scoring rubric

Each critical field gets a confidence tag:
- **high**: Value explicitly stated in document text (e.g., "the annualized cost is $450 million")
- **medium**: Value inferred from context (e.g., cost derived from per-entity × entity count)
- **low**: Value estimated or uncertain (e.g., NAICS code inferred from industry description)
- **unable**: Field could not be populated from available text
- **not_applicable**: Field is structurally irrelevant for this document type (e.g., RIA fields for a notice)

Documents with "low" or "unable" on `total_cost_annualized`, `is_economically_significant`, or `document_type` are routed to Tier 2 agentic extraction.

---

## 4. Phase-by-Phase Implementation Plan

### Phase 0: Test Sample and Manifest Access

**Goal:** Establish a 500-document test sample that all subsequent phases validate against before scaling. Set up local access to the SQLite manifest for querying and sampling.

**Duration estimate:** 1–2 sessions

**Deliverables:**
1. **Manifest download script** — Pull `fr_manifest.db` from S3 to local, query it to understand corpus composition
   - Total document count by `section` (RULE, PRORULE, NOTICE, PRESDOCU, etc.)
   - Distribution by year and decade
   - Agency distribution
   - File size distribution
   - Count of full-issue PDFs vs. individual granules
   - Daily ingestion rate (query `downloaded_at` to understand typical daily volume for steady-state sizing)

2. **Stratified sampling script** — Draw 500 documents from the manifest, stratified by:
   - Document type (`section`): proportional to corpus, with floor of 10 per type
   - Era: pre-1980, 1981–1993, 1994–2009, 2010–present (50+ per era)
   - Agency: top 10 agencies by volume + random sample of others
   - File size: ensure mix of short (<100KB), medium, and long (>1MB) documents

3. **Test prefix in S3** — Copy the 500 sampled PDFs to a dedicated prefix:
   ```
   s3://<BUCKET>/extraction_pipeline_test/pdfs/<document_id>.pdf
   ```
   Also save the sample manifest as a CSV/JSON for easy reference.

4. **Confirm infrastructure details** — Record exact S3 bucket name, verify scraper is running, check manifest size and recency.

**Why this comes first:** Every subsequent phase needs test data. Drawing the sample also forces us to understand the corpus composition, which informs prompt design, chunking strategy, and cost estimates.

**What to build in our working sessions:**
- [ ] Download and query manifest — corpus composition analysis
- [ ] Sampling script
- [ ] Copy sample to S3 test prefix
- [ ] Record bucket name and infrastructure details in Section 5

---

### Phase 1: Text Extraction Infrastructure

**Goal:** Reliable, section-aware text extraction from FR PDFs, producing structured JSON with section boundaries.

**Duration estimate:** 1–2 weeks

**Test-first approach:** Build and validate against the 500-doc sample. Manually review extraction quality on ~50 documents spanning the stratification dimensions before scaling.

**Deliverables:**
1. `fr_text_extractor.py` — Section-aware PDF text extraction using PyMuPDF
   - Recognizes standard FR section headers: AGENCY, ACTION, SUMMARY, DATES, ADDRESSES, FOR FURTHER INFORMATION CONTACT, SUPPLEMENTARY INFORMATION, regulatory text
   - Handles multi-column layout (FR uses 3-column format)
   - Detects and flags tables (for later re-extraction by agentic tools)
   - Outputs JSON:
     ```json
     {
       "document_id": "2024-01234",
       "source_s3_key": "federal_register_pdfs/2024/03/15/.../2024-01234.pdf",
       "extraction_timestamp": "2026-03-20T10:00:00Z",
       "extraction_quality_score": 0.92,
       "page_count": 47,
       "is_scanned": false,
       "sections": {
         "AGENCY": "Environmental Protection Agency",
         "ACTION": "Final rule",
         "SUMMARY": "...",
         "DATES": "...",
         "SUPPLEMENTARY_INFORMATION": "...",
         "REGULATORY_TEXT": "..."
       },
       "raw_text": "...",
       "table_locations": [{"page": 12, "bbox": [x0,y0,x1,y1]}],
       "warnings": ["multi-column layout detected on pages 1-15"]
     }
     ```
   
2. **Local test harness** — Script that runs the extractor against the 500-doc sample and produces a quality report:
   - Extraction success rate (non-empty text)
   - Section detection rate (what fraction have identifiable SUMMARY, SUPPLEMENTARY INFORMATION, etc.)
   - Scanned vs. machine-readable classification accuracy
   - Flagged issues (garbled text, missing sections, zero-length output)

3. **Dual deployment targets** (for scale-up):
   - **Backfill: AWS Batch job definition** — Docker container with PyMuPDF, array job configuration. Output to `s3://<BUCKET>/extracted_text/YYYY/MM/DD/<document_id>.json`. Not deployed until test sample passes quality review.
   - **Daily: Lambda function** — Same extraction logic packaged as a Lambda. Triggered for each new document. At ~200 docs/day with typical FR PDF sizes, Lambda's 15-minute timeout and 10 GB memory ceiling are sufficient. Falls back to Batch for oversized PDFs (>50 MB).

4. **Integration with daily scraper** (for steady-state) — Mechanism to identify newly scraped documents and feed them into text extraction. Options evaluated in Section 2.4; preferred approach is scraper completion event → extraction trigger.

**Key technical risks:**
- Multi-column layout garbling. FR PDFs from different eras have different layouts. The test sample must cover decades.
- Scanned PDFs (pre-digital era, roughly pre-1994). These need OCR, which is a different pipeline. Flag them, count them in Phase 0, and handle separately.
- Tables with complex formatting. Initial pass extracts text; table-aware re-extraction is a Tier 2 agent tool.

**What to build in our working sessions:**
- [ ] Section header regex patterns for FR documents across eras
- [ ] Multi-column detection and handling logic
- [ ] Run on 500-doc sample, review quality report
- [ ] Iterate until section detection rate exceeds threshold (target: >85% for post-1994 docs)
- [ ] Batch job definition and Docker setup (for backfill)
- [ ] Lambda function packaging (for daily steady-state)

---

### Phase 2: Tier 1 Batch Extraction

**Goal:** Process extracted text through Bedrock to produce structured JSON per the extraction schema, using batch inference for cost efficiency (backfill) and real-time inference for daily processing (steady-state).

**Duration estimate:** 2–3 weeks

**Test-first approach:** Develop prompts interactively against 20 hand-picked documents. Then run full pipeline on 500-doc sample. Manually annotate 50 documents as ground truth and measure field-level accuracy before scaling.

**Deliverables:**
1. **Triage prompt and pipeline (Haiku)**
   - Input: first 2 pages of extracted text (header + SUMMARY + beginning of preamble)
   - Output:
     ```json
     {
       "document_type": "final_rule|proposed_rule|notice|proclamation|correction|other",
       "agency_code": "EPA",
       "is_economically_significant": true,
       "has_ria": true,
       "estimated_complexity": "simple|moderate|complex",
       "confidence": "high"
     }
     ```
   - Purpose: filter the corpus to ~150K–200K documents worth full extraction
   - Implementation: Bedrock Batch API (backfill), real-time API (daily)
   
2. **Full extraction prompt and pipeline (Sonnet)**
   - Input: SUPPLEMENTARY INFORMATION section + preamble (chunked if >100K tokens)
   - Output: full extraction schema JSON (Section 3.1)
   - Includes per-field confidence scoring
   - Implementation: Bedrock Batch API for backfill; real-time API for daily filtered documents
   
3. **Step Functions state machine** — `fr-extraction-pipeline`
   - States: prepare_triage_batch → submit_triage → poll_triage → parse_triage_results → filter_for_full_extraction → prepare_extraction_batch → submit_extraction → poll_extraction → parse_extraction_results → confidence_routing → write_outputs
   - **Dual invocation paths:** The state machine accepts either a batch of document_ids (backfill) or a "daily" flag that queries the DynamoDB tracking table for unprocessed documents. The processing states are identical — only the entry point differs.
   - **Daily path optimization:** For daily runs with <500 documents, the state machine skips Bedrock Batch API and uses real-time inference directly, avoiding the Batch API's minimum latency overhead (~30 min for job setup). A `processing_mode` parameter controls this: `batch` for backfill, `realtime` for daily.
   - Error handling: retry with backoff on Bedrock throttling, dead-letter queue for persistent failures
   - Concurrency: configurable max parallel batch jobs

4. **Confidence routing logic** — Lambda that checks extraction output, routes by confidence:
   - High confidence → Parquet writer
   - Low confidence on critical fields → SQS queue for Tier 2

5. **DynamoDB tracking table** — `fr-extraction-tracking`
   - Partition key: `document_id` (matches `document_id` from SQLite manifest)
   - Attributes: `extraction_status` (pending | triage_complete | extracted | enriched | failed), `schema_version`, `extraction_tier`, `last_processed`, `confidence_summary`, `s3_output_key`, `ingested_at` (copied from manifest's `downloaded_at`)
   - GSI on `extraction_status` for querying unprocessed documents (daily trigger path)
   - This is a NEW table, separate from the scraper's SQLite manifest. The manifest tracks *downloads*; this table tracks *extractions*.

6. **New-document registration** — A mechanism that populates the DynamoDB tracking table with new `document_id` entries (status: `pending`) whenever the scraper ingests new documents. Options:
   - **(a) Scraper writes directly:** Add a post-scrape step to `fr_scraper_lambda.py` that writes new document_ids to DynamoDB. Tight coupling but simple.
   - **(b) Manifest diff Lambda:** A Lambda triggered after the scraper that diffs the manifest against DynamoDB to find unregistered documents. Loose coupling, works for both backfill (register all) and daily (register new).
   - **Preferred: (b).** It keeps the scraper unchanged and works for both modes. The diff Lambda reads the manifest, queries DynamoDB for existing document_ids, and inserts any missing ones as `pending`.

**Prompt development strategy:**
- Start with 20 manually selected documents covering the hardest cases: a modern final rule with full RIA, a 1960s notice, a presidential proclamation with modifications, a correction, a proposed rule with detailed cost estimates
- Test across eras: 1940s, 1970s, 1990s (post-EO 12866), 2010s, 2020s
- Iterate prompts until field-level accuracy meets threshold on the 20-doc dev set
- Then run on full 500-doc sample and measure against 50-doc annotated ground truth

**What to build in our working sessions:**
- [ ] Triage prompt (Haiku) with test cases
- [ ] Full extraction prompt (Sonnet) with test cases
- [ ] Step Functions state machine definition (ASL JSON or CDK) with dual invocation paths
- [ ] Batch input preparation Lambda
- [ ] Confidence routing Lambda
- [ ] DynamoDB tracking table schema and creation
- [ ] New-document registration Lambda (manifest diff)
- [ ] Run on 500-doc sample, measure accuracy
- [ ] Iterate prompts based on error analysis

---

### Phase 3: Tier 2 Agentic Enrichment

**Goal:** An agent framework that reasons over complex documents, uses tools to gather additional context, and fills extraction gaps that Tier 1 couldn't resolve.

**Duration estimate:** 3–4 weeks

**Test-first approach:** Identify the ~50 documents from the 500-doc sample where Tier 1 produced low-confidence results. Build and test the agent against these specific cases before deploying at scale.

**Deliverables:**

1. **Agent framework (`extraction_agent/`)**
   ```
   extraction_agent/
   ├── agent.py           # ExtractionAgent class: reasoning loop
   ├── state.py           # ExtractionState: tracks what's known, what's missing
   ├── tools/
   │   ├── base.py        # Tool base class and registry
   │   ├── read_section.py
   │   ├── search_related.py
   │   ├── extract_table.py
   │   ├── compute_estimate.py
   │   ├── lookup_naics.py
   │   ├── lookup_cfr.py
   │   ├── flag_review.py
   │   └── write_output.py
   ├── prompts/
   │   ├── agent_system.py    # System prompt for the extraction agent
   │   └── tool_schemas.py    # Tool descriptions the LLM sees
   └── tests/
       ├── test_agent.py
       ├── test_tools.py
       └── fixtures/          # Known documents with expected outputs
   ```

2. **Tool implementations**

   | Tool | Backend | Notes |
   |------|---------|-------|
   | `read_section(doc_id, section)` | S3 (extracted text JSON) | Returns specific section from already-parsed document |
   | `search_related_docs(rin=, docket=, cfr_part=)` | SQLite manifest or DynamoDB tracking table | Finds related docs in rulemaking chain |
   | `extract_table(doc_id, page_range)` | PyMuPDF with table detection (Camelot/pdfplumber) | Re-extracts specific pages with layout-aware table parsing |
   | `compute_estimate(formula, inputs)` | Pure function | Annualization, PV/FV, per-entity → aggregate, discount rates |
   | `lookup_naics(description)` | Static lookup table in S3 | Maps industry text descriptions to NAICS codes |
   | `lookup_cfr(title, part)` | eCFR API or cached data | Retrieves current CFR section text for context |
   | `flag_for_human_review(reason)` | SQS queue | Escalates with structured reason; terminates agent loop |
   | `write_output(field, value, confidence, source)` | In-memory state | Adds a field to extraction result with provenance |

3. **Agent runner Lambda** — `fr-agent-runner`
   - Receives: `{document_id, tier1_extraction_s3_key, extracted_text_s3_key, related_doc_ids}`
   - Runs agent loop (max 15 steps, 5-minute timeout)
   - Writes: enriched extraction JSON to S3, trace log for debugging
   - Updates DynamoDB tracking record

4. **Step Functions integration**
   - SQS queue receives low-confidence documents from Tier 1 routing
   - Lambda polls queue, groups related documents by docket/RIN where possible
   - Dispatches agent runner per document cluster

**Key design principle:** Each tool is a standalone class with its own unit tests. The agent is testable with mock tools. This is the opposite of a monolithic engine function — every component has a clean interface and can be validated independently.

**CRITICAL SEQUENCING DEPENDENCY — Cross-document search:**
The `search_related_docs` tool needs RIN and docket number to find related documents in a rulemaking chain (e.g., linking a final rule to its NPRM). However, the scraper's SQLite manifest does NOT contain RIN or docket number — these fields only exist inside the PDF text and must be extracted by the Tier 1 pipeline (Phase 2). This means:

1. The agent's cross-document search capability is only as good as the Tier 1 extraction of `rin` and `docket_number` in the `identification` block.
2. Before Phase 3 can be fully operational, we need a **secondary index** built from Tier 1 outputs — a DynamoDB GSI or a simple lookup table mapping RIN → list of document_ids and docket_number → list of document_ids.
3. This index must be populated as part of the Phase 2 output pipeline, not as an afterthought.
4. For the 500-doc test sample, the index will be sparse (most related documents won't be in the sample). Testing `search_related_docs` at full fidelity requires either: (a) running Tier 1 on a larger subset first, or (b) manually seeding the index with known rulemaking chains for test cases.
5. **Daily processing benefit:** In steady-state, the cross-document index grows with each day's extraction. New documents referencing existing RINs/dockets are automatically linkable. This is one area where steady-state processing is strictly better than the test sample — the index gets richer over time.

This dependency should be addressed during Phase 2 implementation — specifically, the confidence routing Lambda should write RIN/docket entries to the secondary index as a side effect of processing each extraction.

**What to build in our working sessions:**
- [ ] ExtractionAgent class with reasoning loop
- [ ] ExtractionState class
- [ ] Tool base class and registry
- [ ] Each tool implementation (prioritize: read_section, search_related, compute_estimate)
- [ ] Agent system prompt
- [ ] Tool schema definitions for Bedrock native tool-use API
- [ ] Unit tests for each tool
- [ ] Integration test: agent against Tier 1 low-confidence cases from 500-doc sample
- [ ] Lambda runner packaging

---

### Phase 4: Output Tables and ETL

**Goal:** Flatten nested extraction JSON into relational tables accessible from R via Athena, with support for both bulk writes (backfill) and daily appends (steady-state).

**Duration estimate:** 1–2 weeks

**Test-first approach:** Build against the 500-doc sample output. Verify that the Parquet files are correctly partitioned and that Athena queries return expected results. Test R connectivity before scaling.

**Deliverables:**
1. **ETL Lambda** (`fr-parquet-writer`) — Triggered by new extraction results in S3
   - Reads nested JSON
   - Writes partitioned Parquet files
   - **Backfill mode:** writes directly to main output prefix, one file per partition: `output_tables/<table>/year=YYYY/document_type=<type>/part-NNNN.parquet`
   - **Daily mode:** writes to staging prefix: `output_tables_staging/<table>/dt=YYYY-MM-DD/batch.parquet`
   - Tables: `documents`, `economic_estimates`, `affected_industries`, `regulatory_instruments`, `cross_references`, `extraction_metadata`
   
2. **Compaction Lambda** (`fr-parquet-compactor`) — Weekly EventBridge trigger
   - Merges staging files into main output partitions
   - Targets ~128 MB per output file
   - Deletes consumed staging files
   - Logs compaction results to CloudWatch

3. **Glue Data Catalog** (`fr_structured` database) — Table definitions for each output table
   - Tables configured to read from BOTH main and staging prefixes (union via Glue table location or Athena view)
   
4. **Athena views** — Pre-built analytical views:
   - `economically_significant_rules` — join of documents + economic_estimates where is_economically_significant = true
   - `rulemaking_chains` — documents linked by RIN/docket across types (NPRM → final → correction)
   - `cost_estimates_by_agency_year` — aggregation view for trend analysis
   - `extraction_freshness` — latest extraction date, count of pending documents, daily processing status
   
5. **R integration** — `noctua` or `RAthena` package configuration for querying from R scripts

**What to build in our working sessions:**
- [ ] Parquet writer Lambda (test on 500-doc output)
- [ ] Glue table definitions (with staging union)
- [ ] Athena view SQL
- [ ] R connection and sample queries
- [ ] Verify round-trip: PDF → text → extraction → Parquet → Athena → R
- [ ] Compaction Lambda (can be deferred until steady-state is imminent)

---

### Phase 5: Quality Control and Validation

**Goal:** Systematic measurement of extraction accuracy and completeness before full-scale deployment.

**Duration estimate:** 2–3 weeks (overlaps with Phases 2–3)

**Deliverables:**
1. **Ground truth annotations** — Manual annotation of 50 documents from the 500-doc sample
   - These 50 should be stratified: ~20 final rules (mix of with/without RIA), ~10 proposed rules, ~10 notices, ~5 proclamations, ~5 corrections
   - Annotate every field in the extraction schema by hand
   
2. **Accuracy metrics:**
   - Field-level precision/recall for classification flags
   - Numeric accuracy for cost/benefit estimates (within 10%? within order of magnitude?)
   - NAICS code accuracy
   - Tier 1 vs. Tier 2 comparison: how much value does the agent add?
   
3. **Cross-model validation** — Extract 5% sample with a second model (e.g., Opus), compare against primary Sonnet extraction. Agreement rate by field.

4. **Error taxonomy** — Catalog of failure modes observed during testing:
   - Hallucinated costs
   - Wrong document type classification
   - Missed cross-references
   - Garbled table extraction
   - Scanned PDF mishandled as machine-readable
   - Context window truncation losing critical economic information

5. **Scale-up decision gate** — Documented go/no-go criteria:
   - Document type classification accuracy > 95%
   - Cost estimate accuracy within order of magnitude > 80% (where applicable)
   - Triage correctly identifies economically significant rules > 90%
   - Extraction completes without error for > 95% of test sample

**Only after this gate passes does full-corpus processing begin.** See Phase 6.

---

### Phase 6: Full-Corpus Backfill

**Goal:** Run the validated pipeline on the entire FR corpus. This is an operational phase, not a development phase.

**Prerequisites:** Phase 5 decision gate passed. All prompts stable. Infrastructure tested end-to-end on 500-doc sample.

**Duration estimate:** 2–5 days of active monitoring (compute runs in background over days/weeks)

**Steps:**
1. Run new-document registration Lambda against the full manifest (populates DynamoDB with all document_ids as `pending`)
2. Run Phase 1 (text extraction) on full corpus via AWS Batch array jobs
3. Run Phase 2 (triage + extraction) via Bedrock Batch API — submit in batches of ~10K documents
4. Monitor DynamoDB tracking table for failures, throttling, edge cases
5. Run Phase 3 (agent enrichment) on accumulated low-confidence queue
6. Run Phase 4 (ETL) on all extraction outputs — write to main output prefix (not staging)
7. Spot-check a random sample of full-corpus outputs against test-sample quality benchmarks

**Key risk:** Edge cases that didn't appear in the 500-doc sample. Budget time for fixing prompts and re-running subsets. The DynamoDB tracking table and schema versioning make partial re-runs straightforward.

**After backfill completes:** Transition to steady-state mode (Phase 7).

---

### Phase 7: Steady-State Daily Processing

**Goal:** Establish the pipeline's long-term operating mode where new Federal Register documents are automatically extracted each day.

**Prerequisites:** Phase 6 backfill complete. Daily trigger infrastructure deployed. Compaction Lambda operational.

**Duration estimate:** 1–2 sessions to configure and verify (most components already built in earlier phases)

**Deliverables:**

1. **Daily extraction trigger** — EventBridge rule or direct invocation from the scraper Lambda
   - Fires after the scraper completes its daily run (scraper currently runs via EventBridge cron)
   - Option A: EventBridge scheduled rule with ~30 min offset from scraper schedule
   - Option B: Scraper Lambda invokes extraction pipeline at end of `lambda_handler` (tighter coupling but guaranteed sequencing)
   - Option C: S3 event notification on `fr_manifest.db` upload (scraper pushes manifest at end of each run — this is a natural "scraper done" signal)
   - **Preferred: (C).** The scraper already calls `push_manifest_to_s3()` as its final step. An S3 event notification on the manifest key triggers the extraction pipeline. No changes to scraper code, no timing assumptions, no coupling.

2. **Daily pipeline orchestration** — The same Step Functions state machine from Phase 2, invoked with `processing_mode: "realtime"`
   - New-document registration Lambda runs first (diffs manifest vs. DynamoDB)
   - Step Functions processes all `pending` documents through triage → extraction → confidence routing → Parquet write
   - Low-confidence documents queued for Tier 2 enrichment (can run same-day or accumulate)

3. **Compaction schedule** — Weekly EventBridge rule triggers the compaction Lambda from Phase 4

4. **Monitoring and alerting:**
   - CloudWatch alarm on daily extraction Lambda errors
   - CloudWatch alarm if DynamoDB `pending` count grows (extraction falling behind ingestion)
   - Weekly summary metric: documents ingested vs. documents extracted vs. documents enriched
   - CloudWatch dashboard with key daily metrics

5. **Operational runbook:**
   - What to do when the daily pipeline fails (retry, check CloudWatch logs, manually trigger)
   - What to do when the scraper fails (extraction pipeline gracefully no-ops — no new documents to process)
   - How to reprocess documents after a schema update (query DynamoDB for `schema_version < current`, set to `pending`, trigger pipeline)
   - How to manually trigger a re-extraction for specific documents

**What to build in our working sessions:**
- [ ] S3 event notification on manifest key → extraction trigger
- [ ] Verify end-to-end daily flow: scraper runs → manifest updated → extraction triggered → new documents processed → Parquet appended → visible in R
- [ ] Compaction Lambda scheduling
- [ ] CloudWatch alarms and dashboard
- [ ] Operational runbook

---

## 5. AWS Resource Inventory

### 5.1 Existing resources (already deployed)

| Resource | Name/ARN | Notes |
|----------|----------|-------|
| S3 bucket | **[TO BE CONFIRMED IN SESSION]** | Raw FR PDFs + SQLite manifest |
| S3 prefix | `federal_register_pdfs/` | PDF storage, organized YYYY/MM/DD/agency/type/ |
| S3 object | `fr_manifest.db` | SQLite manifest, ~X MB (confirm size) |
| Lambda | FR scraper function **[confirm name]** | Daily trigger via EventBridge, runs `fr_scraper_lambda.py` |

### 5.2 New resources to create

| Resource | Name (proposed) | Phase | Purpose |
|----------|----------------|-------|---------|
| S3 prefix | `extraction_pipeline_test/` | 0 | 500-doc test sample |
| S3 prefix | `extracted_text/` | 1 | Section-parsed text JSON |
| S3 prefix | `extractions/tier1/` | 2 | Tier 1 extraction JSON |
| S3 prefix | `extractions/tier2/` | 2 | Tier 2 enriched JSON + traces |
| S3 prefix | `output_tables/` | 4 | Partitioned Parquet files (main) |
| S3 prefix | `output_tables_staging/` | 4/7 | Daily Parquet appends before compaction |
| DynamoDB table | `fr-extraction-tracking` | 2 | Per-document extraction state (separate from scraper manifest) |
| SQS queue | `fr-tier2-enrichment-queue` | 2 | Low-confidence documents for Tier 2 |
| SQS DLQ | `fr-tier2-enrichment-dlq` | 2 | Failed enrichment attempts |
| Step Functions | `fr-extraction-pipeline` | 2 | Main orchestration state machine (backfill + daily) |
| Step Functions | `fr-agent-enrichment` | 3 | Tier 2 agent orchestration |
| Lambda | `fr-triage-batch-prep` | 2 | Prepares JSONL for Bedrock Batch triage |
| Lambda | `fr-extraction-batch-prep` | 2 | Prepares full extraction JSONL |
| Lambda | `fr-confidence-router` | 2 | Routes by confidence to Tier 2 or output |
| Lambda | `fr-agent-runner` | 3 | Runs agentic extraction loop |
| Lambda | `fr-parquet-writer` | 4 | ETL: JSON → Parquet |
| Lambda | `fr-parquet-compactor` | 4/7 | Weekly merge of staging → main Parquet |
| Lambda | `fr-new-doc-registrar` | 2 | Diffs manifest vs. DynamoDB, registers new documents |
| Glue database | `fr_structured` | 4 | Athena-queryable catalog |
| Batch job def | `fr-pdf-extraction` | 1 | Parallel text extraction (backfill only) |
| IAM role | `fr-pipeline-execution-role` | 1 | Shared role for pipeline Lambdas |
| S3 event notification | On `fr_manifest.db` PUT | 7 | Triggers daily extraction pipeline |
| EventBridge rule | `fr-weekly-compaction` | 7 | Weekly trigger for Parquet compaction |
| CloudWatch dashboard | `fr-extraction-pipeline` | 7 | Daily processing metrics |
| CloudWatch alarms | Pipeline error rate, pending backlog | 7 | Operational alerting |

---

## 6. Prompt Registry

Prompts are first-class artifacts in this pipeline, version-controlled and tested. Each prompt has a name, a version, and a test suite.

### 6.1 Triage prompt (Haiku)

**Name:** `fr_triage_v1`  
**Model:** `anthropic.claude-3-haiku-...` (confirm model ID)  
**Status:** NOT STARTED  
**Input:** First ~2,000 tokens of extracted text (header + SUMMARY + start of preamble)  
**Output schema:**
```json
{
  "document_type": "final_rule|proposed_rule|notice|proclamation|correction|other",
  "agency_code": "...",
  "is_economically_significant": true|false,
  "has_ria": true|false,
  "estimated_complexity": "simple|moderate|complex",
  "confidence": "high|medium|low"
}
```
**Test results:** [TO BE FILLED after prompt development]
- Accuracy on 20-doc dev set: ___
- Accuracy on 500-doc sample: ___
- Known failure modes: [list specific misclassification patterns observed]

### 6.2 Full extraction prompt (Sonnet)

**Name:** `fr_extraction_v1`  
**Model:** `anthropic.claude-3-5-sonnet-...` (confirm model ID and batch availability)  
**Status:** NOT STARTED  
**Input:** SUPPLEMENTARY INFORMATION section + document header  
**Output schema:** Full schema from Section 3.1  
**Test results:** [TO BE FILLED after prompt development]
- Field-level accuracy on 50-doc annotated ground truth: ___
- Known failure modes: [list specific extraction errors observed]

### 6.3 Agent system prompt

**Name:** `fr_agent_v1`  
**Model:** Sonnet with native tool-use (confirm version)  
**Status:** NOT STARTED  
**Role:** Instructs the agent on its purpose, available tools, reasoning strategy, and when to escalate  
**Test results:** [TO BE FILLED after prompt development]
- Success rate on low-confidence cases from 500-doc sample: ___
- Average steps per document: ___
- Average cost per document: ___
- Known failure modes: [list observed agent reasoning failures, looping, hallucinated tool calls]

### 6.4 Prompt development protocol

When developing or updating a prompt:
1. Write the prompt
2. Test against 5 diverse documents manually (Bedrock console or API)
3. Run against the 20-document development set
4. Compare outputs against manual annotations
5. Iterate until field-level accuracy meets threshold
6. Run on full 500-doc test sample
7. Record the prompt version, test results, and known failure modes in this section
8. Only then deploy to pipeline for full-corpus processing

---

## 7. Cost Estimates

### 7.1 Test sample costs (500 documents)

| Component | Quantity | Estimated cost |
|-----------|----------|----------------|
| Text extraction (local/EC2) | 500 PDFs | ~$0 (trivial compute) |
| Triage (Haiku) | 500 docs | ~$0.10 |
| Full extraction (Sonnet) | ~200 docs (filtered) | ~$2 |
| Agent enrichment (Sonnet, multi-turn) | ~40 docs | ~$5 |
| **Test sample total** | | **~$7** |

### 7.2 Full backfill costs (1M+ documents)

| Component | Quantity | Unit cost | Total |
|-----------|----------|-----------|-------|
| Text extraction (Batch compute) | ~900 GB PDFs | ~$0.02/GB | ~$20 |
| Triage pass (Haiku batch) | ~1M docs × ~500 tokens | ~$0.00025/1K input | ~$125 |
| Full extraction (Sonnet batch) | ~200K docs × ~5K tokens avg | ~$0.0015/1K (batch) | ~$1,500 |
| Agent enrichment (Sonnet, multi-turn) | ~40K docs × ~15K tokens avg | ~$0.003/1K input | ~$1,800 |
| **Full backfill total** | | | **~$3,500** |

### 7.3 Ongoing monthly costs (steady-state)

| Component | Estimated monthly | Notes |
|-----------|-------------------|-------|
| Daily new document text extraction (~200 docs/day) | ~$0.50 | Lambda compute for PyMuPDF |
| Daily triage (Haiku, real-time API) | ~$0.15 | ~6K docs/month × ~500 tokens |
| Daily full extraction (Sonnet, real-time API) | ~$5 | ~2K filtered docs/month × ~5K tokens (real-time pricing, not batch) |
| Daily agent enrichment (Sonnet, multi-turn) | ~$3 | ~400 docs/month × ~15K tokens |
| S3 storage (all prefixes, ~1 TB total) | ~$23 | Grows slowly |
| DynamoDB (on-demand, ~1M items, light traffic) | ~$5 | Read-heavy for daily diffs |
| Lambda compute (all extraction Lambdas) | ~$2 | |
| Athena queries (depends on usage) | ~$5 | |
| Weekly compaction Lambda | ~$0.50 | Runs once/week |
| **Monthly total** | **~$44** | |

---

## 8. Status Tracker

Update this section after each working session.

Development proceeds on the 500-doc test sample through Phases 0–5. Full-corpus processing (Phase 6) is gated on Phase 5 go/no-go. Steady-state daily processing (Phase 7) follows backfill completion. See Section 1.7 for the two-track workflow and Section 1.8 for the two operational modes.

| Phase | Component | Status | Last Updated | Notes |
|-------|-----------|--------|-------------|-------|
| — | FR scraper Lambda | DEPLOYED | pre-existing | Daily PDF scraping operational |
| — | SQLite manifest on S3 | DEPLOYED | pre-existing | Authoritative document inventory |
| 0 | Manifest download + corpus analysis | NOT STARTED | — | Results go into Section 2.3 |
| 0 | Stratified 500-doc sample | NOT STARTED | — | |
| 0 | Test prefix in S3 | NOT STARTED | — | |
| 0 | Confirm infra details (bucket, names) | NOT STARTED | — | |
| 1 | Section-aware text extractor | NOT STARTED | — | Test on 500-doc sample |
| 1 | Quality report on 500-doc sample | NOT STARTED | — | |
| 1 | Lambda packaging (for daily) | NOT STARTED | — | Same logic as Batch, different deployment |
| 2 | Triage prompt (Haiku) | NOT STARTED | — | Test on 500-doc sample |
| 2 | Full extraction prompt (Sonnet) | NOT STARTED | — | Test on 500-doc sample |
| 2 | Step Functions state machine | NOT STARTED | — | Dual-path: batch + realtime |
| 2 | Confidence routing Lambda | NOT STARTED | — | |
| 2 | DynamoDB tracking table | NOT STARTED | — | |
| 2 | New-document registration Lambda | NOT STARTED | — | Manifest diff → DynamoDB |
| 2 | RIN/docket secondary index | NOT STARTED | — | Built as Phase 2 side effect; needed for Phase 3 |
| 2 | 500-doc sample pipeline run | NOT STARTED | — | |
| 3 | ExtractionAgent class | NOT STARTED | — | Test on ~50 low-confidence cases from Phase 2 |
| 3 | Tool implementations | NOT STARTED | — | |
| 3 | Agent system prompt | NOT STARTED | — | |
| 3 | Agent runner Lambda | NOT STARTED | — | |
| 3 | Agent test on low-confidence cases | NOT STARTED | — | |
| 4 | Parquet writer ETL | NOT STARTED | — | Test on 500-doc output from Phases 2+3 |
| 4 | Glue catalog + Athena | NOT STARTED | — | Includes staging union |
| 4 | R integration | NOT STARTED | — | |
| 4 | Compaction Lambda | NOT STARTED | — | Can defer until Phase 7 |
| 5 | Ground truth annotations (50 docs) | NOT STARTED | — | Can start in parallel with Phase 1 |
| 5 | Accuracy measurement | NOT STARTED | — | |
| 5 | Scale-up decision gate | NOT STARTED | — | BLOCKS Phase 6 |
| 6 | Full corpus text extraction | NOT STARTED | — | Blocked on Phase 5 go/no-go |
| 6 | Full corpus Tier 1 extraction | NOT STARTED | — | Blocked on Phase 5 go/no-go |
| 6 | Full corpus Tier 2 enrichment | NOT STARTED | — | Blocked on Phase 5 go/no-go |
| 6 | Full corpus ETL + Parquet | NOT STARTED | — | Blocked on Phase 5 go/no-go |
| 6 | Post-backfill spot-check | NOT STARTED | — | |
| 7 | Daily extraction trigger (S3 event) | NOT STARTED | — | Blocked on Phase 6 completion |
| 7 | Daily pipeline end-to-end verification | NOT STARTED | — | Blocked on Phase 6 completion |
| 7 | Compaction scheduling | NOT STARTED | — | |
| 7 | CloudWatch monitoring + alarms | NOT STARTED | — | |
| 7 | Operational runbook | NOT STARTED | — | |

---

## 9. Open Questions and Decisions Pending

These are items that need resolution during implementation. When resolved, move to Section 10 (Decision Log) with rationale.

1. **S3 bucket name.** Need to confirm exact name in a working session.

2. **Lambda function name for scraper.** Need to confirm for reference.

3. **Manifest size and document count.** Query the manifest to understand current corpus size, distribution by section/year/agency. Results go into Section 2.3.

4. **Scanned PDF handling (pre-digital era).** Options: (a) OCR with Textract as part of Phase 1, (b) exclude pre-1994 scanned documents from initial pipeline, (c) use Textract only when PyMuPDF extraction quality score is below threshold. Leaning toward (c) — flag them in Phase 0 corpus analysis and decide based on how many there are.

5. **Context window strategy for very long documents.** Options: (a) send only SUPPLEMENTARY INFORMATION section, (b) two-pass extraction (preamble + regulatory text, then merge), (c) use a model with 200K+ context and send everything. Need to benchmark quality vs. cost on the 500-doc sample.

6. **Bedrock Batch API availability.** Need to verify which models support batch inference in us-east-1 and current quota limits.

7. **Agent step budget and cost ceiling.** Proposed 15 steps max. Should there be a per-document dollar ceiling?

8. **Human review queue design.** When the agent flags a document for human review, where does it go? Options: (a) SQS → dashboard, (b) DynamoDB status flag surfaced in a periodic report. Lean toward (b) for simplicity.

9. **Which Bedrock models to use.** Haiku 3 for triage seems clear. For extraction: Sonnet 3.5 vs. Sonnet 4 vs. newer? For agent: needs native tool-use support. Need to check pricing and capability.

10. **CFR lookup tool data source.** eCFR API (live) vs. bulk download? Live is simpler but adds latency to agent loop.

11. **Daily trigger mechanism final selection.** S3 event notification on manifest upload is preferred (Section Phase 7), but needs validation that the event fires reliably after the scraper's `push_manifest_to_s3()` call. Alternative: have the scraper Lambda directly invoke the extraction Step Functions at the end of its run.

12. **Compaction frequency and strategy.** Weekly compaction is proposed. Need to validate that weekly is frequent enough to keep Athena scan performance acceptable given daily append volume. May need to adjust to twice-weekly or daily if staging files accumulate too fast.

Note: The cross-document search dependency (RIN/docket index) is documented in Phase 3 directly, not here — it's a known sequencing constraint, not an open question.

---

## 10. Decision Log

Record resolved decisions here with date, choice, rationale, and what was considered and rejected (so we don't re-litigate closed decisions in future sessions).

| Date | Decision | Choice | Rejected alternatives | Rationale |
|------|----------|--------|----------------------|-----------|
| 2026-02-25 | Orchestration pattern | Step Functions | Lambda fan-out (no state tracking, no retry, no backpressure at 1M+ scale); SQS + Lambda consumers (viable but less visibility into per-document state) | Per-document state tracking, retry logic, conditional branching, concurrency limits, visual execution history |
| 2026-02-25 | Agent framework design | Standalone module with typed tool classes | Monolithic engine function with tools as closures (untestable, tools coupled to engine state); LangChain agent framework (too much abstraction, harder to debug) | Each tool independently testable, agent testable with mocks, clean interface with pipeline |
| 2026-02-25 | Output format | Parquet on S3 + Glue + Athena | DynamoDB (poor for analytical queries, expensive at scale for scans); RDS/Postgres (operational overhead, fixed capacity); CSV on S3 (no schema enforcement, no columnar pushdown) | Columnar for R `arrow` queries, schema evolution via Glue, SQL access via Athena, cost-effective at rest |
| 2026-02-25 | Two-tier extraction | Batch (Tier 1) + Agentic (Tier 2) | All-agentic (too expensive — $0.20-0.50/doc × 1M = $200K+); all-single-pass (misses complex documents); human review for hard cases only (doesn't scale) | ~80% handled by single-pass ($0.02-0.05/doc), ~20% need multi-step reasoning at higher cost |
| 2026-02-25 | LLM response parsing | Bedrock native tool-use API | Heuristic format sniffing (fragile, collapses all error types into empty dict, acceptable for research prototype but not production ETL) | Typed, validated responses with explicit error categorization |
| 2026-02-25 | Tracking store | DynamoDB (new, `fr-extraction-tracking`) separate from scraper manifest | Extending the SQLite manifest with extraction columns (SQLite doesn't support concurrent Lambda writes, manifest is the scraper's responsibility); single DynamoDB table for both downloads and extractions (conflates two different lifecycles) | Manifest tracks downloads (SQLite, scraper-owned), tracking table tracks extractions (DynamoDB, pipeline-owned). Clear ownership boundaries. |
| 2026-02-25 | Development approach | Test on 500-doc sample, scale after validation | Build at scale from the start (too expensive to iterate on prompts at full-corpus cost); test on 10 docs (too small to catch distribution issues) | 500 docs costs ~$7 per full pipeline run, large enough to be statistically meaningful, small enough for manual review |
| 2026-02-25 | Parquet partitioning | `year/document_type/` | `year/agency/document_type/` (too many small files — hundreds of agencies × multiple types × 90 years); `year/month/` (doesn't support the most common filter pattern: time + doc type) | Matches primary R query pattern: filter by time period + document type, then filter agency in-memory |
| 2026-02-26 | Pipeline operational modes | Dual-mode: backfill (Batch API) + steady-state daily (real-time API) with shared extraction logic | Backfill-only with daily processing deferred (delays the pipeline's primary long-term value); daily-only with no backfill (misses historical corpus); separate codebases for batch and daily (duplication, drift) | Same Step Functions state machine handles both modes via `processing_mode` parameter. Backfill uses Bedrock Batch API for 50% cost savings. Daily uses real-time API because ~200 docs/day doesn't justify batch overhead. All extraction logic (prompts, validation, ETL) is shared. |
| 2026-02-26 | Daily trigger mechanism | S3 event notification on `fr_manifest.db` PUT | Scraper directly invokes extraction (couples scraper to extraction, scraper changes require extraction awareness); timed EventBridge offset (fragile timing assumption, scraper runtime varies); S3 event on individual PDFs (too many events, no batching) | Manifest upload is the scraper's natural "I'm done" signal. Decoupled: scraper doesn't know extraction exists. Reliable: fires exactly once per scraper run. |
| 2026-02-26 | Daily Parquet append strategy | Staging prefix + weekly compaction | Rewrite main partition files daily (wasteful for static historical partitions); append tiny files to main partitions (degrades scan performance over time); single monolithic file (doesn't scale, concurrent write risk) | Staging + compaction keeps main dataset optimally organized while ensuring daily freshness. R reads both prefixes seamlessly via `open_dataset()`. |

---

## 11. Repository Structure (Target)

```
fr-extraction-pipeline/
├── README.md
├── requirements.txt
├── setup.py
│
├── extraction_agent/
│   ├── __init__.py
│   ├── agent.py
│   ├── state.py
│   └── tools/
│       ├── __init__.py
│       ├── base.py
│       ├── read_section.py
│       ├── search_related.py
│       ├── extract_table.py
│       ├── compute_estimate.py
│       ├── lookup_naics.py
│       ├── lookup_cfr.py
│       ├── flag_review.py
│       └── write_output.py
│
├── extraction/
│   ├── __init__.py
│   ├── text_extractor.py          # PyMuPDF section-aware parser
│   ├── section_patterns.py        # FR section header patterns by era
│   ├── schema.py                  # Extraction schema definition + validation
│   ├── triage.py                  # Triage prompt builder
│   └── full_extraction.py         # Full extraction prompt builder
│
├── pipeline/
│   ├── __init__.py
│   ├── batch_prep.py              # Bedrock Batch JSONL preparation
│   ├── realtime_invoker.py        # Bedrock real-time API for daily mode
│   ├── confidence_router.py       # Route by confidence to Tier 2 or output
│   ├── parquet_writer.py          # ETL: JSON → Parquet (backfill + daily)
│   ├── parquet_compactor.py       # Weekly staging → main merge
│   ├── doc_registrar.py           # Manifest diff → DynamoDB registration
│   └── tracking.py                # DynamoDB tracking operations
│
├── lambdas/
│   ├── fr_triage_batch_prep/
│   │   └── handler.py
│   ├── fr_extraction_batch_prep/
│   │   └── handler.py
│   ├── fr_confidence_router/
│   │   └── handler.py
│   ├── fr_agent_runner/
│   │   └── handler.py
│   ├── fr_parquet_writer/
│   │   └── handler.py
│   ├── fr_parquet_compactor/
│   │   └── handler.py
│   ├── fr_new_doc_registrar/
│   │   └── handler.py
│   └── fr_text_extractor/
│       └── handler.py             # Lambda version for daily text extraction
│
├── infra/
│   ├── step_functions/
│   │   ├── extraction_pipeline.asl.json
│   │   └── agent_enrichment.asl.json
│   ├── batch/
│   │   └── text_extraction_job.json
│   ├── dynamodb/
│   │   └── tracking_table.json
│   ├── eventbridge/
│   │   └── compaction_schedule.json
│   ├── s3_events/
│   │   └── manifest_notification.json
│   ├── cloudwatch/
│   │   ├── dashboard.json
│   │   └── alarms.json
│   └── deploy.sh
│
├── prompts/
│   ├── registry.py
│   ├── fr_triage_v1.py
│   ├── fr_extraction_v1.py
│   └── fr_agent_v1.py
│
├── tests/
│   ├── test_text_extractor.py
│   ├── test_schema_validation.py
│   ├── test_triage_prompt.py
│   ├── test_extraction_prompt.py
│   ├── test_agent.py
│   ├── test_tools.py
│   ├── test_confidence_router.py
│   ├── test_parquet_writer.py
│   ├── test_parquet_compactor.py
│   ├── test_doc_registrar.py
│   └── fixtures/
│       ├── sample_pdfs/
│       ├── expected_extractions/
│       └── mock_responses/
│
├── scripts/
│   ├── download_manifest.py       # Pull manifest from S3 for local querying
│   ├── draw_test_sample.py        # Stratified sample of 500 docs
│   ├── corpus_analysis.py         # Manifest exploration and summary stats
│   └── copy_sample_to_s3.py       # Set up test prefix
│
├── analysis/
│   ├── validation_sample.R
│   ├── accuracy_metrics.R
│   └── cost_monitoring.R
│
├── runbooks/
│   ├── daily_pipeline_failure.md
│   ├── reprocessing_schema_update.md
│   └── manual_reextraction.md
│
└── notebooks/
    ├── 01_corpus_exploration.ipynb
    ├── 02_prompt_development.ipynb
    ├── 03_validation_review.ipynb
    └── 04_exploratory_analysis.ipynb
```

---

## 12. Glossary

| Term | Meaning in this project |
|------|------------------------|
| FR | Federal Register |
| Manifest | The SQLite database (`fr_manifest.db`) maintained by the scraper Lambda — tracks all downloaded PDFs |
| Tracking table | The DynamoDB table (`fr-extraction-tracking`) — tracks extraction processing state per document |
| Granule | An individual document within a daily FR issue (e.g., one rule, one notice) |
| Package | A complete daily FR issue (e.g., `FR-2024-03-15`) containing multiple granules |
| RIA | Regulatory Impact Analysis — economic analysis accompanying major rules |
| RIN | Regulation Identifier Number — tracks a rule through NPRM → final |
| CFR | Code of Federal Regulations — the codification target of rules |
| EO 12866 / EO 14094 | Executive Orders defining "economically significant" rules ($100M+ annual impact) |
| RFA | Regulatory Flexibility Act — requires analysis of impact on small entities |
| SBREFA | Small Business Regulatory Enforcement Fairness Act |
| CRA | Congressional Review Act — designates "major" rules subject to congressional review |
| Triage | Tier 1 cheap classification pass (Haiku) to filter corpus before full extraction |
| Enrichment | Tier 2 agentic multi-step extraction for complex documents |
| Provenance | Record of which source and method produced each extracted value |
| Schema version | Tracks extraction schema evolution for idempotent reprocessing |
| Test sample | The stratified 500-document subset used to validate each phase before scaling |
| Backfill mode | One-time processing of the historical corpus (Phase 6) using Bedrock Batch API |
| Steady-state mode | Ongoing daily processing of new documents (Phase 7) using real-time Bedrock API |
| Staging prefix | S3 location for daily Parquet appends, before weekly compaction into main output |
| Compaction | Weekly merge of small daily Parquet files into optimally-sized partition files |
