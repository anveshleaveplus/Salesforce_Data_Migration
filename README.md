# Oracle to Salesforce Account Field Mapping Tool

This Python project connects to an Oracle database and Salesforce, reads schema and field metadata, generates all possible pairwise mappings between Oracle columns and Salesforce Account fields with explainable scores, and outputs results in multiple formats.

## Features

- Connects to Oracle database and reads schema from `ALL_TAB_COLUMNS`
- Optionally samples column values to detect patterns (email, phone, URL, postcode, ABN)
- Connects to Salesforce and retrieves Account field metadata
- Generates full pairwise matrix of mappings with scores based on:
  - Name similarity (RapidFuzz token_set_ratio)
  - Data type compatibility
  - Pattern signals from sampled values
  - Synonym bonuses
- Outputs:
  - CSV with all pairs and score breakdowns
  - Top-K CSVs for both directions
  - YAML files for mappings

## Requirements

- Python 3.10+
- Oracle Instant Client (optional, for thick mode)
- Access to Oracle database
- Salesforce account with API access

## Installation

1. Clone or download this repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in your configuration.

## Configuration

Edit the `.env` file with your settings:

- **Oracle**: Host, port, service name, user, password, schema
- **Salesforce**: Username, password, security token, domain
- **Options**: Table filters, sampling rows, top-K count

## Usage

Run the main script:

```bash
python src/map_all_mappings.py
```

Outputs will be generated in the `out/` directory.

## Outputs

- `oracle_to_account_all_pairs.csv`: Full matrix with scores
- `oracle_to_account_topk.csv`: Top K mappings per Oracle column
- `account_to_oracle_topk.csv`: Top K mappings per Salesforce field
- `mappings_full.yaml`: Detailed mappings with reasons
- `mappings.yaml`: First-choice mappings only

## Notes

- Ensure Oracle Instant Client is installed if using thick mode.
- Sampling can be disabled by setting `SAMPLE_ROWS=0`.
- The tool handles connection failures gracefully with error messages.