"""Constants for use within the CLI"""


GEN3_ENVIRONMENT_KEY = "GEN3_ENVIRONMENT"
INDEXD_USER = "INDEXDUSER"
INDEXD_PASSWORD = "INDEXDPASS"  # pragma: allowlist secret

# for names of injected variables needed in attrition endpoint
CASE_COUNTS_VAR_ID = "--case_counts_only--"
CONTROL_COUNTS_VAR_ID = "--control_counts_only--"

# some summary statstics column names
STATS_COLUMN_PVAL = "Score.pval"
STATS_COLUMN_SPA_PVAL = "SPA.pval"
