-- name: process_bronze
WITH raw_casted AS (
    SELECT
        *,
        TRY_TO_DATE(NULLIF(TRIM(transaction_date), ''), 'yyyy-MM-dd') AS casted_date
    FROM bronze_table
),

casted_data AS (
    SELECT
        -- transaction_sk = SHA256[:32] (128-bit) over (date, transaction_id, counterparty, amount).
        -- Content-hash by design: rows identical on all four observables collapse to one SK
        -- (intentional dedup of true duplicates; also makes re-runs idempotent).
        -- 128 bits keeps cryptographic collision risk negligible for any realistic dataset.
        SUBSTR(SHA2(CONCAT_WS('~',casted_date, NULLIF(transaction_id,''),counterparty_details, CAST(transaction_amount AS STRING)), 256), 1, 32) AS transaction_sk,
        casted_date AS transaction_date,
        TO_DATE(NULLIF(posting_date, ''), 'yyyy-MM-dd') AS posting_date,
        TRIM(LOWER(counterparty_details)) as counterparty_details,
        TRIM(LOWER(description)) AS description,
        REGEXP_REPLACE(account_number, "['\\s]", "") AS account_number,
        TRIM(bank_name) AS bank_name,
        LOWER(TRIM(details)) AS details,
        REGEXP_REPLACE(transaction_id, "['\\s]", "") AS transaction_id,
        CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(transaction_amount, '[\\s\\xa0]', ''), ',', '.'), '') AS DECIMAL(18, 2)) as transaction_amount,
        TRIM(transaction_currency) AS transaction_currency,
        CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(blocked_amount, '[\\s\\xa0]', ''), ',', '.'), '') AS DECIMAL(18, 2)) as blocked_amount,
        TRIM(blocked_currency) AS blocked_currency,
        CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(foreign_payment_amount, '[\\s\\xa0]', ''), ',', '.'), '') AS DECIMAL(18, 2)) as foreign_payment_amount,
        TRIM(foreign_payment_currency) AS foreign_payment_currency,
        TRIM(account) AS account,
        CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(balance_after_transaction, '[\\s\\xa0]', ''), ',', '.'), '') AS DOUBLE) as balance_after_transaction,
        _source_file,
        TIMESTAMP(_ingested_at) as _ingested_at,
        _hash
    FROM raw_casted
    WHERE casted_date IS NOT NULL
),

classified_payment_methods AS (
    SELECT *,
        CASE
            WHEN details LIKE '%kantor%' OR details LIKE 'przelew kurs%' THEN 'currency_exchange'
            WHEN details LIKE 'tr.kart%' THEN 'card'
            WHEN details LIKE '%blik%' THEN 'blik'
            WHEN details LIKE 'przelew sepa%' THEN 'sepa_transfer'
            WHEN details LIKE 'przelew%' THEN 'transfer'
            WHEN COALESCE(details, '') = '' THEN 'pending'
            ELSE 'other'
        END AS payment_method
    FROM casted_data
),

classified_events AS (
    SELECT *,
        CASE
            -- 1. FX & Swaps
            WHEN payment_method = 'currency_exchange' THEN 'fx_swap'

            -- 2. Internal & Investments
            WHEN payment_method IN ('sepa_transfer', 'transfer')
                 AND (counterparty_details LIKE '{own_name_investment_pattern}' OR account_number = '{investment_iban}') THEN 'investment_outflow'
            WHEN description = 'own transfer' OR counterparty_details LIKE '%revolut%' OR counterparty_details RLIKE '{own_name_rlike}' THEN 'internal_transfer'

            -- 3. Debt & Loans
            WHEN counterparty_details LIKE '%spłata zadłużenia karty kredytowej%' OR description LIKE '%credit card repayment%' THEN 'credit_card_repayment'
            WHEN description LIKE '%rata kredytu%' THEN 'loan_repayment'

            -- 4. Income
            WHEN counterparty_details LIKE '{employer_name}' OR description LIKE '%wynagrodzenie%' THEN 'income_salary'

            WHEN counterparty_details LIKE '{consulting_client}' THEN 'income_consulting'

            WHEN counterparty_details RLIKE '{family_member_rlike}' AND description LIKE '%vendita%' THEN 'income_sale'

            -- 5. Family & Taxes
            WHEN (counterparty_details LIKE '{spouse_like}' OR counterparty_details LIKE '{spouse_like_alt}' OR counterparty_details RLIKE '{family_member_rlike}') THEN 'transfer_family'
            WHEN description LIKE '%obc.podatek%' OR counterparty_details LIKE '%urząd skarbowy%' OR counterparty_details LIKE '%ministerstwo finansow%' THEN 'expense_tax'

            -- 6. Fallbacks
            WHEN description LIKE '%blokada%' THEN 'expense'
            -- Life-insurance routing intentionally lives in gold's category training
            -- ("Life Insurance" sub of Health). Single source of routing for sub-category;
            -- silver only needs a generic 'expense' so include_in_expense=true.
            WHEN payment_method = 'pending' THEN 'pending_block'
            WHEN payment_method IN ('card', 'blik', 'transfer', 'sepa_transfer') THEN 'expense'
            ELSE 'unclassified'
        END AS event_type
    FROM classified_payment_methods
)

SELECT
    *,
    event_type NOT IN (
        'fx_swap', 'internal_transfer', 'credit_card_repayment', 'loan_repayment', 
        'pending_block', 'investment_outflow', 'income_salary', 'income_interest', 
        'income_consulting', 'income_sale', 'expense_reimbursement', 'transfer_family', 'unclassified'
    ) AS include_in_expense
FROM classified_events
