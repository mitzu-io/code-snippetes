SELECT
  TIMESTAMP_SECONDS(CAST(JSON_VALUE(sli.period, '$.start') AS integer)) period_start,
  TIMESTAMP_SECONDS(CAST(JSON_VALUE(sli.period, '$.end') AS integer)) period_end,
  COALESCE(sp.name, si.customer_name) AS product_name,
  ss.customer,
  sc.metadata,
  (sli.amount/ 100) / GREATEST(1, DATE_DIFF(DATE( TIMESTAMP_SECONDS(CAST(JSON_VALUE(sli.period, '$.end') AS integer))), DATE(TIMESTAMP_SECONDS(CAST(JSON_VALUE(sli.period, '$.start') AS integer))), MONTH)) AS mrr,
  sli.amount / 100 AS amount_paid
FROM
  `dataset.stripe_invoice_line_items` sli
JOIN
  `dataset.stripe_invoices` si
ON
  si.id = sli.invoice_id
JOIN
  `dataset.stripe_customers` sc
ON
  sc.id = si.customer
LEFT JOIN
  `dataset.stripe_subscriptions` ss
ON
  ss.id = sli.subscription
LEFT JOIN
  `dataset.stripe_products` sp
ON
  sp.id = JSON_VALUE(ss.plan,'$.product')
WHERE
  TRUE
  AND amount != 0
  AND si.status IN ('paid', 'open')
