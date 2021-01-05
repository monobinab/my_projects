  WITH
    billing_export_table AS (
  SELECT
    billing_account_id,
    service,
    sku,
    usage_start_time,
    usage_end_time,
    project,
    labels,
    system_labels,
    location,
    cost,
    currency,
    currency_conversion_rate,
    usage,
    ' Gross' AS cost_metric_type,
    0 AS credit_amount,
    invoice,
    cost_type
  FROM
    `data-analytics-pocs.billing.gcp_billing_export_v1_0090FE_ED3D81_AF8E3B`

  UNION ALL

  SELECT
    billing_account_id,
    service,
    sku,
    usage_start_time,
    usage_end_time,
    project,
    labels,
    system_labels,
    location,
    0 AS cost,
    currency,
    currency_conversion_rate,
    usage,
    c.name AS cost_metric_type,
    c.amount AS credit_amount,
    invoice,
    cost_type
  FROM
    `data-analytics-pocs.billing.gcp_billing_export_v1_0090FE_ED3D81_AF8E3B`
    JOIN UNNEST(credits) c
    WHERE c.name IS NOT NULL)
select
b.*
,p.*
from billing_export_table b
left outer join
(
SELECT 
  organization_name,
  projectid, 
  MAX(case when fs.folder_level = 1 then folder_name end) AS SVP,
  MAX(case when fs.folder_level = 2 then folder_name end) AS VP,
  MAX(case when fs.folder_level = 3 then folder_name end) AS Sr_Dir,
  MAX(case when fs.folder_level = 4 then folder_name end) AS Dir 
FROM `cardinal-data-piper-sbx.monobina_sbx.gcp_org_folder_hierarchy`
join unnest(folder_structure) fs
GROUP BY organization_name, projectid
) p on b.project.id = p.projectid
;

