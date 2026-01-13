SELECT *
FROM {{ ref('dim_part') }}
WHERE 1 = 2
