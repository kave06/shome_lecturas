SELECT count(*)
FROM energy_counter
;

SELECT *
FROM official_close_read
WHERE phy_counter_id SIMILAR TO '58238207'
;

SELECT CAST(value AS DOUBLE PRECISION) as requested
FROM iotgate.node_component_state
WHERE stream_id = 236
ORDER BY observed_at DESC
LIMIT 1
;

SELECT id
FROM official_close
ORDER BY init_date DESC
LIMIT 1
;

DELETE
FROM official_close_read
-- WHERE official_close_id BETWEEN 23 AND 62
-- WHERE official_close_id=63
;

DELETE
FROM official_close
-- WHERE id BETWEEN 23 AND 62
-- WHERE id=58
;

SELECT *
FROM official_close_read
WHERE official_close_id =9
;

SELECT *
FROM official_close_read
WHERE official_close_id = 63
  ;
SELECT *
FROM official_close_read
WHERE official_close_id = 9
;

SELECT count(*)
FROM energy_counter, iotgate.node_component_state
WHERE iotgate_component_id=iotgate.node_component_state.stream_id
;

SELECT DISTINCT stream_id
FROM iotgate.node_component_state
-- WHERE observed_at
;

SELECT phy_counter_id, iotgate_component_id
FROM energy_counter
WHERE iotgate_component_id IN
      (
        SELECT iotgate_component_id
        FROM energy_counter
        WHERE type='distribution'
      )
 ;

SELECT stream_id, EXTRACT(MONTH FROM observed_at), MAX(value)
FROM iotgate.node_component_state
WHERE EXTRACT(MONTH FROM observed_at)=6
GROUP BY stream_id, EXTRACT(MONTH FROM observed_at)
;


SELECT stream_id, EXTRACT(MONTH FROM observed_at), MAX(value)
FROM iotgate.node_component_state
WHERE EXTRACT(MONTH FROM observed_at)=6 AND stream_id iN
      (
        SELECT iotgate_component_id
        FROM energy_counter
        WHERE iotgate_component_id IN
              (
                SELECT iotgate_component_id
                FROM energy_counter
                WHERE type='distribution'
              )
      )
GROUP BY stream_id, EXTRACT(MONTH FROM observed_at)
;

SELECT tmp.valueMonth, energy_counter.phy_counter_id, tmp.month, tmp.stream_id
FROM energy_counter,
(
SELECT stream_id, EXTRACT(MONTH FROM observed_at) AS month, MAX(value) as valueMonth
FROM iotgate.node_component_state, energy_counter
WHERE EXTRACT(MONTH FROM observed_at)=6 AND stream_id iN
    (
    SELECT iotgate_component_id
    FROM energy_counter
    WHERE iotgate_component_id IN
        (
        SELECT iotgate_component_id
        FROM energy_counter
        WHERE type='distribution'
        )
    )
GROUP BY stream_id, EXTRACT(MONTH FROM observed_at)
) AS tmp
WHERE energy_counter.iotgate_component_id=tmp.stream_id
;

UPDATE official_close_read
SET requested=222222
WHERE phy_counter_id='FF110807' AND
  official_close_id=21
;


WITH CTE_Duplicates AS
    (
    SELECT
         *,
        row_number()
    OVER(
          partition by phy_counter_id, official_close_id
          order by official_close_id
          )as rn
            from official_close_read
            WHERE official_close_id=22
        )
DELETE from CTE_Duplicates
WHERE phy_counter_id IN
    (
    SELECT phy_counter_id
      FROM CTE_Duplicates
      WHERE rn > 1
    )
;




select count(*)
from official_close_read
;

SELECT DISTINCT phy_counter_id
FROM official_close_read
;

SELECT count(*)
from official_close_read
where official_close_id=74
;

SELECT dwelling.name, t1.phy_counter_id, t1.requested, t1.measuring, t1.iotgate_component_id
FROM
    (
    SELECT
      requested,
      official_close_read.phy_counter_id,
      dwelling_id,
      official_close_read.measuring,
      en.iotgate_component_id
    FROM official_close_read
    LEFT JOIN energy_counter en
    ON official_close_read.phy_counter_id = en.phy_counter_id
    ) AS t1
LEFT JOIN dwelling
  ON t1.dwelling_id = dwelling.id
;
