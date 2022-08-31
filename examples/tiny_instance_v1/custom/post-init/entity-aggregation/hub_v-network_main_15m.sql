CREATE VIEW trend."hub_v-network_main_15m" AS
SELECT
  r.target_id AS entity_id,
  timestamp,
  count(*) AS samples,
  SUM("outside_temp") AS "outside_temp",
  SUM("inside_temp") AS "inside_temp",
  SUM("power_kwh") AS "power_kwh",
  SUM("freq_power") AS "freq_power"
FROM trend."hub_node_main_15m" t
JOIN relation."node->v-network" r ON t.entity_id = r.source_id
GROUP BY timestamp, r.target_id;

GRANT SELECT ON trend."hub_v-network_main_15m" TO minerva;
