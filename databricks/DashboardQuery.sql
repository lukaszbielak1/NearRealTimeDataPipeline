WITH CTE AS (
  SELECT
    routeShortName,
    MAX(generated) as generated
  FROM
    bus_monitor
  group by
    routeShortName
)
SELECT
  bus.generated,
  headsign,
  bus.routeShortName,
  concat(bus.routeShortName, "-",bus.headsign) as Name,
  lat,
  lon
FROM
  bus_monitor bus
  INNER JOIN CTE cte ON bus.generated = cte.generated 
                     AND bus.routeShortName = cte.routeShortName