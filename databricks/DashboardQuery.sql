WITH CTE AS (
  SELECT
    vehicleCode,
    MAX(generated) as generated
  FROM
    bus_monitor
  group by
    vehicleCode
)
SELECT
  bus.generated,
  bus.headsign,
  bus.routeShortName,
  concat(bus.routeShortName, "-",bus.headsign) as Name,
  lat,
  lon
FROM
  bus_monitor bus
  INNER JOIN CTE cte ON bus.generated = cte.generated 
                     AND bus.vehicleCode = cte.vehicleCode
