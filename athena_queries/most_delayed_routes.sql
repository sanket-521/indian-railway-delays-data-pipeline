-- 📄 Purpose: Identify routes (source → destination) with the worst average delays.

-- 🗺️ Query: Most Delayed Routes
SELECT 
    source_station_code,
    destination_station_code,
    ROUND(avg_delay_route, 2) AS avg_delay_minutes
FROM avg_delay_per_route
ORDER BY avg_delay_route DESC
LIMIT 10;

