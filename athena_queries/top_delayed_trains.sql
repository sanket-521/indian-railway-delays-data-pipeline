--  📄 Purpose: Find the trains with the highest average arrival delays.

-- 🧭 Query: Top 10 Most Delayed Trains
SELECT 
    train_no,
    train_name,
    ROUND(avg_arrival_delay, 2) AS avg_arrival_delay_minutes,
    ROUND(avg_departure_delay, 2) AS avg_departure_delay_minutes
FROM avg_delay_per_train
ORDER BY avg_arrival_delay DESC
LIMIT 10;
