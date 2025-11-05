-- 📄 Purpose: Understand on which day of the week trains are most delayed.

-- 📆 Query: Average Delay by Day of Week
SELECT 
    day_of_week,
    ROUND(AVG(avg_arrival_delay), 2) AS avg_arrival_delay_minutes
FROM avg_delay_per_train
GROUP BY day_of_week
ORDER BY avg_arrival_delay_minutes DESC;
