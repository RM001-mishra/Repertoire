use game_analysis;
-- Problem Statement - Game Analysis dataset
-- 1) Players play a game divided into 3-levels (L0,L1 and L2)
-- 2) Each level has 3 difficulty levels (Low,Medium,High)
-- 3) At each level,players have to kill the opponents using guns/physical fight
-- 4) Each level has multiple stages at each difficulty level.
-- 5) A player can only play L1 using its system generated L1_code.
-- 6) Only players who have played Level1 can possibly play Level2 
--    using its system generated L2_code.
-- 7) By default a player can play L0.
-- 8) Each player can login to the game using a Dev_ID.
-- 9) Players can earn extra lives at each stage in a level.

alter table pd alter column L1_Status set data type varchar(30);
alter table pd alter column L2_Status set data type varchar(30);
alter table pd alter column P_ID set data type int;
alter table pd add primary key (P_ID);

alter table ld rename column timestamp  to start_datetime 
alter table ld alter column start_datetime set data type TIMESTAMP WITHOUT TIME ZONE;
alter table ld alter column Dev_Id set data type varchar(10);
alter table ld alter column Difficulty set data type varchar(15);
alter table ld add primary key(P_ID,Dev_id,start_datetime);

-- pd (P_ID,PName,L1_status,L2_Status,L1_code,L2_Code)
select * from pd
-- ld (P_ID,Dev_ID,start_time,stages_crossed,level,difficulty,kill_count,
-- headshots_count,score,lives_earned)
select * from ld

-- Q1) Extract P_ID,Dev_ID,PName and Difficulty_level of all players 
-- at level 0

SELECT ld.P_ID, ld.Dev_ID, pd.PName, ld.Difficulty
FROM pd
JOIN ld ON pd.P_ID = ld.P_ID
WHERE ld.Level = 0;

-- Q2) Find Level1_code wise Avg_Kill_Count where lives_earned is 2 and atleast
--    3 stages are crossed

SELECT pd.L1_Code, AVG(ld.Kill_Count) AS Avg_Kill_Count
FROM pd
JOIN ld ON pd.P_ID = ld.P_ID
WHERE ld.Level = 1
  AND ld.Lives_Earned = 2
  AND ld.Stages_crossed >= 3
GROUP BY pd.L1_Code; 

-- Q3) Find the total number of stages crossed at each diffuculty level
-- where for Level2 with players use zm_series devices. Arrange the result
-- in decsreasing order of total number of stages crossed.

SELECT Difficulty, SUM(Stages_crossed) AS Total_Stages_Crossed
FROM ld
JOIN  pd ON ld.P_ID = pd.P_ID
WHERE ld.Level = 2 AND ld.Dev_ID LIKE 'zm_%'
GROUP BY ld.Difficulty
ORDER BY Total_Stages_Crossed DESC; 

-- Q4) Extract P_ID and the total number of unique dates for those players 
-- who have played games on multiple days.

SELECT P_ID, COUNT(DISTINCT DATE(start_datetime)) AS Total_Unique_Dates
FROM ld
GROUP BY P_ID
HAVING COUNT(DISTINCT DATE(start_datetime)) > 1;

-- Q5) Find P_ID and level wise sum of kill_counts where kill_count
-- is greater than avg kill count for the Medium difficulty.

SELECT P_ID, ld.Level, SUM(kill_count) AS total_kill_count
FROM ld
JOIN (
    SELECT level, difficulty, AVG(kill_count) AS avg_kill_count
    FROM ld
    WHERE difficulty = 'Medium'
    GROUP BY ld.level, difficulty
) AS avg_kill ON ld.level = avg_kill.level AND ld.difficulty = avg_kill.difficulty
WHERE kill_count > avg_kill.avg_kill_count
GROUP BY P_ID, ld.Level;

-- Q6)  Find Level and its corresponding Level code wise sum of lives earned 
-- excluding level 0. Arrange in asecending order of level.

SELECT ld.Level, pd.L1_Code AS Level_Code, SUM(ld.lives_earned) AS Total_Lives_Earned
FROM pd
JOIN ld ON pd.P_ID = ld.P_ID
WHERE ld.Level > 0
GROUP BY ld.Level, pd.L1_Code
ORDER BY ld.Level ASC;

-- Q7) Find Top 3 score based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well. 

WITH RankedScores AS (
    SELECT Dev_ID, Score, Difficulty,
           ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY Score ASC) AS Rank
    FROM ld
)
SELECT Dev_ID, Score, Difficulty, Rank
FROM RankedScores
WHERE Rank <= 3;

-- Q8) Find first_login datetime for each device id

SELECT Dev_ID, MIN(start_datetime) AS first_login_datetime
FROM ld
GROUP BY Dev_ID;

-- Q9) Find Top 5 score based on each difficulty level and Rank them in 
-- increasing order using Rank. Display dev_id as well.

WITH RankedScores AS (
    SELECT Dev_ID, Score, Difficulty,
           RANK() OVER (PARTITION BY Difficulty ORDER BY Score DESC) AS Rank
    FROM ld
)
SELECT Dev_ID, Score, Difficulty, Rank
FROM RankedScores
WHERE Rank <= 5;

-- Q10) Find the device ID that is first logged in(based on start_datetime) 
-- for each player(p_id). Output should contain player id, device id and 
-- first login datetime.

SELECT P_ID, Dev_ID, MIN(start_datetime) AS first_login_datetime
FROM ld
GROUP BY P_ID, Dev_ID;

-- Q11) For each player and date, how many kill_count played so far by the player. That is, the total number of games played -- by the player until that date.
-- a) window function
SELECT P_ID, start_datetime:: Date, 
    SUM(kill_count) OVER (PARTITION BY P_ID ORDER BY start_datetime) AS total_kills_so_far
FROM ld;

-- b) without window function
SELECT ld.P_ID, ld.start_datetime::Date AS date,
       SUM(ld.kill_count) AS total_kill_count
FROM ld
GROUP BY ld.P_ID, ld.start_datetime ORDER BY ld.P_ID, date;


-- Q12) Find the cumulative sum of stages crossed over a start_datetime 

SELECT P_ID, Dev_ID, start_datetime, stages_crossed,
       SUM(stages_crossed) OVER (PARTITION BY P_ID, Dev_ID ORDER BY start_datetime) AS cumulative_stages_crossed
FROM ld;

-- Q13) Find the cumulative sum of an stages crossed over a start_datetime 
-- for each player id but exclude the most recent start_datetime

WITH CumulativeSum AS (
    SELECT P_ID, start_datetime, stages_crossed, SUM(stages_crossed) 
	OVER (PARTITION BY P_ID ORDER BY start_datetime ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cumulative_sum
    FROM ld
)
SELECT P_ID, start_datetime, cumulative_sum
FROM CumulativeSum;

-- Q14) Extract top 3 highest sum of score for each device id and the corresponding player_id

WITH RankedScores AS (
    SELECT ld.P_ID, ld.Dev_ID,SUM(ld.score) AS total_score,
        ROW_NUMBER() OVER (PARTITION BY ld.Dev_ID ORDER BY SUM(ld.score) DESC) AS rank
    FROM ld
    GROUP BY ld.P_ID,ld.Dev_ID
)
SELECT rs.P_ID, rs.Dev_ID, rs.total_score
FROM RankedScores rs
WHERE rs.rank <= 3;

-- Q15) Find players who scored more than 50% of the avg score scored by sum of 
-- scores for each player_id

SELECT ld.P_ID, ld.Score, ps.Total_Score, ps.Total_Score * 0.5 AS Fifty_Percent_Average
FROM ld
JOIN (
    SELECT P_ID, SUM(Score) AS Total_Score
    FROM ld
    GROUP BY P_ID
) AS ps ON ld.P_ID = ps.P_ID
WHERE ld.Score > ps.Total_Score * 0.5;

-- Q16) Create a stored procedure to find top n headshots_count based on each dev_id and Rank them in increasing order
-- using Row_Number. Display difficulty as well.

CREATE FUNCTION GetTopNHeadshots(n INTEGER)
RETURNS TABLE (Dev_ID VARCHAR(255), Headshots_Count INTEGER, Difficulty VARCHAR(255), Rank INTEGER) AS $$
BEGIN
    RETURN QUERY
    WITH RankedHeadshots AS (
        SELECT Dev_ID, Headshots_Count, Difficulty,
               ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY Headshots_Count) AS Rank
        FROM level_details
    )
    SELECT Dev_ID, Headshots_Count, Difficulty, Rank
    FROM RankedHeadshots
    WHERE Rank <= n;
END;
$$ LANGUAGE plpgsql;


-- Q17) Create a function to return sum of Score for a given player_id.

CREATE FUNCTION GetTotalScore(player_id INT) RETURNS INT
language plpgsql as $$
declare total_score INT;
BEGIN
    SELECT SUM(score) INTO total_score
    FROM ld
    WHERE P_ID = player_id;
    RETURN total_score;
END;
$$ ;
