-- TWITTER DATABASE NORMALIZATION & SQL ANALYSIS
-- This file contains the complete SQL code for:
-- 1. Creating a normalized database schema
-- 2. Migrating data from the original denormalized table
-- 3. Analytical queries examining tweet patterns

-- =====================================================
-- PART 1: NORMALIZED SCHEMA CREATION
-- =====================================================

-- Locations table to normalize location data
CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(255) NOT NULL UNIQUE
);

-- Timezones table to normalize timezone information
CREATE TABLE time_zones (
    timezone_id SERIAL PRIMARY KEY,
    timezone_name VARCHAR(100) NOT NULL,
    utc_offset INT,
    UNIQUE (timezone_name, utc_offset)
);

-- Users table with normalized user information
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    user_screen_name VARCHAR(100) NOT NULL,
    user_description TEXT,
    user_followers_count INT NOT NULL DEFAULT 0,
    user_friends_count INT NOT NULL DEFAULT 0,
    user_status_count INT NOT NULL DEFAULT 0,
    user_created_at TIMESTAMP,
    user_lang VARCHAR(10),
    location_id INT REFERENCES locations(location_id),
    timezone_id INT REFERENCES time_zones(timezone_id)
);

-- Tweets table with normalized tweet information
CREATE TABLE tweets (
    tweet_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    text VARCHAR(280) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    retweet_count INT NOT NULL DEFAULT 0,
    tweet_source VARCHAR(255),
    tweet_type VARCHAR(20) NOT NULL DEFAULT 'original', -- 'original' or 'reply'
    parent_tweet_id BIGINT REFERENCES tweets(tweet_id) -- For replies
);

-- Retweets relationship table
CREATE TABLE retweets (
    retweet_id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(user_id),
    original_tweet_id BIGINT NOT NULL REFERENCES tweets(tweet_id),
    retweet_time TIMESTAMP NOT NULL,
    UNIQUE (user_id, original_tweet_id)
);

-- Hashtags table
CREATE TABLE hashtags (
    hashtag_id SERIAL PRIMARY KEY,
    hashtag VARCHAR(144) NOT NULL UNIQUE
);

-- Junction table for many-to-many relationship between tweets and hashtags
CREATE TABLE hashtag_tweets (
    hashtag_id INT NOT NULL REFERENCES hashtags(hashtag_id),
    tweet_id BIGINT NOT NULL REFERENCES tweets(tweet_id),
    PRIMARY KEY (hashtag_id, tweet_id)
);

-- Create indexes for performance
CREATE INDEX idx_tweets_user_id ON tweets(user_id);
CREATE INDEX idx_tweets_parent_id ON tweets(parent_tweet_id);
CREATE INDEX idx_retweets_user_id ON retweets(user_id);
CREATE INDEX idx_retweets_tweet_id ON retweets(original_tweet_id);
CREATE INDEX idx_hashtag_tweets_hashtag_id ON hashtag_tweets(hashtag_id);
CREATE INDEX idx_hashtag_tweets_tweet_id ON hashtag_tweets(tweet_id);
CREATE INDEX idx_users_location ON users(location_id);
CREATE INDEX idx_users_timezone ON users(timezone_id);
CREATE INDEX idx_users_lang ON users(user_lang);

-- =====================================================
-- PART 2: DATA MIGRATION
-- =====================================================

-- Step 1: Populate locations table
INSERT INTO locations (location_name)
SELECT DISTINCT user_location
FROM bad_giant_table
WHERE user_location IS NOT NULL AND user_location != '';

-- Step 2: Populate time_zones table
INSERT INTO time_zones (timezone_name, utc_offset)
SELECT DISTINCT user_time_zone, user_utc_offset
FROM bad_giant_table
WHERE user_time_zone IS NOT NULL AND user_time_zone != '';

-- Step 3: Populate users table
INSERT INTO users (
    user_id, user_name, user_screen_name, user_description,
    user_followers_count, user_friends_count, user_status_count,
    user_created_at, user_lang, location_id, timezone_id
)
SELECT DISTINCT
    user_id, 
    user_name, 
    user_screen_name, 
    user_description,
    user_followers_count, 
    user_friends_count, 
    user_statuses_count,
    timestamp_ms::timestamp, 
    user_lang,
    l.location_id,
    tz.timezone_id
FROM bad_giant_table bgt
LEFT JOIN locations l ON bgt.user_location = l.location_name
LEFT JOIN time_zones tz ON bgt.user_time_zone = tz.timezone_name AND bgt.user_utc_offset = tz.utc_offset
WHERE user_id IS NOT NULL;

-- Step 4: Populate tweets table
INSERT INTO tweets (
    tweet_id, user_id, text, created_at, 
    retweet_count, tweet_source, tweet_type, parent_tweet_id
)
SELECT 
    id, 
    user_id, 
    text, 
    created_at,
    retweet_count, 
    source,
    CASE 
        WHEN in_reply_to_status_id IS NOT NULL THEN 'reply'
        ELSE 'original'
    END,
    in_reply_to_status_id
FROM bad_giant_table
WHERE id IS NOT NULL;

-- Step 5: Populate retweets table
INSERT INTO retweets (user_id, original_tweet_id, retweet_time)
SELECT user_id, retweet_of_tweet_id, created_at
FROM bad_giant_table
WHERE retweet_of_tweet_id IS NOT NULL;

-- Step 6: Extract and populate hashtags
WITH all_hashtags AS (
    SELECT hashtag1 AS hashtag FROM bad_giant_table WHERE hashtag1 IS NOT NULL AND hashtag1 != ''
    UNION
    SELECT hashtag2 AS hashtag FROM bad_giant_table WHERE hashtag2 IS NOT NULL AND hashtag2 != ''
    UNION
    SELECT hashtag3 AS hashtag FROM bad_giant_table WHERE hashtag3 IS NOT NULL AND hashtag3 != ''
    UNION
    SELECT hashtag4 AS hashtag FROM bad_giant_table WHERE hashtag4 IS NOT NULL AND hashtag4 != ''
    UNION
    SELECT hashtag5 AS hashtag FROM bad_giant_table WHERE hashtag5 IS NOT NULL AND hashtag5 != ''
    UNION
    SELECT hashtag6 AS hashtag FROM bad_giant_table WHERE hashtag6 IS NOT NULL AND hashtag6 != ''
)
INSERT INTO hashtags (hashtag)
SELECT DISTINCT hashtag FROM all_hashtags;

-- Step 7: Create hashtag-tweet relationships
WITH hashtag_tweet_relations AS (
    SELECT id AS tweet_id, hashtag1 AS hashtag FROM bad_giant_table 
    WHERE hashtag1 IS NOT NULL AND hashtag1 != ''
    UNION
    SELECT id AS tweet_id, hashtag2 AS hashtag FROM bad_giant_table 
    WHERE hashtag2 IS NOT NULL AND hashtag2 != ''
    UNION
    SELECT id AS tweet_id, hashtag3 AS hashtag FROM bad_giant_table 
    WHERE hashtag3 IS NOT NULL AND hashtag3 != ''
    UNION
    SELECT id AS tweet_id, hashtag4 AS hashtag FROM bad_giant_table 
    WHERE hashtag4 IS NOT NULL AND hashtag4 != ''
    UNION
    SELECT id AS tweet_id, hashtag5 AS hashtag FROM bad_giant_table 
    WHERE hashtag5 IS NOT NULL AND hashtag5 != ''
    UNION
    SELECT id AS tweet_id, hashtag6 AS hashtag FROM bad_giant_table 
    WHERE hashtag6 IS NOT NULL AND hashtag6 != ''
)
INSERT INTO hashtag_tweets (tweet_id, hashtag_id)
SELECT DISTINCT r.tweet_id, h.hashtag_id
FROM hashtag_tweet_relations r
JOIN hashtags h ON r.hashtag = h.hashtag;

-- =====================================================
-- PART 3: ANALYTICAL QUERIES
-- =====================================================

-- 1. LANGUAGE DISTRIBUTION ANALYSIS

-- 1.1 Total number of tweets
SELECT COUNT(*) AS total_tweets
FROM tweets;

-- 1.2 Tweets distribution across languages
SELECT u.user_lang, COUNT(t.tweet_id) AS tweet_count
FROM tweets t
JOIN users u ON t.user_id = u.user_id
GROUP BY u.user_lang
ORDER BY tweet_count DESC;

-- 1.3 Fraction of tweets and users by language
SELECT
    u.user_lang,
    COUNT(t.tweet_id) * 1.0 / (SELECT COUNT(*) FROM tweets) AS tweet_fraction,
    COUNT(DISTINCT u.user_id) * 1.0 / (SELECT COUNT(*) FROM users) AS user_fraction
FROM tweets t
JOIN users u ON t.user_id = u.user_id
GROUP BY u.user_lang
ORDER BY tweet_fraction DESC;

-- 2. RETWEETING BEHAVIOR ANALYSIS

-- 2.1 Fraction of tweets that are retweets
WITH 
    total_tweets AS (
        SELECT COUNT(*) AS count_tweets
        FROM tweets
    ),
    total_retweets AS (
        SELECT COUNT(DISTINCT original_tweet_id) AS count_retweets
        FROM retweets
    )
SELECT 
    CAST(total_retweets.count_retweets AS FLOAT) / 
    (total_tweets.count_tweets + total_retweets.count_retweets) AS fraction_retweets
FROM 
    total_tweets,
    total_retweets;

-- 2.2 Average number of retweets per tweet
SELECT
    AVG(retweet_count) AS average_retweets
FROM tweets;

-- 2.3 Fraction of tweets never retweeted
SELECT
    (SELECT COUNT(*) FROM tweets WHERE retweet_count = 0) / COUNT(*) AS no_retweets_fraction
FROM tweets;

-- 2.4 Fraction of tweets retweeted fewer times than average
WITH avg_retweets AS (
    SELECT AVG(retweet_count) AS average_retweets FROM tweets
)
SELECT
    (SELECT COUNT(*) FROM tweets WHERE retweet_count < (SELECT average_retweets FROM avg_retweets))/ COUNT(*) AS less_than_average_retweets_fraction
FROM tweets;

-- 3. HASHTAG ANALYSIS

-- 3.1 Number of distinct hashtags
SELECT COUNT(DISTINCT hashtag) AS distinct_hashtags
FROM hashtags;

-- 3.2 Top ten most popular hashtags
SELECT hashtag, COUNT(ht.tweet_id) AS usage_count
FROM hashtags h
JOIN hashtag_tweets ht ON h.hashtag_id = ht.hashtag_id
GROUP BY hashtag
ORDER BY usage_count DESC
LIMIT 10;

-- 3.3 Top three hashtags per language
WITH hashtag_counts AS (
    SELECT 
        h.hashtag,
        u.user_lang,
        COUNT(*) AS usage_count
    FROM 
        hashtags h
    INNER JOIN hashtag_tweets ht ON h.hashtag_id = ht.hashtag_id
    INNER JOIN tweets t ON ht.tweet_id = t.tweet_id
    INNER JOIN users u ON t.user_id = u.user_id
    GROUP BY h.hashtag, u.user_lang
),
ranked_hashtags AS (
    SELECT 
        user_lang,
        hashtag,
        usage_count,
        RANK() OVER (PARTITION BY user_lang ORDER BY usage_count DESC) AS rank
    FROM 
        hashtag_counts
)
SELECT 
    user_lang,
    hashtag,
    usage_count
FROM 
    ranked_hashtags
WHERE 
    rank <= 3
ORDER BY 
    user_lang, rank;

-- 4. CONVERSATION PATTERN ANALYSIS

-- 4.1 Tweets neither replies nor replied to
SELECT COUNT(*)
FROM tweets t
WHERE t.tweet_type = 'original'
  AND t.parent_tweet_id IS NULL;

-- 4.2 Probability of same language in replies
WITH UserReplies AS (
    SELECT
        t1.user_id AS user1_id,
        t2.user_id AS user2_id,
        u1.user_lang AS user1_lang,
        u2.user_lang AS user2_lang
    FROM tweets t1
    JOIN tweets t2 ON t1.tweet_id = t2.parent_tweet_id
    JOIN users u1 ON t1.user_id = u1.user_id
    JOIN users u2 ON t2.user_id = u2.user_id
)
SELECT
    (COUNT(*) FILTER (WHERE user1_lang = user2_lang) * 1.0) / COUNT(*) AS same_language_probability
FROM UserReplies;

-- 4.3 Probability of same language for arbitrary users
WITH user_language_counts AS (
    SELECT user_lang, COUNT(*) AS num_users
    FROM users
    GROUP BY user_lang
),
language_pairs AS (
    SELECT user_lang, 
           (num_users * (num_users - 1)) / 2 AS num_pairs
    FROM user_language_counts
    WHERE num_users > 1
),
total_pairs AS (
    SELECT (COUNT(*) * (COUNT(*) - 1)) / 2 AS total_pairs
    FROM users
)
SELECT
    SUM(num_pairs) / (SELECT total_pairs FROM total_pairs) AS probability_same_language
FROM language_pairs;

-- =====================================================
-- PART 4: ADDITIONAL ANALYTICAL QUERIES
-- =====================================================

-- Most active users (by tweet count)
SELECT u.user_screen_name, COUNT(t.tweet_id) AS tweet_count
FROM tweets t
JOIN users u ON t.user_id = u.user_id
GROUP BY u.user_screen_name
ORDER BY tweet_count DESC
LIMIT 10;

-- Most influential users (by average retweets per tweet)
SELECT 
    u.user_screen_name, 
    AVG(t.retweet_count) AS avg_retweets,
    COUNT(t.tweet_id) AS tweet_count
FROM tweets t
JOIN users u ON t.user_id = u.user_id
GROUP BY u.user_screen_name
HAVING COUNT(t.tweet_id) >= 5  -- Minimum 5 tweets to be considered
ORDER BY avg_retweets DESC
LIMIT 10;

-- Tweet activity by hour of day
SELECT 
    EXTRACT(HOUR FROM created_at) AS hour_of_day,
    COUNT(*) AS tweet_count
FROM tweets
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- Correlation between follower count and retweet rate
SELECT 
    CASE 
        WHEN u.user_followers_count < 100 THEN '<100'
        WHEN u.user_followers_count < 1000 THEN '100-999'
        WHEN u.user_followers_count < 10000 THEN '1K-9.9K'
        WHEN u.user_followers_count < 100000 THEN '10K-99.9K'
        ELSE '100K+'
    END AS follower_range,
    AVG(t.retweet_count) AS avg_retweets,
    COUNT(*) AS tweet_count
FROM tweets t
JOIN users u ON t.user_id = u.user_id
GROUP BY follower_range
ORDER BY 
    CASE follower_range
        WHEN '<100' THEN 1
        WHEN '100-999' THEN 2
        WHEN '1K-9.9K' THEN 3
        WHEN '10K-99.9K' THEN 4
        ELSE 5
    END;

-- Distribution of hashtag usage (number of hashtags per tweet)
WITH hashtags_per_tweet AS (
    SELECT
        t.tweet_id,
        COUNT(ht.hashtag_id) AS num_hashtags
    FROM
        tweets t
    LEFT JOIN hashtag_tweets ht ON t.tweet_id = ht.tweet_id
    GROUP BY t.tweet_id
)
SELECT
    num_hashtags,
    COUNT(*) AS tweet_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tweets) AS percentage
FROM
    hashtags_per_tweet
GROUP BY
    num_hashtags
ORDER BY
    num_hashtags;

-- Co-occurring hashtags (pairs of hashtags that appear together)
WITH hashtag_pairs AS (
    SELECT
        h1.hashtag AS hashtag1,
        h2.hashtag AS hashtag2,
        COUNT(*) AS co_occurrence_count
    FROM
        hashtag_tweets ht1
    JOIN hashtag_tweets ht2 ON ht1.tweet_id = ht2.tweet_id AND ht1.hashtag_id < ht2.hashtag_id
    JOIN hashtags h1 ON ht1.hashtag_id = h1.hashtag_id
    JOIN hashtags h2 ON ht2.hashtag_id = h2.hashtag_id
    GROUP BY
        h1.hashtag, h2.hashtag
)
SELECT
    hashtag1,
    hashtag2,
    co_occurrence_count
FROM
    hashtag_pairs
ORDER BY
    co_occurrence_count DESC
LIMIT 20;
