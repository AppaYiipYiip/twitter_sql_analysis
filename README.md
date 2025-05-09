# Twitter Database Normalization & SQL Analysis

![SQL](https://img.shields.io/badge/SQL-Analysis-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-orange)
![Data](https://img.shields.io/badge/Data-Twitter-lightblue)
![Normalization](https://img.shields.io/badge/Database-Normalization-green)

## Project Overview

This project demonstrates database normalization principles and SQL analytical capabilities using a dataset of approximately 100,000 tweets. Starting with a poorly designed single-table dataset, I:

1. Analyzed database design flaws and identified functional dependencies
2. Created a properly normalized database schema (up to 3NF)
3. Developed SQL migration scripts to transform the data
4. Wrote analytical queries to extract meaningful insights about user behavior and content patterns

## Database Design Process

### Initial Analysis of Design Flaws

The original dataset was provided as a single denormalized table (`bad_giant_table`) with significant flaws:

- **Redundant Data**: User information duplicated across every tweet from the same user
- **Non-Atomic Attributes**: Hashtags stored in separate columns (hashtag1-hashtag6)
- **No Referential Integrity**: No foreign key constraints for retweets or replies
- **Update Anomalies**: Changes to user details required updating multiple rows
- **Rigid Limitations**: Maximum of six hashtags per tweet imposed by schema

### Normalization Approach

I applied database normalization principles to redesign the schema:

1. **First Normal Form (1NF)**: Eliminated non-atomic attributes by creating a separate hashtags structure
2. **Second Normal Form (2NF)**: Removed partial dependencies by separating user information
3. **Third Normal Form (3NF)**: Eliminated transitive dependencies involving locations and timezones

### Entity-Relationship Design

The normalized schema includes:

```
┌───────────┐       ┌───────────┐       ┌───────────┐
│ Users     │       │ Tweets    │       │ Hashtags  │
├───────────┤       ├───────────┤       ├───────────┤
│ user_id   │◄─────┤ user_id   │       │ hashtag_id │
│ name      │       │ tweet_id  │◄─┐    │ hashtag   │
│ followers │       │ text      │  │    └───────────┘
│ lang      │       │ parent_id │  │          ▲
└───────────┘       └───────────┘  │          │
      ▲                  ▲         │          │
      │                  │         │    ┌──────────────┐
┌───────────┐      ┌───────────┐   │    │ HashtagTweet │
│ Locations │      │ Retweets  │   │    ├──────────────┤
├───────────┤      ├───────────┤   │    │ hashtag_id   │
│ loc_id    │      │ user_id   │   │    │ tweet_id     │
│ loc_name  │      │ orig_id   ├───┘    └──────────────┘
└───────────┘      └───────────┘
```

This design resolved the issues by:
- Storing user information only once
- Creating proper relationships between entities
- Supporting unlimited hashtags through a junction table
- Maintaining referential integrity with foreign keys

## SQL Analysis and Key Findings

After implementing the normalized schema, I developed analytical queries to extract insights:

### Language Distribution

```sql
SELECT
    u.user_lang,
    COUNT(t.tweet_id) * 1.0 / (SELECT COUNT(*) FROM tweets) AS tweet_fraction,
    COUNT(DISTINCT u.user_id) * 1.0 / (SELECT COUNT(*) FROM users) AS user_fraction
FROM tweets t
JOIN users u ON t.user_id = u.user_id
GROUP BY u.user_lang;
```

**Key Finding**: English (66%), Spanish (16%), and Japanese (9.5%) dominated the tweet volume, but the user distribution showed English (40%), Spanish (9.5%), and Japanese (6%), indicating users of these languages were disproportionately active.

### Retweet Behavior

```sql
WITH avg_retweets AS (
    SELECT AVG(retweet_count) AS average_retweets FROM tweets
)
SELECT
    (SELECT COUNT(*) FROM tweets WHERE retweet_count = 0) / COUNT(*) AS no_retweets_fraction,
    (SELECT COUNT(*) FROM tweets WHERE retweet_count < (SELECT average_retweets FROM avg_retweets))/ COUNT(*) AS less_than_average_retweets_fraction
FROM tweets;
```

**Key Finding**: A highly skewed distribution where 77% of tweets were never retweeted, while 95% had fewer retweets than the average (70.9). This suggests a power law distribution with a few "viral" tweets driving up the average.

### Hashtag Analysis

```sql
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
```

**Key Finding**: From over 9,300 unique hashtags, different language communities showed distinct preferences. English tweets favored #reasonsifailatbeingagirl, #oomf, and #honestyhour, while Japanese users preferred #countkun, #sougofollow, and #followmejp.

### Conversation Pattern Analysis

```sql
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
```

**Key Finding**: Users were much more likely to reply to others who shared their language setting (86%) compared to the baseline probability of two random users sharing a language (48%), showing strong language-based community structures.

## Technical Implementation Challenges

### Handling Retweets and Replies

I needed to decide how to model retweet relationships:

1. **Selected Approach**: Created a separate `retweets` table to track who retweeted what
   ```sql
   CREATE TABLE retweets (
       retweet_id SERIAL PRIMARY KEY,
       user_id BIGINT NOT NULL REFERENCES users(user_id),
       original_tweet_id BIGINT NOT NULL REFERENCES tweets(tweet_id),
       retweet_time TIMESTAMP NOT NULL
   );
   ```

2. For replies, I added type and parent references directly in the tweets table:
   ```sql
   tweet_type VARCHAR(20) NOT NULL DEFAULT 'original',
   parent_tweet_id BIGINT REFERENCES tweets(tweet_id)
   ```

### Complex Data Migration

Migration from the denormalized table required carefully ordered steps:

1. Extract entities with no dependencies first (locations, timezones, hashtags)
2. Populate core entities with simple foreign keys (users, tweets)
3. Create relationship entries last (hashtag-tweet connections, retweets)

```sql
-- Example: Extracting hashtags from multiple columns
WITH all_hashtags AS (
    SELECT hashtag1 AS hashtag FROM bad_giant_table WHERE hashtag1 IS NOT NULL
    UNION
    SELECT hashtag2 AS hashtag FROM bad_giant_table WHERE hashtag2 IS NOT NULL
    -- Additional hashtag columns...
)
INSERT INTO hashtags (hashtag)
SELECT DISTINCT hashtag FROM all_hashtags;
```

## Reflections and Insights

This project reinforced several important database design principles:

1. **Normalization Benefits**: The normalized schema reduced data redundancy by approximately 70%, improved query performance for complex analyses, and eliminated update anomalies.

2. **Query Optimization Techniques**: Using window functions, CTEs, and appropriate indexes significantly improved query performance, especially for complex analyses like top hashtags per language.

3. **Social Media Patterns**: The data revealed interesting social behaviors, including strong language homophily in conversations and highly skewed engagement patterns typical of social networks.

The skills applied in this project—database normalization, SQL query optimization, and data transformation—are directly applicable to real-world data engineering scenarios, especially when working with poorly structured source data that requires cleaning and restructuring for analysis.

## Technical Skills Demonstrated

- **Database Design**: Normalization principles, schema design, ER modeling
- **SQL Mastery**: Complex queries, window functions, CTEs, subqueries, aggregations
- **Data Transformation**: Converting denormalized data into a proper relational structure
- **Analytical Thinking**: Deriving meaningful patterns from social media data
