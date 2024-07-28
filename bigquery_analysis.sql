-- Top 10 Most Popular Restaurants in Prague
SELECT rd.restaurant_name, rew.popularity_rnk_all_restaurants
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_dim` AS rd
JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_reviews_fact` AS rew
    ON rd.restaurant_id = rew.restaurant_id
WHERE rew.popularity_rnk_all_restaurants IS NOT NULL
ORDER BY rew.popularity_rnk_all_restaurants
LIMIT 10

-- Restaurants with the Highest Number of Reviews (Top 10)
SELECT rd.restaurant_name, rew.total_reviews_count
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_dim` AS rd
JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_reviews_fact` AS rew
    ON rd.restaurant_id = rew.restaurant_id
ORDER BY rew.total_reviews_count DESC
LIMIT 10

-- Top 50 Restaurants with the Most Awards
WITH AwardsArray AS (
    SELECT restaurant_name,ARRAY(
            SELECT TRIM(award)
            FROM UNNEST(REGEXP_EXTRACT_ALL(awards, r"'([^']*)'")) AS award
        ) AS awards_array
    FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_dim`
)
SELECT restaurant_name,
    ARRAY_LENGTH(awards_array) AS num_awards
FROM AwardsArray
ORDER BY num_awards DESC
LIMIT 50

-- Count of Restaurants, that are Claimed/Unclaimed by the Owner
SELECT 
    CASE 
        WHEN claimed IS TRUE THEN 'Claimed'
        ELSE 'Unclaimed'
    END AS claim_status,
    COUNT(*) AS count
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_dim`
GROUP BY claim_status

-- Number of Restaurants, that are vegetarian friendly
SELECT COUNT(*) AS vegetarian_friendly 
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.dietary_dim`
WHERE vegetarian_friendly IS TRUE

-- Number of Restaurants, that have vegan options
SELECT COUNT(*) AS vegan_options
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.dietary_dim`
WHERE vegan_options IS TRUE

-- Number of Restaurants, that have gluten free options
SELECT COUNT(*) AS gluten_free
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.dietary_dim`
WHERE gluten_free IS TRUE

-- Restaurants, that are Vegan, Vegetarian Friendly and Gluten-Free with Avg Ratings and Total Number of Reviews
SELECT rest.restaurant_name, rew.avg_rating, rew.total_reviews_count
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.dietary_dim` AS diet
JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_dim` AS rest
    ON diet.restaurant_id = rest.restaurant_id
JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_reviews_fact` AS rew
    ON rew.restaurant_id = rest.restaurant_id
WHERE vegetarian_friendly IS TRUE AND 
    vegan_options IS TRUE AND gluten_free IS TRUE
ORDER BY rew.avg_rating DESC

-- Most Popular Retaurants with Avg Rating and Price Range (Eur)
SELECT rest.restaurant_name, rew.popularity_rnk_all_restaurants, rew.avg_rating,
rng.eur_price_range_from,rng.eur_price_range_to
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_dim` AS rest
JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_reviews_fact` AS rew
  ON rest.restaurant_id = rew.restaurant_id
JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.price_range_dim` rng
  ON rest.restaurant_id = rng.restaurant_id
WHERE rew.popularity_rnk_all_restaurants IS NOT NULL AND
  rng.eur_price_range_from IS NOT NULL
ORDER BY rew.popularity_rnk_all_restaurants

-- Avg Price of all the Restaurants (Those with Price Range Available)
SELECT ROUND(AVG((eur_price_range_from + eur_price_range_to) / 2.0), 2) AS avg_price_eur
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.price_range_dim`
WHERE eur_price_range_from IS NOT NULL AND eur_price_range_to IS NOT NULL

-- Avg Opened Hours per Week
SELECT  ROUND(AVG(open_hours_per_week), 2) AS avg_opened_hour_per_week
FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_dim`

-- Top 10 Cuisines and their Count
SELECT cuisine, COUNT(*) AS count
FROM (
    SELECT TRIM(cuisine) AS cuisine
    FROM (SELECT
          REGEXP_EXTRACT_ALL(cuisines, r"'([^']*)'") AS cuisine_array
          FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.attributes_dim`
          ),
    UNNEST(cuisine_array) AS cuisine
)
GROUP BY cuisine
ORDER BY count DESC
LIMIT 10

-- Top 10 Features and their Count
SELECT feature, COUNT(*) AS count
FROM (
    SELECT TRIM(feature) AS feature
    FROM (SELECT
          REGEXP_EXTRACT_ALL(features, r"'([^']*)'") AS feature_array
          FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.attributes_dim`
          ),
    UNNEST(feature_array) AS feature
)
GROUP BY feature
ORDER BY count DESC
LIMIT 10