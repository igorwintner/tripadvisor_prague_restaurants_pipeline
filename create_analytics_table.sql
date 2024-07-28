CREATE OR REPLACE TABLE `tripadvisor-prague-project.tripadvisor_prague_restaurants.tbl_analytics` AS (
  SELECT res.restaurant_name,
  rew.avg_rating,
  rew.popularity_rnk_all_restaurants, 
  rew.total_reviews_count,
  res.claimed,
  res.open_days_per_week,
  res.open_hours_per_week,
  res.awards,
  loc.longitude,
  loc.latitude,
  rng.eur_price_range_from,
  rng.eur_price_range_to,
  att.cuisines,
  att.features,
  diet.vegetarian_friendly,
  diet.vegan_options,
  diet.gluten_free
  FROM `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_reviews_fact` rew
  JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.restaurant_dim` res
    ON rew.restaurant_id = res.restaurant_id
  JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.location_dim` loc
    ON rew.location_id = loc.location_id
  JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.price_range_dim` rng
    ON rew.price_range_id = rng.price_range_id
  JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.attributes_dim` att
    ON rew.attributes_id = att.attributes_id
  JOIN `tripadvisor-prague-project.tripadvisor_prague_restaurants.dietary_dim` diet
    ON rew.dietary_id = diet.dietary_id)