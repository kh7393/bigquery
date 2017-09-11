SELECT
  fullVisitorId AS vid,
  CONCAT(fullVisitorId,"-",STRING(visitId)) AS sid,
  date,
  # query for total events - youtube
  SUM(IF(hits.eventInfo.eventCategory = 'youtube', 1,0)) AS youtubeEvents,
  # query whether the session has youtube event or not [unique event]
  MAX(IF(hits.eventInfo.eventCategory = 'youtube', 1,0)) AS youtubeCheck,
  # counts as unique events
  # query for total events - ecommerce impressions
  SUM(IF(REGEXP_MATCH(hits.eventInfo.eventCategory, r'ecommerce') AND hits.eventInfo.eventAction = 'impression', 1, 0)) as EcommerceImpressionEvents,
  # SUM(IF(hits.eventInfo.eventCategory LIKE '%ecommerce%' AND hits.eventInfo.eventAction = 'impression', 1, 0)) as EcommerceImpressionEvents,
  # query whether the session has ecommerce impression event or not [unique event]
  MAX(IF(hits.eventInfo.eventCategory = 'ecommerce' AND hits.eventInfo.eventAction = 'impression', 1,0)) AS ecommerceImpressionCheck,
  # search check: whether the session has flight search or not
  MAX(IF(REGEXP_MATCH(hits.page.pagePath, r'/vbook/actions/(selectflights|selectitinerary|(mobi/|)createitinerary)($|\?)'),1,0)) AS flightSearchCheck,
  # trans check: whether the session has transaction or not
  MAX(IF(totals.transactions IS NULL, 0,1)) AS transCheck,
  # query tables by date range
 FROM
  TABLE_DATE_RANGE([125557395.ga_sessions_], TIMESTAMP('20170210'), TIMESTAMP('20170211')) as Table1
  # don't group by visitor or session id if it's date-specific
 GROUP BY
  date,
  vid,
  sid




  SELECT
  MAX(CASE WHEN customDimensions.index = 82 THEN customDimensions.value ELSE NULL END) WITHIN RECORD as customer_id,
  visitId,
  hits.hitNumber,
  GROUP_CONCAT(CASE WHEN hits.customDimensions.index = 13 THEN hits.customDimensions.value ELSE NULL END) WITHIN hits as searches
FROM
  [airnz-ga-bigquery:125557395.ga_sessions_20170619]
HAVING
  customer_id IS NOT NULL AND
  searches = 'syd'
LIMIT 100

SELECT count(hits.eventInfo.eventCategory) FROM TABLE_DATE_RANGE([125557395.ga_sessions_], TIMESTAMP('20170210'), TIMESTAMP('20170211')) 
  WHERE hits.eventInfo.eventCategory = 'ecommerce' AND hits.eventInfo.eventAction = 'impression' AS ecommerceImpressionEvents
