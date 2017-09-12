SELECT
  fullVisitorId AS vid,
  # exact count distinct gives the session volume in GA
  CONCAT(fullVisitorId,"-",STRING(visitId)) AS sid,
  date,
  # query for hits with custom dimension - storefront
  SUM(IF(hits.customDimensions.index = 69, 1,0)) AS storefrontDimension,
  # query for no. of sessions with storefront check
  #MAX(IF(hits.customDimensions.index = 69, 1,0)) AS storefrontCheck,
  # query for total events - youtube
  SUM(IF(hits.eventInfo.eventCategory = 'youtube', 1,0)) AS youtubeEvents,
  # query for no. of sessions with youtube events
 # MAX(IF(hits.eventInfo.eventCategory = 'youtube', 1,0)) AS youtubeCheck,
  # counts as unique events
  # query for total events - ecommerce impressions
  SUM(IF(REGEXP_MATCH(hits.eventInfo.eventCategory, r'ecommerce') AND hits.eventInfo.eventAction = 'impression', 1, 0)) as EcommerceImpressionEvents,
  # SUM(IF(hits.eventInfo.eventCategory LIKE '%ecommerce%' AND hits.eventInfo.eventAction = 'impression', 1, 0)) as EcommerceImpressionEvents,
  # query for no. of sessions with ecommerce impressions
  #MAX(IF(hits.eventInfo.eventCategory = 'ecommerce' AND hits.eventInfo.eventAction = 'impression', 1,0)) AS ecommerceImpressionCheck,
  # https://github.com/sparklineanalytics/analysts/blob/master/AirNZ/GUID%20Roll%20Up%20Analysis/base_table.sql
  # query for no. of sessions with flight search
  #MAX(IF(REGEXP_MATCH(hits.page.pagePath,r'/vbook/actions/(selectflights|selectitinerary|(mobi/|)createitinerary)') and hits.type = 'PAGE',1,0)) AS flightSearchCheck,
  # query for total flight searches 
  SUM(IF(REGEXP_MATCH(hits.page.pagePath,r'/vbook/actions/(selectflights|selectitinerary|(mobi/|)createitinerary)') and hits.type = 'PAGE',1,0)) AS flightSearches,
  # query for no. of sessions with flight bookings
  #MAX(IF(REGEXP_MATCH(hits.page.pagePath, r'/vbook/actions/(mobi/|)bookingconfirmation') AND hits.type = 'PAGE', 0,1)) AS flightbookingCheck,
  SUM(IF(REGEXP_MATCH(hits.page.pagePath, r'/vbook/actions/(mobi/|)bookingconfirmation') AND hits.type = 'PAGE', 0,1)) AS flightbookings
    # query tables by date range
 FROM
  TABLE_DATE_RANGE([125557395.ga_sessions_], TIMESTAMP('20170210'), TIMESTAMP('20170211')) as Table1
  # provides the no. of sessions in GA. Remove this criteria to find # of hits (non-check)/ number of unique events (checks)
 WHERE
  totals.visits IS NOT NULL
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

