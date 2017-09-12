SELECT
  fullVisitorId AS vid,
  # exact count distinct gives the session volume in GA
  CONCAT(fullVisitorId,"-",STRING(visitId)) AS sid,
  date,
  # query for hits with custom dimension - storefront
  # criteria for storefront
CASE
      WHEN REGEXP_MATCH(hits.page.hostname, r'grabaseat\.co\.nz') THEN 'GAS-NZ'
      WHEN REGEXP_MATCH(hits.page.hostname, r'(airn(z|ewzealand)|taxi|airpoints)\.co\.nz') THEN 'NZ'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.co\.uk') THEN 'UK'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.au') THEN 'AUS'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.ca') THEN 'Canada'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.cn') THEN 'China'
      WHEN REGEXP_MATCH(hits.page.hostname, r'(airnewzealand\.com\.cn|cn\.airnewzealand\.com)$') THEN 'China EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.co\.jp') THEN 'Japan EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.jp') THEN 'Japan'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.sg') THEN 'Singapore'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.hk') THEN 'Hong Kong EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.hk') THEN 'Hong Kong'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.in') THEN 'India'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.co\.id') THEN 'Indonesia'
      WHEN REGEXP_MATCH(hits.page.hostname, r'en\.airnewzealand-ar\.com') THEN 'Argentina EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand-ar\.com') THEN 'Argentina'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.mx') THEN 'Mexico'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.co') THEN 'Columbia'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.de$') THEN 'Germany'
      WHEN REGEXP_MATCH(hits.page.hostname, r'en\.airnewzealand-br\.com') THEN 'Brazil EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand-br\.com') THEN 'Brazil'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.fr') THEN 'France'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.eu') THEN 'Europe'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.pf') THEN 'French Polynesia'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.vn') THEN 'Vietnam'
      WHEN REGEXP_MATCH(hits.page.hostname, r'pacificislands\.airnewzealand\.com$') THEN 'Pacific Islands'
      WHEN REGEXP_MATCH(hits.page.hostname, r'(www|flightbookings|m|hotels)\.(airnewzealand|airnz)\.com$') THEN 'US'
      ELSE NULL END AS storefront,


  SUM(IF(hits.customDimensions.index = 11, 1,0)) AS SearchedOriginCity,
  # query for total events - youtube
  SUM(IF(hits.eventInfo.eventCategory = 'youtube', 1,0)) AS youtubeEvents,
  # query for total events - chatbot
  SUM(IF(hits.eventInfo.eventCategory = 'chatbot', 1,0)) AS chatbotEvents,
  # query for total events - masthead 
  SUM(IF(hits.eventInfo.eventCategory = 'mast head main' OR hits.eventInfo.eventCategory = 'mast head level 2' OR hits.eventInfo.eventCategory = 'mast head level 3', 1,0)) AS mastheadEvents,
  # query for total events - cta buttons 
  SUM(IF(hits.eventInfo.eventCategory = 'cta button' OR hits.eventInfo.eventCategory = 'CTA BUTTON' OR hits.eventInfo.eventCategory = 'cta buttons', 1,0)) AS CTAEvents,
  # query for total events - 100% scroll depth
  SUM(IF(hits.eventInfo.eventCategory = 'Scroll Depth' AND hits.eventInfo.eventLabel = '100%', 1,0)) AS ScrollDepth50Events,
  # query for total events - 75% scroll depth
  SUM(IF(hits.eventInfo.eventCategory = 'Scroll Depth' AND hits.eventInfo.eventLabel = '75%', 1,0)) AS ScrollDepth50Events,
  # query for total events - 25% scroll depth
  SUM(IF(hits.eventInfo.eventCategory = 'Scroll Depth' AND hits.eventInfo.eventLabel = '25%', 1,0)) AS ScrollDepth50Events,
  # query for total events - 50% scroll depth
  SUM(IF(hits.eventInfo.eventCategory = 'Scroll Depth' AND hits.eventInfo.eventLabel = '50%', 1,0)) AS ScrollDepth50Events,
  # query for total events - footer menu
  SUM(IF(hits.eventInfo.eventCategory = 'footer menu', 1,0)) AS footermenuEvents, 
  # query for total events - loyalty
  SUM(IF(hits.eventInfo.eventCategory = 'loyalty', 1,0)) AS loyaltyEvents, 
   # query for total events - video
  SUM(IF(hits.eventInfo.eventCategory = 'video', 1,0)) AS videoEvents, 
   # query for total ev
    ents - NebulaCX
  SUM(IF(hits.eventInfo.eventCategory = 'NebulaCX', 1,0)) AS NebulaCXEvents,
  
  # query for total events - ecommerce impressions
  SUM(IF(REGEXP_MATCH(hits.eventInfo.eventCategory, r'ecommerce') AND hits.eventInfo.eventAction = 'impression', 1, 0)) as EcommerceImpressionEvents,
  # SUM(IF(hits.eventInfo.eventCategory LIKE '%ecommerce%' AND hits.eventInfo.eventAction = 'impression', 1, 0)) as EcommerceImpressionEvents,
  # https://github.com/sparklineanalytics/analysts/blob/master/AirNZ/GUID%20Roll%20Up%20Analysis/base_table.sql
  # query for total flight searches. if criteria AND hitstype = page, provides pageviews
  SUM(IF(REGEXP_MATCH(hits.page.pagePath,r'/vbook/actions/(selectflights|selectitinerary|(mobi/|)createitinerary)') and hits.type = 'PAGE',1,0)) AS flightSearches,
  # query for total flight bookings. if criteria AND hitstype = page, provides pageviews
  SUM(IF(REGEXP_MATCH(hits.page.pagePath, r'/vbook/actions/(mobi/|)bookingconfirmation') AND hits.type = 'PAGE', 1,0)) AS flightbookings
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



# criteria for storefront
CASE
      WHEN REGEXP_MATCH(hits.page.hostname, r'grabaseat\.co\.nz') THEN 'GAS-NZ'
      WHEN REGEXP_MATCH(hits.page.hostname, r'(airn(z|ewzealand)|taxi|airpoints)\.co\.nz') THEN 'NZ'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.co\.uk') THEN 'UK'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.au') THEN 'AUS'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.ca') THEN 'Canada'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.cn') THEN 'China'
      WHEN REGEXP_MATCH(hits.page.hostname, r'(airnewzealand\.com\.cn|cn\.airnewzealand\.com)$') THEN 'China EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.co\.jp') THEN 'Japan EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.jp') THEN 'Japan'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.sg') THEN 'Singapore'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.hk') THEN 'Hong Kong EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.hk') THEN 'Hong Kong'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.in') THEN 'India'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.co\.id') THEN 'Indonesia'
      WHEN REGEXP_MATCH(hits.page.hostname, r'en\.airnewzealand-ar\.com') THEN 'Argentina EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand-ar\.com') THEN 'Argentina'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.mx') THEN 'Mexico'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.co') THEN 'Columbia'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.de$') THEN 'Germany'
      WHEN REGEXP_MATCH(hits.page.hostname, r'en\.airnewzealand-br\.com') THEN 'Brazil EN'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand-br\.com') THEN 'Brazil'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.fr') THEN 'France'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.eu') THEN 'Europe'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.pf') THEN 'French Polynesia'
      WHEN REGEXP_MATCH(hits.page.hostname, r'airnewzealand\.com\.vn') THEN 'Vietnam'
      WHEN REGEXP_MATCH(hits.page.hostname, r'pacificislands\.airnewzealand\.com$') THEN 'Pacific Islands'
      WHEN REGEXP_MATCH(hits.page.hostname, r'(www|flightbookings|m|hotels)\.(airnewzealand|airnz)\.com$') THEN 'US'
      ELSE NULL END AS storefront

  #MAX(IF(hits.customDimensions.index = 69, 1,0)) AS storefrontCheck,
  # MAX(IF(hits.eventInfo.eventCategory = 'youtube', 1,0)) AS youtubeCheck,
  #MAX(IF(hits.eventInfo.eventCategory = 'ecommerce' AND hits.eventInfo.eventAction = 'impression', 1,0)) AS ecommerceImpressionCheck,
  #MAX(IF(REGEXP_MATCH(hits.page.pagePath,r'/vbook/actions/(selectflights|selectitinerary|(mobi/|)createitinerary)') and hits.type = 'PAGE',1,0)) AS flightSearchCheck,
  #MAX(IF(REGEXP_MATCH(hits.page.pagePath, r'/vbook/actions/(mobi/|)bookingconfirmation') AND hits.type = 'PAGE', 0,1)) AS flightbookingCheck,