CREATE VIEW fund_nav_friday AS
SELECT 
	fund_nav.wind_code AS wind_code,
	fund_nav_g.nav_date_friday AS nav_date_friday,
	nav AS nav,
	nav_acc AS nav_acc,
	source_mark AS source_mark
FROM fund_nav,
	(
	SELECT wind_code,
		   (nav_date + INTERVAL (4 - WEEKDAY(nav_date)) DAY) AS nav_date_friday,
		   MAX(nav_date) AS nav_date_max
	FROM fund_nav
	GROUP BY wind_code, nav_date_friday
	) as fund_nav_g
where fund_nav.wind_code = fund_nav_g.wind_code
and fund_nav.nav_date = fund_nav_g.nav_date_max;
