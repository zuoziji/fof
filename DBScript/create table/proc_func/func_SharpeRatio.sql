CREATE FUNCTION `func_SharpeRatio`(fund_code VARCHAR(200), riskFreeRate decimal(8,4)) RETURNS decimal(18,8)
BEGIN

DECLARE beginDate DATE;
DECLARE endDate DATE;
DECLARE beginNAV DOUBLE;
DECLARE endNAV DOUBLE;
DECLARE excessReturn DOUBLE;
DECLARE yearPart real;
DECLARE dayCount real;
DECLARE len real;
DECLARE volatility decimal(8,4);
-- Retriving the starting net asset value and corresponding date
SELECT nav_date, nav_acc
INTO beginDate , beginNAV FROM fund_nav
WHERE wind_code = fund_code
ORDER BY nav_date
LIMIT 1;
-- Retriving the ending net asset value and corresponding date
SELECT nav_date, nav_acc
INTO endDate , endNAV FROM fund_nav
WHERE wind_code = fund_code
ORDER BY nav_date DESC
LIMIT 1;
-- Calculate the standard deviation of hitorical return
SELECT STD(currentNAV.nav_acc / lastNAV.nav_acc - 1)
INTO volatility FROM
(SELECT nav_date, nav_acc
FROM fund_nav fund
WHERE wind_code = fund_code
) AS currentNAV
INNER JOIN
(
SELECT nav_date, nav_acc 
FROM fund_nav fund 
WHERE wind_code = fund_code 
) AS lastNAV 
ON 
lastNAV.nav_date = (select temp.nav_date 
from fund_nav temp
WHERE temp.wind_code = fund_code 
and temp.nav_date < currentNAV.nav_date
ORDER BY temp.nav_date DESC
LIMIT 1);

-- Calculate results, ACT/365
# SELECT COUNT(nav_date)
# INTO dayCount FROM fund_nav
# WHERE wind_code = fund_code;

SET len = DATEDIFF(endDate, beginDate);
SET yearPart = len / 365;
SET yearPart = 1 / yearPart;
SET excessReturn = POWER(endNAV / beginNAV, yearPart) - 1 - riskFreeRate;
SET volatility = volatility * SQRT(365 / len);

RETURN excessReturn / volatility;

END