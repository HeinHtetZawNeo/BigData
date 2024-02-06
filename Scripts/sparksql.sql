#Yearly Average Price for a Specific Commodity:
SELECT year(Price_Date) AS Year, AVG(Price) AS Average_Price
FROM commodities
WHERE Com_Type = 'SpecificCommodity' -- Replace 'SpecificCommodity' with your actual commodity type
GROUP BY year(Price_Date)
ORDER BY Year;

#Comparison of Yearly Average Prices Between Commodities:
SELECT Com_Type, year(Price_Date) AS Year, AVG(Price) AS Average_Price
FROM commodities
GROUP BY Com_Type, year(Price_Date)
ORDER BY Com_Type, Year;

#Year-over-Year Price Change for Each Commodity:
WITH YearlyAverages AS (
    SELECT Com_Type, year(Price_Date) AS Year, AVG(Price) AS Average_Price
    FROM commodities
    GROUP BY Com_Type, year(Price_Date)
)
SELECT a.Com_Type, a.Year, a.Average_Price, (a.Average_Price - b.Average_Price) / b.Average_Price * 100 AS YoY_Change_Percentage
FROM YearlyAverages a
         LEFT JOIN YearlyAverages b ON a.Com_Type = b.Com_Type AND a.Year = b.Year + 1
ORDER BY a.Com_Type, a.Year;

#Yearly Highest and Lowest Prices for Each Commodity:
WITH YearlyPrices AS (
    SELECT Com_Type, year(Price_Date) AS Year, AVG(Price) AS AvgPrice
    FROM commodities
    GROUP BY Com_Type, year(Price_Date)
),
     RankedPrices AS (
         SELECT Com_Type, Year, AvgPrice,
                rank() OVER (PARTITION BY Com_Type ORDER BY AvgPrice DESC) as MaxPriceRank,
                rank() OVER (PARTITION BY Com_Type ORDER BY AvgPrice ASC) as MinPriceRank
         FROM YearlyPrices
     )
SELECT Com_Type, Year as Highest_Price_Year, AvgPrice as Highest_Price
FROM RankedPrices
WHERE MaxPriceRank = 1
UNION ALL
SELECT Com_Type, Year as Lowest_Price_Year, AvgPrice as Lowest_Price
FROM RankedPrices
WHERE MinPriceRank = 1;

#Yearly Price Volatility for Each Commodity:
SELECT Com_Type, year(Price_Date) AS Year, stddev(Price) AS Price_Volatility
FROM commodities
GROUP BY Com_Type, year(Price_Date)
ORDER BY Com_Type, Year;
