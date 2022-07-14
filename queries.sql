/*
* STANDARDIZED calculate avgs  for attrition true and false
*/

SELECT "Attrition" ,
    AVG("OverTime_Scaled") AS "avg_OverTime", AVG("MonthlyIncome_Scaled") AS "avg_MonthlyIncome",
    AVG("StockOptionLevel_Scaled") AS "avg_StockOption", AVG("Age_Scaled") AS "avg_Age",
    AVG("DistanceFromHome_Scaled") AS "avg_DistanceHome", AVG("JobInvolvement_Scaled") AS "avg_JobInv",
    AVG("EnvironmentSatisfaction_Scaled") AS "avg_EnvSat", AVG("JobRole_Scaled") AS "avg_JobRole",
    AVG("YearsSinceLastPromotion_Scaled") AS "avg_YSLP", AVG("WorkLifeBalance_Scaled") AS "avg_WLBalance"
FROM "LEONARDO_LECCI_ibm_hr_numbers_standardized_sql"
GROUP BY "Attrition"

/*
* STANDARDIZED calculate distances for job roles and departments per row
*/

SELECT *
FROM(
        SELECT * , CASE WHEN b."final_dist_BF" > b."final_dist_BS"  THEN 1 
                        WHEN b."final_dist_BF" < b."final_dist_BS" THEN 0 ELSE 99 END as "business_outcome"
    FROM (SELECT "JobRole", "Department", "Attrition", "EmployeeNumber",
            SQRT(ABS("OverTime_Scaled"-0.7656123276561233)^2+ABS("MonthlyIncome_Scaled"-0.306675587996756)^2+
                    ABS("StockOptionLevel_Scaled"-0.28159124087591464)^2+ABS("Age_Scaled"-0.46575669099756545)^2+
                    ABS("DistanceFromHome_Scaled"-0.282712084347121255)^2+ABS("JobInvolvement_Scaled"-0.5902781832927825)^2+
                    ABS("EnvironmentSatisfaction_Scaled"-0.5904695863746957)^2+ABS("JobRole_Scaled"-0.6773114355231143)^2+
                    ABS("YearsSinceLastPromotion_Scaled"-0.14901703163017033)^2+ABS("WorkLifeBalance_Scaled"-0.5938037307380372)^2
                    ) AS "final_dist_BS",
            SQRT(ABS("OverTime_Scaled"-0.4641350210970464)^2+ABS("MonthlyIncome_Scaled"-0.19893670886075962)^2+
                    ABS("StockOptionLevel_Scaled"-0.17574683544303782)^2+ABS("Age_Scaled"-0.371637130801688)^2+
                    ABS("DistanceFromHome_Scaled"-0.3440379746835444)^2+ABS("JobInvolvement_Scaled"-0.5064050632911395)^2+
                    ABS("EnvironmentSatisfaction_Scaled"-0.4880717299578059)^2+ABS("JobRole_Scaled"-0.6993670886075949)^2+
                    ABS("YearsSinceLastPromotion_Scaled"-0.1297341772151898)^2+ABS("WorkLifeBalance_Scaled"-0.55283966244725771)^2
                    ) AS "final_dist_BF"
        FROM "LEONARDO_LECCI_ibm_hr_numbers_standardized_sql"
        WHERE "Attrition" = 0
              ) AS b
    ORDER BY b."final_dist_BF"
    
    ) AS c

WHERE c."business_outcome" = 1