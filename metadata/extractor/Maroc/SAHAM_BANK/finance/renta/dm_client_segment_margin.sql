SELECT
    CONCAT(
        '{"clientSegmentCode":"', db1.Client_Segment_Code, '",',
        '"clientSegmentName":"', db1.Client_Segment_Name, '",',
        '"clientCount":', COALESCE(cast(db1.Client_Count as varchar(10)), 'null'), ',',
        '"totalOutstandingBalance":', COALESCE(cast(db1.Total_Outstanding_Balance as varchar(20)), 'null'), ',',
        '"interestMargin":', COALESCE(cast(db1.Interest_Margin as varchar(20)), 'null'), ',',
        '"feeMargin":', COALESCE(cast(db1.Fee_Margin as varchar(20)), 'null'), ',',
        '"provisionCost":', COALESCE(cast(db1.Provision_Cost as varchar(20)), 'null'), ',',
        '"netContribution":', COALESCE(cast(db1.Net_Contribution as varchar(20)), 'null'), ',',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS client_segment_data
FROM  {$Db_Schema1}.finance_dm_client_segment_margin db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
