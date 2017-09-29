CREATE VIEW `fund_mgrcomp` AS
    SELECT 
        `fundinfonew`.`fund_mgrcomp` AS `fund_mgrcomp`,
        `fundinfonew`.`strategy_type` AS `strategy_type`,
        COUNT(0) AS `fund_count`
    FROM
        `fundinfonew`
    WHERE
        (`fundinfonew`.`fund_mgrcomp` IS NOT NULL)
    GROUP BY `fundinfonew`.`fund_mgrcomp` , `fundinfonew`.`strategy_type`