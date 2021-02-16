select * from public."DBS"

select * from public."DBS" where public."DBS"."NAME" = 'landing_hourly_coches_events_behaviour'

-- obtengo el SD_ID y TBL_ID
SELECT * FROM public."TBLS" where public."TBLS"."TBL_NAME" = 'events' and public."TBLS"."DB_ID" = 1

-- obtengo el SERDE_ID y el CD_ID
select * from PUBLIC."SDS" where public."SDS"."SD_ID" = 184

-- obtengo cosas como el password de conexion a la base de datos
SELECT * FROM public."SERDE_PARAMS" where public."SERDE_PARAMS"."SERDE_ID" = 166

-- obtengo columnas de tipo hive, las nested de spark no :(
select * from PUBLIC."COLUMNS_V2" where public."COLUMNS_V2"."CD_ID" = 127

-- obtengo el TBLPROPERTIES (por ejemplo las columnas nested de spark)
select * from PUBLIC."TABLE_PARAMS" where PUBLIC."TABLE_PARAMS"."TBL_ID" = 117

