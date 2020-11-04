-- Create a materialized view use of wood for cooking. This is a join between the table "energy-for-cooking", sub_place
-- and wbr aoi.


CREATE MATERIALIZED VIEW public.mv_use_of_wood_for_cooking
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "energy-for-cooking" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT a.id,a.sp_code,a.sp_name,a.electricity,
    a.gas,a.paraffin,a.wood,a.coal,a.animal_dung,a.solar,a.other,a.nothing,a.unspecified,a.not_applicable,
    st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
   FROM limpopo_subplace a
     JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

-- Create a layer with statistics for the wood_for_cooking index
CREATE MATERIALIZED VIEW public.mv_use_of_wood_for_cooking_index AS
 WITH sample AS (
         SELECT a.id,a.sp_code,a.sp_name,a.electricity,a.gas,a.paraffin,a.wood,a.coal,a.animal_dung,
            a.solar,a.other,a.nothing,a.unspecified,a.not_applicable,
            a.wood::double precision / st_area(a.geom) * 1000000::double precision AS wood_density,
            a.geom
           FROM mv_use_of_wood_for_cooking as a
        )
 SELECT sample.id,
    sample.sp_code,
    sample.sp_name,
        CASE
            WHEN (10::double precision * (sample.wood_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.wood_density) AS percentile_cont
               FROM sample sample_1)))) > 10::double precision THEN 10::double precision
            ELSE 10::double precision * (sample.wood_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.wood_density) AS percentile_cont
               FROM sample sample_1)))
        END AS index,
    sample.geom
   FROM sample
WITH DATA;


-- Create a materialized view use of wood for heating. This is a join between the table "energy-for-heating", sub_place
-- and wbr aoi.

CREATE MATERIALIZED VIEW public.mv_use_of_wood_for_heating
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "energy-for-heating" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT a.id,a.sp_code,a.sp_name, a.electricity, a.gas,
		a.paraffin, a.wood, a.coal, a.animal_dung, a.solar, a.other, a.nothing, a.unspecified, a.not_applicable,
		st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
		FROM limpopo_subplace a
     	JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

-- Create a layer with statistics for the wood_for_heating index
CREATE MATERIALIZED VIEW public.mv_use_of_wood_for_heating_index AS
 WITH sample AS (
         SELECT a.id,a.sp_code,a.sp_name, a.electricity, a.gas, a.paraffin,
			a.wood::double precision / st_area(a.geom) * 1000000::double precision AS wood_density, a.coal, a.animal_dung, a.solar, a.other,
			a.nothing, a.unspecified, a.not_applicable, a.geom
		FROM public.mv_use_of_wood_for_heating as a
        )
 SELECT sample.id,
    sample.sp_code,
    sample.sp_name,
        CASE
            WHEN (10::double precision * (sample.wood_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.wood_density) AS percentile_cont
               FROM sample sample_1)))) > 10::double precision THEN 10::double precision
            ELSE 10::double precision * (sample.wood_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.wood_density) AS percentile_cont
               FROM sample sample_1)))
        END AS index,
    sample.geom
   FROM sample
WITH DATA;


-- Create a materialized view supply of building materials. This is a join between the table "type-of-main-dwelling", sub_place
-- and wbr aoi.

CREATE MATERIALIZED VIEW public.mv_supply_of_building_materials
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "type-of-main-dwelling" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT a.id,a.sp_code,a.sp_name, a.brick_concrete,
        a.traditional_dwelling, a.flat_apartment, a.cluster_house, a.townhouse, a.semi_detached_house,
        a.house_flat_room_in_backyard, a.informal_dwelling, a.informal_dwelling_shack, a.room_flatlet, a.caravan_tent,
        a.other, a.unspecified, a.not_applicable,
		st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
		FROM limpopo_subplace a
     	JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

-- Create a layer with statistics for the supply of building materials index
CREATE MATERIALIZED VIEW public.mv_supply_of_building_materials_index AS
 WITH sample AS (
         SELECT a.id,a.sp_code,a.sp_name, a.brick_concrete,
a.traditional_dwelling::double precision / st_area(a.geom) * 1000000::double precision AS dwelling_density,
a.flat_apartment, a.cluster_house, a.townhouse, a.semi_detached_house, a.house_flat_room_in_backyard,
a.informal_dwelling, a.informal_dwelling_shack, a.room_flatlet, a.caravan_tent, a.other, a.unspecified,
a.not_applicable, a.geom
		FROM public.mv_supply_of_building_materials as a
        )
 SELECT sample.id,
    sample.sp_code,
    sample.sp_name,
        CASE
            WHEN (10::double precision * (sample.dwelling_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.dwelling_density) AS percentile_cont
               FROM sample sample_1)))) > 10::double precision THEN 10::double precision
            ELSE 10::double precision * (sample.dwelling_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.dwelling_density) AS percentile_cont
               FROM sample sample_1)))
        END AS index,
    sample.geom
   FROM sample
WITH DATA;

-- Create a materialized view supply of building materials. This is a join between the table "source-of-water", sub_place
-- and wbr aoi.

CREATE MATERIALIZED VIEW public.mv_direct_supply_of_water_from_the_environment
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "source-of-water" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT  a.id,a.sp_code,a.sp_name, a.regional_local_water_source, a.borehole, a.spring, a.rain_water,
        a.dam_pool_stagnant_water, a.river_stream, a.water_vendor, a.water_tanker, a.other, a.not_applicable,
		st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
		FROM limpopo_subplace a
     	JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

-- Create a layer with statistics for direct supply of water from the environment index
CREATE MATERIALIZED VIEW public.mv_direct_supply_of_water_from_the_environment_index AS
 WITH sample AS (
         SELECT a.id,  a.sp_code, a.sp_name,sum(a.borehole + a.spring + a.rain_water +a.dam_pool_stagnant_water
											+ a.river_stream + a.water_tanker ) / st_area(a.geom) * 1000000::double precision AS water_density ,
											 a.geom
		FROM public.mv_direct_supply_of_water_from_the_environment as a
	    group by (a.id,a.sp_code,a.sp_name, a.geom)
        )
 SELECT sample.id,
    sample.sp_code,
    sample.sp_name,
        CASE
            WHEN (10::double precision * (sample.water_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.water_density) AS percentile_cont
               FROM sample sample_1)))) > 10::double precision THEN 10::double precision
            ELSE 10::double precision * (sample.water_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.water_density) AS percentile_cont
               FROM sample sample_1)))
        END AS index,
    sample.geom
   FROM sample
WITH DATA;

-- Create an index based on the water access survey villages
CREATE MATERIALIZED VIEW mv_water_access_by_village_index AS
with sample as (
SELECT id, st_transform(geom,32735) as geom, village,
sum (  borehole  + "natural spring" + "tanker truck" + "river/stream"+ "dug well"  ) / st_area(st_transform(geom,32735)) * 1000000 as water_density
	FROM public.wateraccess_by_village
group by (id,village,geom)
	)
	SELECT sample.id,
    sample.village,
        CASE
            WHEN (10::double precision * (sample.water_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.water_density) AS percentile_cont
               FROM sample sample_1)))) > 10::double precision THEN 10::double precision
            ELSE 10::double precision * (sample.water_density / (( SELECT percentile_cont(0.9::double precision)
                WITHIN GROUP (ORDER BY sample_1.water_density) AS percentile_cont
               FROM sample sample_1)))
        END AS index,
    sample.geom
   FROM sample;


-- Poverty Index Calculations
-- Create a materialized view dependency ratio . This is a join between the table "employment-status-hhold-head", sub_place
-- and wbr aoi.
CREATE MATERIALIZED VIEW public.mv_dependency_ratio
TABLESPACE pg_default
AS
 WITH limpopo_subplace AS (
         SELECT a_1.id,
            a_1.sp_code,
            a_1.sp_name,
            a_1.employed,
            a_1.unemployed,
            a_1.dosicoraged_worker_seeker,
            a_1.not_economically_active,
            a_1.less_than_15,
            b_1.geom
           FROM ("employment-status-hhold-head" a_1
             JOIN sub_place b_1 ON ((a_1.sp_code = b_1.sp_code)))
        )
 SELECT a.id,
    a.sp_code,
    a.sp_name,
    a.employed,
    a.unemployed,
    a.dosicoraged_worker_seeker,
    a.not_economically_active,
    a.less_than_15,
    st_intersection(st_transform(a.geom, 32735), st_transform(b.geom, 32735)) AS geom
   FROM (limpopo_subplace a
     JOIN aoi_wbr b ON (st_intersects(b.geom, a.geom)))
WITH DATA;

-- Create a layer with statistics for dependency ratio index

CREATE MATERIALIZED VIEW public.mv_dependency_ratio_index AS
SELECT id,  sp_code, sp_name,
case when employed = 0 then
0 else
round((100 - ((sum(unemployed::decimal + dosicoraged_worker_seeker::decimal
                       + not_economically_active::decimal + less_than_15::decimal) / employed::decimal)/100))/10,4)

end AS "index",geom
FROM public.mv_dependency_ratio
group by (id,sp_code,sp_name,employed,geom) ;

-- Create a materialized view proportion of low-income households .
-- This is a join between the table "annual-household-income", sub_place
-- and wbr aoi.
CREATE MATERIALIZED VIEW public.mv_proportion_of_low_income_households
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "annual-household-income" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT a.id,a.sp_code,a.sp_name, a.no_income, a."1-4800k", a."4801-9600", a."9601-19600", a."19601-38200",
a."38201-76400", a."76401-153800", a."153801-307600", a."307601-614400",
a."614001-1228800", a."1228801-2457600", a."greater than 24576001", a.unspecified,
    st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
   FROM limpopo_subplace a
     JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

-- Create a layer with statistics for dependency ratio index

CREATE MATERIALIZED VIEW public.mv_proportion_of_low_income_households_index AS
WITH sample AS (
SELECT a.id,a.sp_code,a.sp_name, sum(a.no_income + a."1-4800k" + a."4801-9600") / st_area(a.geom) * 1000000::decimal
	as income_density, geom
FROM public.mv_proportion_of_low_income_households as a
group by (a.id, a.sp_code,a.sp_name,geom)
	)
	SELECT sample.id,
	       sample.sp_name,
    sample.sp_code,

			   CASE WHEN
			   		(sample.income_density / (( SELECT percentile_cont(0.9::decimal)
                WITHIN GROUP (ORDER BY sample_1.income_density) AS percentile_cont
               FROM sample sample_1))) > 10 THEN 10
			   ELSE
			   (sample.income_density / (( SELECT percentile_cont(0.9::decimal)
                WITHIN GROUP (ORDER BY sample_1.income_density) AS percentile_cont
               FROM sample sample_1)))
			   END AS "index" ,sample.geom



   FROM sample;

-- Access to services sub indices

-- Create a materialized view access_to_services_electricity .
-- This is a join between the table "energy-for-lighting", sub_place
-- and wbr aoi
CREATE MATERIALIZED VIEW public.mv_access_to_services_electricity
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "energy-for-lighting" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT a.id, a.sp_code, a.sp_name,
a.electricity, a.gas, a.paraffin, a.candles, a.solar, a.nothing, a.unspecified, a.not_applicable,
    st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
   FROM limpopo_subplace a
     JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

--TODO
-- Add mv_access_to_services_electricity_index below here

-- Create a materialized view access_to_services_electricity .
-- This is a join between the table "toilet-facilities", sub_place
-- and wbr aoi
CREATE MATERIALIZED VIEW public.mv_access_to_services_sanitation
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "toilet-facilities" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT a.id, a.sp_code, a.sp_name,
a.nothing, a.flush_toilet_connected_to_sewer, a.flush_with_septic_tank, a.chemical_toilet, a.pit_toilet_with_ventilation,
a.pit_toilet_without_ventilation, a.bucket_toilet, a.other, a.unspecified, a.not_applicable,
    st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
   FROM limpopo_subplace a
     JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

--TODO
-- Add mv_access_to_services_sanitation_index below here

-- Create a materialized view access_to_services_refuse_disposal .
-- This is a join between the table "refuse-disposal", sub_place
-- and wbr aoi
CREATE MATERIALIZED VIEW public.mv_access_to_services_refuse_disposal
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "refuse-disposal" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT a.id,   a.sp_code, a.sp_name,   a.removed_local_auth_1_per_week, a.removed_local_auth_less_often,
a.communal_refuse_dump, a.own_refuse_dump, a.no_refuse_dump, a.other, a.unspecified, a.not_applicable,
    st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
   FROM limpopo_subplace a
     JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

--TODO
-- Add mv_access_to_services_refuse_disposal_index below here


-- Consumption ( Level of ownership for various goods as proxy to poverty)
-- Create table for combinining all the indicatiors

CREATE MATERIALIZED VIEW mv_level_of_ownership

AS
 SELECT a.fid,
    a.sp_code,
    a.sp_name,
    a.cell_1,
    a.cell_2,
    a.comp_1,
    a.comp_2,
    a.dvdply_1,
    a.dvdply_2,
    a.elgstv_1,
    a.elgstv_2,
    a.mtrcar_1,
    a.mtrcar_2,
    a.radio_1,
    a.radio_2,
    a.refrig_1,
    a.refrig_2,
    a.sattv_1,
    a.sattv_2,
    a.tv_1,
    a.tv_2,
    a.vcmcln_1,
    a.vcmcln_2,
    a.wshmch_1,
    a.wshmch_2,
    b.geom
   FROM ownership_proxy a
     JOIN sub_place b ON b.sp_code::character varying::text = a.sp_code::text
WITH DATA;

-- Create level of ownership clipped to the WBR boundary
CREATE MATERIALIZED VIEW mv_level_of_ownership_wbr as
SELECT a.fid, a.sp_code, a.sp_name, a.cell_1, a.cell_2, a.comp_1, a.comp_2, a.dvdply_1, a.dvdply_2, a.elgstv_1, a.elgstv_2,
     a.mtrcar_1, a.mtrcar_2, a.radio_1, a.radio_2, a.refrig_1, a.refrig_2, a.sattv_1, a.sattv_2, a.tv_1,
	 a.tv_2, a.vcmcln_1, a.vcmcln_2, a.wshmch_1, a.wshmch_2,
       st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
   FROM mv_level_of_ownership a
     JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

-- Create index for consumption
CREATE MATERIALIZED VIEW mv_level_of_ownership_index AS
with sample as (
SELECT fid, sp_code, sp_name,
round(1 - (sum (cell_1::numeric + comp_1::numeric + dvdply_1::numeric + elgstv_1::numeric
	 + mtrcar_1::numeric + radio_1::numeric + refrig_1::numeric + sattv_1::numeric + tv_1::numeric + vcmcln_1::numeric + wshmch_1::numeric ) / (
	sum (cell_1::numeric + comp_1::numeric + dvdply_1::numeric + elgstv_1::numeric + mtrcar_1::numeric
		 + radio_1::numeric + refrig_1::numeric + sattv_1::numeric + tv_1::numeric + vcmcln_1::numeric + wshmch_1::numeric ) +
	sum (cell_2::numeric +  comp_2::numeric +  dvdply_2::numeric +  elgstv_2::numeric +
 mtrcar_2::numeric +  radio_2::numeric +  refrig_2::numeric +
sattv_2::numeric +  tv_2::numeric +  vcmcln_2::numeric + wshmch_2::numeric )))::numeric,4)  / st_area(geom) * 1000000 as ownership_ratio ,

geom
	FROM public.mv_level_of_ownership_wbr
	group by (fid, sp_code,sp_name,geom)
	)

	SELECT sample.fid,
    sample.sp_code,
    sample.sp_name,
    round(
        CASE
            WHEN (10::double precision * (sample.ownership_ratio / (( SELECT percentile_cont(0.9::double precision) WITHIN GROUP (ORDER BY sample_1.ownership_ratio) AS percentile_cont
               FROM sample sample_1)))) > 10::double precision THEN round(10::numeric,3)
            ELSE round(10::numeric * (sample.ownership_ratio / (( SELECT percentile_cont(0.9::double precision) WITHIN GROUP (ORDER BY sample_1.ownership_ratio) AS percentile_cont
               FROM sample sample_1)))::numeric, 3)
        END, 3) AS ownership_index,
    sample.geom
   FROM sample;

-- Overall social index zonal stats
CREATE MATERIALIZED VIEW mv_overal_social_index AS
SELECT id,
    round(
        CASE
            WHEN (100::double precision * mean /
					(( SELECT percentile_cont(0.9::double precision) WITHIN GROUP (ORDER BY mean) AS percentile_cont
               FROM quinary_catchment ))) > 100::double precision THEN 100::numeric
            ELSE round((100::double precision * mean /
					(( SELECT percentile_cont(0.9::double precision) WITHIN GROUP (ORDER BY mean) AS percentile_cont
               FROM quinary_catchment )))::numeric,3)
        END, 3) AS index,
    geom
   FROM quinary_catchment;