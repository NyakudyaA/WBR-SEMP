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

-- Poverty Index Calculations
-- Create a materialized view dependency ratio . This is a join between the table "employment-status-hhold-head", sub_place
-- and wbr aoi.
CREATE MATERIALIZED VIEW public.mv_dependency_ratio
 AS
 WITH limpopo_subplace AS (
         SELECT a_1.*,
            b_1.geom
           FROM "employment-status-hhold-head" a_1
             JOIN sub_place b_1 ON a_1.sp_code = b_1.sp_code

        )
 SELECT a.id,a.sp_code,a.sp_name, a.employed,
a.unemployed, a.dosicoraged_worker_seeker, a.not_economically_active, a.less_than_15,
    st_intersection(st_transform(a.geom,32735),st_transform(b.geom,32735)) AS geom
   FROM limpopo_subplace a
     JOIN aoi_wbr b ON st_intersects(b.geom, a.geom);

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