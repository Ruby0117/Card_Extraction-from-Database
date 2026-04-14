"""
Card Extraction Pipeline
Extracts and transforms card data from Hive tables, decodes numeric codes to human-readable labels,
and exports per-survey-year Excel files to Azure Data Lake Storage.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, expr
from pyspark.sql.window import Window
from pyspark.sql.types import *

from src.decode import apply_all_decodings
from src.bool_columns import convert_boolean_columns
from src.lookups import build_lookups, join_lookups
from src.export import export_by_year


def run_extraction(spark: SparkSession, output_username: str) -> None:
    """
    Main entry point for the card extraction pipeline.

    Steps:
        1. Load raw tables from Hive
        2. Select and rename columns
        3. Decode numeric option codes to human-readable strings
        4. Convert boolean flags to Yes/No
        5. Join structure and province lookups
        6. Export one Excel file per survey year to ADLS

    Args:
        spark: Active SparkSession
        output_username: ADLS username subfolder (e.g. "ywu")
    """
    # 1. Load raw tables
    card = spark.sql("SELECT * FROM crm_hmi.survey_card")
    new_structure = spark.sql("SELECT * FROM crm_hmi.survey_structure")
    new_province = spark.sql("SELECT * FROM crm_shared_entity.province_or_state")

    # 2. Build lookup dataframes
    structure_lookup, province_lookup = build_lookups(new_structure, new_province)

    # 3. Select and rename card columns
    cards = _select_card_columns(card)

    _log_dimensions(cards)

    # 4. Decode option columns
    cards = apply_all_decodings(cards)

    # 5. Convert booleans
    cards = convert_boolean_columns(cards)

    # 6. Join lookups
    new_cards = join_lookups(cards, structure_lookup, province_lookup)

    # 7. Encode province IDs
    new_cards = _encode_province_ids(new_cards)

    # 8. Drop unused columns
    new_cards = new_cards.drop("CardIDLookup", "Owner")

    # 9. Export
    output_base = f"abfss://sandbox@eppd1stcdp01.dfs.core.windows.net/users/{output_username}/Extract/"
    export_by_year(new_cards, output_base)


def _select_card_columns(card):
    """Select and alias all survey columns from the raw card table."""
    return card.select(
        col("survey_cardid").alias("cardidlookup"),
        col("survey_cardidnum").alias("cardid"),
        col("survey_structureid").alias("structidlookup"),
        col("survey_sahsrsyear").alias("surveyyear"),
        col("survey_regionalmailbox").alias("regmailbox"),
        col("survey_address").alias("address"),
        col("ownerid").alias("owner"),
        col("statecode").alias("status"),
        col("survey_sortcode").alias("specsort"),
        col("survey_dwellingtype").alias("dweltype"),
        # Q1
        col("survey_ownermanagertype").alias("q1_ownermanager"),
        col("survey_otherownermanagertypespecify").alias("q1_otherrole"),
        # Q2
        col("survey_respondent_label").alias("q2_respondent_label"),
        col("survey_newcontactrole").alias("q2_newcontactrole"),
        col("survey_geo_location").alias("q2_geo_location"),
        col("survey_contact_primary").alias("q2_contact_primary"),
        col("survey_contact_secondary").alias("q2_contact_secondary"),
        col("survey_providenewcontactdetails").alias("q2_options"),
        # Q3
        col("survey_issocialaffordable").alias("q3_sahconfirm"),
        # Q4
        col("survey_isprojectname").alias("q4_identifierconfirmation"),
        col("survey_projectname").alias("q4_identifier"),
        # Q5 — owner types
        col("survey_isfegovernment").alias("q5_type_a"),
        col("survey_isprovincialgovernment").alias("q5_provownertyp"),
        col("survey_isterritorialgovernment").alias("q5_terrownertyp"),
        col("survey_ismunicipalgovernment").alias("q5_muniownertyp"),
        col("survey_isaboriginalindigenousgovenmentownertype").alias("q5_abinownertyp"),
        col("survey_isnonprofit").alias("q5_nonprofitownertyp"),
        col("survey_iscooperative").alias("q5_coopownertyp"),
        col("survey_isprivate").alias("q5_privateownertyp"),
        col("survey_ownertypeother").alias("q5_otherownertyp"),
        col("survey_isotherspecify").alias("q5_other"),
        col("survey_question5options").alias("q5_options"),
        # Q6 — manager types
        col("survey_isdoestheownermanageittoo").alias("q6_managsameasowner"),
        col("survey_isfegovernmentmanager").alias("q6_fedrmanagtyp"),
        col("survey_isprovincialgovernmentmanager").alias("q6_provmanagtyp"),
        col("survey_isterritorialgovernmentmanager").alias("q6_terrmanagtyp"),
        col("survey_ismunicipalgovernmentmanager").alias("q6_munimanagtyp"),
        col("survey_isaboriginalindigenousgovermanagertype").alias("q6_abinmanagtyp"),
        col("survey_isnonprofitmanager").alias("q6_nonprofitmanagtyp"),
        col("survey_iscooperativemanager").alias("q6_coopmanagtyp"),
        col("survey_isprivatemanager").alias("q6_privatemanagtyp"),
        col("survey_managertypeother").alias("q6_othermanagtyp"),
        col("survey_isothermanager").alias("q6_other"),
        col("survey_question6options").alias("q6_options"),
        # Q7 — operation deficit gov types
        col("survey_isfegovernmentsurveyquestion8").alias("q7_fedrgovtyp"),
        col("survey_isprovincialgovernmentquestion8").alias("q7_provgovtyp"),
        col("survey_isterritorialgovernmentquestion8").alias("q7_terrgovtyp"),
        col("survey_ismunicipalgovernmentquesion8").alias("q7_munigovtyp"),
        col("survey_isaboriginalindigenousgovernmentquestion8").alias("q7_abingovtyp"),
        col("survey_isotherquestion8").alias("q7_other"),
        col("survey_governmentotherquestion8").alias("q7_othera"),
        col("survey_question7options").alias("q7_options"),
        # Q8 — funding agreement types
        col("survey_isfegovernmentquestion9").alias("q8_fedrfundtyp"),
        col("survey_isprovincialgovernmentquestion9").alias("q8_provfundtyp"),
        col("survey_isterritorialgovernmentquestion9").alias("q8_terrfundtyp"),
        col("survey_ismunicipalgovernmentquestion9").alias("q8_munigovtyp"),
        col("survey_isaboriginalindigenousgovernmentquestion9").alias("q8_abinfundtyp"),
        col("survey_isotherquestion9").alias("q8_other"),
        col("survey_fundingagreementotherquestion9").alias("q8_othera"),
        col("survey_question8options").alias("q8_options"),
        # Q9 — clientele
        col("survey_isfamilieswithchildren").alias("q9_withchildren"),
        col("survey_issinglewomenquestion10").alias("q9_singlewomen"),
        col("survey_issinglemenquestion10").alias("q9_singlemen"),
        col("survey_isseniors").alias("q9_seniors"),
        col("survey_isyouthquestion10").alias("q9_youth"),
        col("survey_isimmigrantsrefugees").alias("q9_immigrantsrefugees"),
        col("survey_ispersonsphysicaldisabilitiesquestion10").alias("q9_physicaldisabilities"),
        col("survey_ispersonsmentaldisabilitiesquestion10").alias("q9_mentaldisabilities"),
        col("survey_isveteransquestion10").alias("q9_veterans"),
        col("survey_isfirstnationsquestion10").alias("q9_firstnations"),
        col("survey_ismetisquestion10").alias("q9_metis"),
        col("survey_isinuitquestion10").alias("q9_inuit"),
        col("survey_isvictimsdomesticviolencequestion10").alias("q9_domesticviolence"),
        col("survey_isexitinghomelessnessquestion10").alias("q9_homelessness"),
        col("survey_othermainclientele").alias("q9_othermainclientele"),
        col("survey_isothersmainclientele").alias("q9_other"),
        col("survey_question9options").alias("q9_options"),
        # Q10 — rent inclusions
        col("survey_isheatquestoin12").alias("q10_heat"),
        col("survey_ishotwaterquestion12").alias("q10_hotwater"),
        col("survey_iselectricityquestion12").alias("q10_electricity"),
        col("survey_isparkingspacesquestion12").alias("q10_parkingspace"),
        col("survey_iscabletvquestion12").alias("q10_cabletv"),
        col("survey_isinternetwifiquestion12").alias("q10_internetwifi"),
        col("survey_isnumberofappliancesquestion12").alias("q10_appliances"),
        col("survey_numberofappliancesquestion12").alias("q10_numberofappliances"),
        col("survey_isfurnishedquestion12").alias("q10_furnished"),
        col("survey_isotherquestion12").alias("q10_other"),
        col("survey_rentpriceotherquestion12").alias("q10_othera"),
        col("survey_question11options").alias("q10_options"),
        # Q11 — build year
        col("survey_buildyear").alias("q11_buildyear"),
        col("survey_buildyearactualorestimated").alias("q11_buildyear_actual_estimate"),
        col("survey_buildyearknown").alias("q11_options"),
        # Q12 — building materials
        col("survey_isallwoodframeq15").alias("q12_allwoodframe"),
        col("survey_isconcreteblockwallsframeq15").alias("q12_concretewallsframe"),
        col("survey_isbrickwallsq15").alias("q12_brickwallsframe"),
        col("survey_issteelstudwallscoveredwithsidingq15").alias("q12_steelwallsframe"),
        col("survey_isstructuredsteelframeq15").alias("q12_structsteelframe"),
        col("survey_isotherq15").alias("q12_other"),
        col("survey_otherspecifytextquestion15").alias("q12_othera"),
        col("survey_question15options").alias("q12_options"),
        # Q13 — heating
        col("survey_isforcedairfurnacequestion17").alias("q13_forcedairfurnace"),
        col("survey_forcedairfurnaceoptionsquestion17").alias("q13_forcedairfurnaceoptions"),
        col("survey_isheatpumpquestion17").alias("q13_heatpump"),
        col("survey_iselectricbaseboardsquestion17").alias("q13_electricbaseboard"),
        col("survey_ishotwaterorsteamradiatorsquestion17").alias("q13_hotwater"),
        col("survey_isheatingstoveoilwoodgasquestion17").alias("q13_heatingstove"),
        col("survey_isotherspecifyquestion17").alias("q13_other"),
        col("survey_primarymaterialsusedother").alias("q13_othera"),
        col("survey_question16options").alias("q13_options"),
        # Q14 — accessibility
        col("survey_iselevators").alias("q14_elevators"),
        col("survey_structelevatorcount").alias("q14_elevatorsnumber"),
        col("survey_isstreetlevelentrance").alias("q14_streetlevelentrance"),
        col("survey_iswidedoorwaysforwheelchairs").alias("q14_widedoorways"),
        col("survey_iswheelchairramps").alias("q14_accessramps"),
        col("survey_ishandrailsincommonareahallwayaccessfeat").alias("q14_handrails"),
        col("survey_isstairliftthroughfloorliftaccessfeat").alias("q14_stairlift"),
        col("survey_appropriatedoorhardwareaccessfeat").alias("q14_appropriatedoordhdwr"),
        col("survey_iselectricdooropeneraccessfeat").alias("q14_electronicdooropener"),
        col("survey_iskeylessentryaccessfeat").alias("q14_keylessentry"),
        col("survey_isaccessibleparkingaccessfeat").alias("q14_accessibleparking"),
        col("survey_isscooterwheelchairsstorageareaaccessfeat").alias("q14_scooterstorage"),
        col("survey_ispavedwalkwayaccessfeat").alias("q14_pavedwalkways"),
        col("survey_isotheraccesfeature").alias("q14_other"),
        col("survey_otheraccessibilityfeatures").alias("q14_othera"),
        col("survey_question18options").alias("q14_options"),
        # Q15–Q17 — building condition & repairs
        col("survey_question22options").alias("q15_buildinglastestimation"),
        col("survey_basedonbca").alias("q16_buildingcondition"),
        col("survey_isfoundationquestion26").alias("q17_foundation"),
        col("survey_issuperstructureframequestion26").alias("q17_superstructure"),
        col("survey_isroofingquestion26").alias("q17_roofing"),
        col("survey_isexteriorenclosurequestion26").alias("q17_exteriorenclosure"),
        col("survey_isinteriorconstructionquestion26").alias("q17_interiorconstruction"),
        col("survey_isplumbingquestion26").alias("q17_plumbing"),
        col("survey_iselectricalquestion26").alias("q17_electrical"),
        col("survey_ishvacquestion26").alias("q17_heatingventilationac"),
        col("survey_isunitsquestion26").alias("q17_units"),
        col("survey_iselevatorsquestion26").alias("q17_elevators"),
        col("survey_isfireprotectionquestion26").alias("q17_fireprotection"),
        col("survey_isexteriorsiteimprovementquestion26").alias("q17_exteriorsiteimprovement"),
        col("survey_isotherspecifyquestion26").alias("q17_other"),
        col("survey_otherspecifytextquestion26").alias("q17_othera"),
        col("survey_question25options").alias("q17_options"),
        # Q18–Q27 — units, occupancy, rent, vacancy
        col("survey_totalnumberofunits").alias("q18_totutct"),
        col("survey_totalnumberofunitsknown").alias("q18_options"),
        col("survey_sahrsunit").alias("q19_totsbzutct"),
        col("survey_question29options").alias("q19_options"),
        col("survey_sahrsbed0unit").alias("q20_bachunit"),
        col("survey_sahrsbed1unit").alias("q20_bed1unit"),
        col("survey_sahrsbed2unit").alias("q20_bed2unit"),
        col("survey_sahrsbed3unit").alias("q20_bed3unit"),
        col("survey_sahrsbed4unit").alias("q20_bed4unit"),
        col("survey_question30options").alias("q20_options"),
        col("survey_newhouseholdoccupiedstudio").alias("q21_bachoccupiednewhh"),
        col("survey_newhouseholdoccupied1bed").alias("q21_1bedoccupiednewhh"),
        col("survey_newhouseholdoccupied2bed").alias("q21_2bedoccupiednewhh"),
        col("survey_newhouseholdoccupied3bed").alias("q21_3bedoccupiednewhh"),
        col("survey_newhouseholdoccupied4bed").alias("q21_4bedoccupiednewhh"),
        col("survey_question21options").alias("q21_options"),
        col("survey_totalnewhouseholdoccupied").alias("q22_totoccupiednewhh"),
        col("survey_totalnewhouseholdoccupiedknown").alias("q22_options"),
        col("survey_rentbasedonoperationalcostsquestion32").alias("q23_rentoperationalcots"),
        col("survey_rentgearedtoincomequestion32").alias("q23_rentgearedtoincome"),
        col("survey_rentsetagainstthemarketrentforquestion32").alias("q23_rentsetagainstmarket"),
        col("survey_rentisfixedbyanexternalentityquestion32").alias("q23_rentfixedexternal"),
        col("survey_otherrentmechanismspecifyquestion31").alias("q23_otherrentmechanism"),
        col("survey_otherrentmechanism").alias("q23_otherrentmechanisma"),
        col("survey_unitswithotherrentmechanismquestion32").alias("q23_otherrentmechanismnumb"),
        col("survey_question31options").alias("q23_options"),
        col("survey_numberofvacantunitsstudio").alias("q24_vunitbach"),
        col("survey_numberofvacantunits1bed").alias("q24_vunit1bed"),
        col("survey_numberofvacantunits2bed").alias("q24_vunit2bed"),
        col("survey_numberofvacantunits3bed").alias("q24_vunit3bed"),
        col("survey_numberofvacantunits4bed").alias("q24_vunit4bed"),
        col("survey_question32options").alias("q24_options"),
        col("survey_question36options").alias("q25_howrentinfoentered"),
        col("survey_bacheloraveragerentamount").alias("q27_bachavgrent"),
        col("survey_1bedroomaveragerentamount").alias("q27_bed1avgrent"),
        col("survey_2bedroomsaveragerentamount").alias("q27_bed2avgrent"),
        col("survey_3bedroomsaveragerentamount").alias("q27_bed3avgrent"),
        col("survey_4bedroomsaveragerentamount").alias("q27_bed4avgrent"),
        col("survey_question39aoptions").alias("q27_options"),
        # Metadata
        col("survey_rentgridisvalid").alias("rent_grid_is_valid"),
        col("survey_autostatus").alias("auto_status"),
        col("survey_manualstatus").alias("manual_status"),
        col("survey_removalreasonspermanently").alias("removalreasonspermanently"),
        col("survey_removalreasonstemporarily").alias("removalreasonstemporarily"),
        col("survey_sahenumerator").alias("enumerator"),
        col("survey_provinceorstateid").alias("provinceid"),
        col("modifiedon"),
        col("modifiedbyyominame").alias("modifiedby"),
        col("load_date_time"),
    )


def _encode_province_ids(new_cards):
    """Map province names to Statistics Canada numeric province codes."""
    return new_cards.withColumn("provinceid", expr("""
        CASE
            WHEN province = 'Newfoundland and Labrador' THEN 10
            WHEN province = 'Prince Edward Island'     THEN 11
            WHEN province = 'Nova Scotia'              THEN 12
            WHEN province = 'New Brunswick'            THEN 13
            WHEN province = 'Quebec'                   THEN 24
            WHEN province = 'Ontario'                  THEN 35
            WHEN province = 'Manitoba'                 THEN 46
            WHEN province = 'Saskatchewan'             THEN 47
            WHEN province = 'Alberta'                  THEN 48
            WHEN province = 'British Columbia'         THEN 59
            WHEN province = 'Yukon'                    THEN 60
            WHEN province = 'Northwest Territories'    THEN 61
            WHEN province = 'Nunavut'                  THEN 62
            ELSE CAST(provinceid AS STRING)
        END
    """))


def _log_dimensions(df) -> None:
    print(f"Rows: {df.count()}, Columns: {len(df.columns)}")
    duplicates = df.groupBy("cardid").count().filter("count > 1")
    dup_count = duplicates.count()
    if dup_count > 0:
        print(f"WARNING: {dup_count} duplicate cardid(s) found.")
    else:
        print("No duplicate cardid values found.")
