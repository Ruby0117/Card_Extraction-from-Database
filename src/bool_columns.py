"""
bool_columns.py
Converts all Dynamics 365 boolean columns (stored as 'true'/'false' strings)
to human-readable 'Yes' / 'No' labels.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

# All boolean flag columns across Q5–Q23
BOOLEAN_COLUMNS = [
    # Q5 — xxxx types
    "q5_fedrxxxxtyp", "q5_provxxxxtyp", "q5_terrxxxxtyp", "q5_munixxxxtyp",
    "q5_abinxxxxtyp", "q5_nonprofitxxxxtyp", "q5_coopxxxxtyp",
    "q5_privatexxxxtyp", "q5_other",
    # Q6 — xxxxer types
    "q6_xxxxsameasxxxx", "q6_fedrxxxxtyp", "q6_provxxxxtyp",
    "q6_terrxxxxtyp", "q6_munixxxxtyp", "q6_abinxxxxtyp",
    "q6_nonprofitxxxxtyp", "q6_coopxxxxtyp", "q6_privatexxxxtyp", "q6_other",
    # Q7 — operation deficit xxx types
    "q7_fedrxxxtyp", "q7_provxxxtyp", "q7_terrxxxtyp", "q7_munixxxtyp",
    "q7_abinxxxtyp", "q7_other",
    # Q8 — funding agreement types
    "q8_fedrxxxtyp", "q8_provxxxtyp", "q8_terrxxxtyp", "q8_munixxxtyp",
    "q8_abinxxxtyp", "q8_other",
    # Q9 — clientele
    "q9_withchildren", "q9_singlewomen", "q9_singlemen", "q9_seniors",
    "q9_youth", "q9_immigrantsrefugees", "q9_physicaldisabilities",
    "q9_mentaldisabilities", "q9_veterans", "q9_firstnations", "q9_metis",
    "q9_inuit", "q9_domesticviolence", "q9_homelessness", "q9_other",
    # Q10 — rent inclusions
    "q10_heat", "q10_hotwater", "q10_electricity", "q10_parkingspace",
    "q10_cabletv", "q10_internetwifi", "q10_appliances", "q10_furnished",
    "q10_other",
    # Q12 — building materials
    "q12_allwoodframe", "q12_concretewallsframe", "q12_brickwallsframe",
    "q12_steelwallsframe", "q12_structsteelframe", "q12_other",
    # Q13 — heating
    "q13_forcedairfurnace", "q13_heatpump", "q13_electricbaseboard",
    "q13_hotwater", "q13_heatingstove", "q13_other",
    # Q14 — accessibility
    "q14_elevators", "q14_streetlevelentrance", "q14_widedoorways",
    "q14_accessramps", "q14_handrails", "q14_stairlift",
    "q14_appropriatedoordhdwr", "q14_electronicdooropener", "q14_keylessentry",
    "q14_accessibleparking", "q14_scooterstorage", "q14_pavedwalkways",
    "q14_other",
    # Q17 — repairs needed
    "q17_foundation", "q17_superstructure", "q17_roofing",
    "q17_exteriorenclosure", "q17_interiorconstruction", "q17_plumbing",
    "q17_electrical", "q17_heatingventilationac", "q17_units",
    "q17_elevators", "q17_fireprotection", "q17_exteriorsiteimprovement",
    "q17_other",
    # Misc
    "q23_otherrentmechanism",
    "rent_grid_is_valid",
]


def convert_boolean_columns(df: DataFrame) -> DataFrame:
    """
    Convert all boolean flag columns from native boolean / 'true'/'false'
    string representation to 'Yes' / 'No'.

    Steps:
        1. Cast each column to lowercase string (handles both Python bool and string).
        2. Map 'true' → 'Yes', 'false' → 'No', anything else → pass-through.

    Args:
        df: Input Spark DataFrame with raw boolean columns.

    Returns:
        DataFrame with boolean columns replaced by 'Yes' / 'No' strings.
    """
    # Step 1 — normalise to lowercase string
    for col_name in BOOLEAN_COLUMNS:
        df = df.withColumn(col_name, expr(f"lower(CAST({col_name} AS STRING))"))

    # Step 2 — map to Yes / No
    for col_name in BOOLEAN_COLUMNS:
        df = df.withColumn(col_name, expr(f"""
            CASE
                WHEN {col_name} = 'true'  THEN 'Yes'
                WHEN {col_name} = 'false' THEN 'No'
                ELSE CAST({col_name} AS STRING)
            END
        """))

    return df
