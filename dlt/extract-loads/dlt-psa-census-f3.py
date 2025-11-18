import dlt
import pandas as pd
import os



def file_path(filename):
    ROOT_DIR = os.path.dirname(__file__)
    return os.path.join(ROOT_DIR, "staging", "psacensus2020f3", filename)

CITY_FILES = [
    # ------------------ NCR ------------------
    ("valenzuela",      "CPH PUF 2020 Valenzuela - HOUSEHOLD.CSV", "NCR"),
    ("san_juan",        "CPH PUF 2020 San Juan - HOUSEHOLD.CSV", "NCR"),
    ("taguig",          "CPH PUF 2020 Taguig - HOUSEHOLD.CSV", "NCR"),
    ("pasig",           "CPH PUF 2020 Pasig - HOUSEHOLD.CSV", "NCR"),
    ("pateros",         "CPH PUF 2020 Pateros - HOUSEHOLD.CSV", "NCR"),
    ("quezon_city",     "CPH PUF 2020 Quezon City - HOUSEHOLD.CSV", "NCR"),
    ("marikina",        "CPH PUF 2020 Marikina - HOUSEHOLD.CSV", "NCR"),
    ("muntinlupa",      "CPH PUF 2020 Muntinlupa - HOUSEHOLD.CSV", "NCR"),
    ("navotas",         "CPH PUF 2020 Navotas - HOUSEHOLD.CSV", "NCR"),
    ("paranaque",       "CPH PUF 2020 Parañaque - HOUSEHOLD.CSV", "NCR"),
    ("pasay",           "CPH PUF 2020 Pasay - HOUSEHOLD.CSV", "NCR"),
    ("makati",          "CPH PUF 2020 Makati - HOUSEHOLD.CSV", "NCR"),
    ("malabon",         "CPH PUF 2020 Malabon - HOUSEHOLD.CSV", "NCR"),
    ("mandaluyong",     "CPH PUF 2020 Mandaluyong - HOUSEHOLD.CSV", "NCR"),
    ("manila",          "CPH PUF 2020 Manila - HOUSEHOLD.CSV", "NCR"),
    ("caloocan",        "CPH PUF 2020 Caloocan - HOUSEHOLD.CSV", "NCR"),
    ("las_pinas",       "CPH PUF 2020 Las Piñas - HOUSEHOLD.CSV", "NCR"),

    # ------------------ CAR ------------------
    ("benguet",               "CPH PUF 2020 Benguet - HOUSEHOLD.CSV", "CAR"),
    ("ifugao",                "CPH PUF 2020 Ifugao - HOUSEHOLD.CSV", "CAR"),
    ("kalinga",               "CPH PUF 2020 Kalinga - HOUSEHOLD.CSV", "CAR"),
    ("mountain_province",     "CPH PUF 2020 Mountain Province - HOUSEHOLD.CSV", "CAR"),
    ("abra",                  "CPH PUF 2020 Abra - HOUSEHOLD.CSV", "CAR"),
    ("apayao",                "CPH PUF 2020 Apayao - HOUSEHOLD.CSV", "CAR"),
    ("baguio_city",           "CPH_PUF_2020_Baguio - HOUSEHOLD.CSV", "CAR"),

    # ------------------ REGION I ------------------
    ("ilocos_norte",     "CPH PUF 2020 Ilocos Norte - HOUSEHOLD.CSV", "Region I"),
    ("ilocos_sur",       "CPH PUF 2020 Ilocos Sur - HOUSEHOLD.CSV", "Region I"),
    ("la_union",         "CPH PUF 2020 La Union - HOUSEHOLD.CSV", "Region I"),
    ("pangasinan",       "CPH PUF 2020 Pangasinan - HOUSEHOLD.CSV", "Region I"),

    # ------------------ REGION II ------------------
    ("batanes",          "CPH PUF 2020 Batanes - HOUSEHOLD.CSV", "Region II"),
    ("cagayan",          "CPH PUF 2020 Cagayan - HOUSEHOLD.CSV", "Region II"),
    ("isabela",          "CPH PUF 2020 Isabela - HOUSEHOLD.CSV", "Region II"),
    ("nueva_vizcaya",    "CPH PUF 2020 Nueva Vizcaya - HOUSEHOLD.CSV", "Region II"),
    ("quirino",          "CPH PUF 2020 Quirino - HOUSEHOLD.CSV", "Region II"),

    # ------------------ REGION III (CENTRAL LUZON) ------------------
    ("angeles_city",     "CPH PUF 2020 Angeles City - HOUSEHOLD.CSV", "Region III"),
    ("aurora",           "CPH PUF 2020 Aurora - HOUSEHOLD.CSV", "Region III"),
    ("bataan",           "CPH PUF 2020 Bataan - HOUSEHOLD.CSV", "Region III"),
    ("bulacan",          "CPH PUF 2020 Bulacan - HOUSEHOLD.CSV", "Region III"),
    ("nueva_ecija",      "CPH PUF 2020 Nueva Ecija - HOUSEHOLD.CSV", "Region III"),
    ("olongapo_city",    "CPH PUF 2020 Olongapo City - HOUSEHOLD.CSV", "Region III"),
    ("pampanga",         "CPH PUF 2020 Pampanga - HOUSEHOLD.CSV", "Region III"),
    ("tarlac",           "CPH PUF 2020 Tarlac - HOUSEHOLD.CSV", "Region III"),
    ("zambales",         "CPH PUF 2020 Zambales - HOUSEHOLD.CSV", "Region III"),

    # ------------------ REGION IV-A (CALABARZON) ------------------
    ("batangas",         "CPH PUF 2020 Batangas - HOUSEHOLD.CSV", "Region IV-A"),
    ("cavite",           "CPH PUF 2020 Cavite - HOUSEHOLD.CSV", "Region IV-A"),
    ("laguna",           "CPH PUF 2020 Laguna - HOUSEHOLD.CSV", "Region IV-A"),
    ("lucena_city",      "CPH PUF 2020 Lucena City - HOUSEHOLD.CSV", "Region IV-A"),
    ("quezon",           "CPH PUF 2020 Quezon - HOUSEHOLD.CSV", "Region IV-A"),
    ("rizal",            "CPH PUF 2020 Rizal - HOUSEHOLD.CSV", "Region IV-A"),

    # ------------------ REGION IV-B (MIMAROPA) ------------------
    ("marinduque",           "CPH PUF 2020 Marinduque - HOUSEHOLD.CSV", "Region IV-B"),
    ("occidental_mindoro",   "CPH PUF 2020 Occidental Mindoro - HOUSEHOLD.CSV", "Region IV-B"),
    ("oriental_mindoro",     "CPH PUF 2020 Oriental Mindoro - HOUSEHOLD.CSV", "Region IV-B"),
    ("palawan",              "CPH PUF 2020 Palawan - HOUSEHOLD.CSV", "Region IV-B"),
    ("puerto_princesa",      "CPH PUF 2020 Puerto Princesa - HOUSEHOLD.CSV", "Region IV-B"),
    ("romblon",              "CPH PUF 2020 Romblon - HOUSEHOLD.CSV", "Region IV-B"),

    # ------------------ REGION V (Bicol Region) ------------------
    ("albay",             "CPH PUF 2020 Albay - HOUSEHOLD.CSV", "Region V"),
    ("camarines_norte",   "CPH PUF 2020 Camarines Norte - HOUSEHOLD.CSV", "Region V"),
    ("camarines_sur",     "CPH PUF 2020 Camarines Sur - HOUSEHOLD.CSV", "Region V"),
    ("catanduanes",       "CPH PUF 2020 Catanduanes - HOUSEHOLD.CSV", "Region V"),
    ("masbate",           "CPH PUF 2020 Masbate - HOUSEHOLD.CSV", "Region V"),
    ("sorsogon",          "CPH PUF 2020 Sorsogon - HOUSEHOLD.CSV", "Region V"),

    # ------------------ REGION VI (Western Visayas) ------------------
    ("aklan",                 "CPH PUF 2020 Aklan - HOUSEHOLD.CSV", "Region VI"),
    ("antique",               "CPH PUF 2020 Antique - HOUSEHOLD.CSV", "Region VI"),
    ("bacolod_city",          "CPH PUF 2020 Bacolod City - HOUSEHOLD.CSV", "Region VI"),
    ("capiz",                 "CPH PUF 2020 Capiz - HOUSEHOLD.CSV", "Region VI"),
    ("guimaras",              "CPH PUF 2020 Guimaras - HOUSEHOLD.CSV", "Region VI"),
    ("iloilo",                "CPH PUF 2020 Iloilo - HOUSEHOLD.CSV", "Region VI"),
    ("iloilo_city",           "CPH PUF 2020 Iloilo City - HOUSEHOLD.CSV", "Region VI"),
    ("negros_occidental",     "CPH PUF 2020 Negros Occidental - HOUSEHOLD.CSV", "Region VI"),

    # ------------------ REGION VII (Central Visayas) ------------------
    ("bohol",                 "CPH PUF 2020 Bohol - HOUSEHOLD.CSV", "Region VII"),
    ("cebu",                  "CPH PUF 2020 Cebu - HOUSEHOLD.CSV", "Region VII"),
    ("cebu_city",             "CPH PUF 2020 Cebu City - HOUSEHOLD.CSV", "Region VII"),
    ("lapu_lapu_city",        "CPH PUF 2020 Lapu Lapu City - HOUSEHOLD.CSV", "Region VII"),
    ("mandaue_city",          "CPH PUF 2020 Mandaue City - HOUSEHOLD.CSV", "Region VII"),
    ("negros_oriental",       "CPH PUF 2020 Negros Oriental - HOUSEHOLD.CSV", "Region VII"),
    ("siquijor",              "CPH PUF 2020 Siquijor - HOUSEHOLD.CSV", "Region VII"),

    # ------------------ REGION VIII (Eastern Visayas) ------------------
    ("biliran",               "CPH PUF 2020 Biliran - HOUSEHOLD.CSV", "Region VIII"),
    ("eastern_samar",         "CPH PUF 2020 Eastern Samar - HOUSEHOLD.CSV", "Region VIII"),
    ("leyte",                 "CPH PUF 2020 Leyte - HOUSEHOLD.CSV", "Region VIII"),
    ("northern_samar",        "CPH PUF 2020 Northern Samar - HOUSEHOLD.CSV", "Region VIII"),
    ("southern_leyte",        "CPH PUF 2020 Southern Leyte - HOUSEHOLD.CSV", "Region VIII"),
    ("tacloban_city",         "CPH PUF 2020 Tacloban City - HOUSEHOLD.CSV", "Region VIII"),
    ("western_samar",         "CPH PUF 2020 Western Samar - HOUSEHOLD.CSV", "Region VIII"),

    # ------------------ REGION IX (Zamboanga Peninsula) ------------------
    ("isabela_city",          "CPH PUF 2020 Isabela City - HOUSEHOLD.CSV", "Region IX"),
    ("zamboanga_city",        "CPH PUF 2020 Zamboanga City - HOUSEHOLD.CSV", "Region IX"),
    ("zamboanga_del_norte",   "CPH PUF 2020 Zamboanga Del Norte - HOUSEHOLD.CSV", "Region IX"),
    ("zamboanga_del_sur",     "CPH PUF 2020 Zamboanga Del Sur - HOUSEHOLD.CSV", "Region IX"),
    ("zamboanga_sibugay",     "CPH PUF 2020 Zamboanga Sibugay - HOUSEHOLD.CSV", "Region IX"),

    # ------------------ REGION X (Northern Mindanao) ------------------
    ("bukidnon",              "CPH PUF 2020 Bukidnon - HOUSEHOLD.CSV", "Region X"),
    ("cagayan_de_oro",        "CPH PUF 2020 Cagayan De Oro - HOUSEHOLD.CSV", "Region X"),
    ("camiguin",              "CPH PUF 2020 Camiguin - HOUSEHOLD.CSV", "Region X"),
    ("iligan_city",           "CPH PUF 2020 Iligan City - HOUSEHOLD.CSV", "Region X"),
    ("lanao_del_norte",       "CPH PUF 2020 Lanao Del Norte - HOUSEHOLD.CSV", "Region X"),
    ("misamis_occidental",    "CPH PUF 2020 Misamis Occidental - HOUSEHOLD.CSV", "Region X"),
    ("misamis_oriental",      "CPH PUF 2020 Misamis Oriental - HOUSEHOLD.CSV", "Region X"),

    # ------------------ REGION XI (Davao Region) ------------------
    ("compostela_valley",     "CPH PUF 2020 Compostela Valley - HOUSEHOLD.CSV", "Region XI"),
    ("davao_city",            "CPH PUF 2020 Davao City - HOUSEHOLD.CSV", "Region XI"),
    ("davao_del_norte",       "CPH PUF 2020 Davao Del Norte - HOUSEHOLD.CSV", "Region XI"),
    ("davao_del_sur",         "CPH PUF 2020 Davao Del Sur - HOUSEHOLD.CSV", "Region XI"),
    ("davao_occidental",      "CPH PUF 2020 Davao Occidental - HOUSEHOLD.CSV", "Region XI"),
    ("davao_oriental",        "CPH PUF 2020 Davao Oriental - HOUSEHOLD.CSV", "Region XI"),

    # ------------------ REGION XII (SOCCSKSARGEN) ------------------
    ("general_santos_city",   "CPH PUF 2020 General Santos City - HOUSEHOLD.CSV", "Region XII"),
    ("north_cotabato",        "CPH PUF 2020 North Cotabato - HOUSEHOLD.CSV", "Region XII"),
    ("sarangani",             "CPH PUF 2020 Sarangani - HOUSEHOLD.CSV", "Region XII"),
    ("south_cotabato",        "CPH PUF 2020 South Cotabato - HOUSEHOLD.CSV", "Region XII"),
    ("sultan_kudarat",        "CPH PUF 2020 Sultan Kudarat - HOUSEHOLD.CSV", "Region XII"),

    # ------------------ REGION XIII (Caraga) ------------------
    ("agusan_del_norte",      "CPH PUF 2020 Agusan Del Norte - HOUSEHOLD.CSV", "Region XIII"),
    ("agusan_del_sur",        "CPH PUF 2020 Agusan Del Sur - HOUSEHOLD.CSV", "Region XIII"),
    ("butuan_city",           "CPH PUF 2020 Butuan City - HOUSEHOLD.CSV", "Region XIII"),
    ("dinagat_islands",       "CPH PUF 2020 Dinagat Islands - HOUSEHOLD.CSV", "Region XIII"),
    ("surigao_del_norte",     "CPH PUF 2020 Surigao Del Norte - HOUSEHOLD.CSV", "Region XIII"),
    ("surigao_del_sur",       "CPH PUF 2020 Surigao Del Sur - HOUSEHOLD.CSV", "Region XIII"),

    # ------------------ BARMM (Bangsamoro Autonomous Region) ------------------
    ("basilan",               "CPH PUF 2020 Basilan - HOUSEHOLD.CSV", "BARMM"),
    ("interim_province",      "CPH PUF 2020 Interim Province - HOUSEHOLD.CSV", "BARMM"),
    ("lanao_del_sur",         "CPH PUF 2020 Lanao Del Sur - HOUSEHOLD.CSV", "BARMM"),
    ("maguindanao",           "CPH PUF 2020 Maguindanao - HOUSEHOLD.CSV", "BARMM"),
    ("sulu",                  "CPH PUF 2020 Sulu - HOUSEHOLD.CSV", "BARMM"),
    ("tawi_tawi",             "CPH PUF 2020 Tawi Tawi - HOUSEHOLD.CSV", "BARMM"),
]


@dlt.resource(name="all_regions")
def psa_all_regions():
    for city, filename, region in CITY_FILES:
        df = pd.read_csv(file_path(filename))
        df["city"] = city
        df["region"] = region
        yield df


# ---------------------------------------------------------
# PIPELINE EXECUTION
# ---------------------------------------------------------
def run():
    p = dlt.pipeline(
        pipeline_name="dlt-psa-census-f3",
        destination="clickhouse",
        dataset_name="psa_census_2020_f3",
    )

    print("Fetching and loading...")
    info = p.run(psa_all_regions())
    print(info)


if __name__ == "__main__":
    run()