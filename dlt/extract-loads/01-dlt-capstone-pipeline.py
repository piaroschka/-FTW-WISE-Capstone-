import os
import dlt, pandas as pd

# barmm
@dlt.resource(name="barmm_basilan")
def barmm_basilan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Basilan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_interim")
def barmm_interim():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Interim Province - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_lanaods")
def barmm_lanaods():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Lanao Del Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_maguindanao")
def barmm_maguindanao():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Maguindanao - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_sulu")
def barmm_sulu():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Sulu - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_tawi")
def barmm_tawi():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Tawi Tawi - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# car
@dlt.resource(name="car_abra")
def car_abra():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Abra - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_apayao")
def car_apayao():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Apayao - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_baguio")
def car_baguio():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Baguio City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_benguet")
def car_benguet():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Benguet - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_ifugao")
def car_ifugao():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Ifugao - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_kalinga")
def car_kalinga():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Kalinga - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_mountain")
def car_mountain():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Mountain Province - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# xiii - caraga
@dlt.resource(name="xiii_agusan_dn")
def xiii_agusan_dn():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Agusan Del Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_agusan_ds")
def xiii_agusan_ds():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Agusan Del Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_butuan")
def xiii_butuan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Butuan City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_dinagat")
def xiii_dinagat():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Dinagat Islands - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_surigao_dn")
def xiii_surigao_dn():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Surigao Del Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_surigao_ds")
def xiii_surigao_ds():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Surigao Del Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# ncr
@dlt.resource(name="ncr_caloocan")
def ncr_caloocan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Caloocan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_laspinas")
def ncr_laspinas():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Las Piñas - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_makati")
def ncr_makati():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Makati - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_malabon")
def ncr_malabon():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Malabon - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_mandaluyong")
def ncr_mandaluyong():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Mandaluyong - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_manila")
def ncr_manila():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Manila - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_marikina")
def ncr_marikina():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Marikina - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_muntinlupa")
def ncr_muntinlupa():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Muntinlupa - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_navotas")
def ncr_navotas():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Navotas - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_paranaque")
def ncr_paranaque():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Parañaque - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_pasay")
def ncr_pasay():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pasay - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_pasig")
def ncr_pasig():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pasig - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_pateros")
def ncr_pateros():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pateros - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_quezon")
def ncr_quezon():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Quezon City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_sanjuan")
def ncr_sanjuan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 San Juan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_taguig")
def ncr_taguig():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Taguig - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_valenzuela")
def ncr_valenzuela():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Valenzuela - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region i
@dlt.resource(name="i_ilocos_n")
def i_ilocos_n():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Ilocos Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="i_ilocos_s")
def i_ilocos_s():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Ilocos Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="i_launion")
def i_launion():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 La Union - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="i_pangasinan")
def i_pangasinan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pangasinan MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region ii
@dlt.resource(name="ii_batanes")
def ii_batanes():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Batanes - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ii_cagayan")
def ii_cagayan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Cagayan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ii_isabela")
def ii_isabela():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Isabela - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ii_nueva_v")
def ii_nueva_v():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Nueva Vizcaya - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ii_quirino")
def ii_quirino():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Quirino - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region iii

# region iv-a
@dlt.resource(name="iv_a_batangas")
def iv_a_batangas():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Batangas - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_a_cavite")
def iva_cavite():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Cavite - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_a_laguna")
def iva_laguna():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Laguna - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_a_lucena")
def iva_lucena():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Lucena City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_a_quezon")
def iva_quezon():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Quezon - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_a_rizal")
def iva_rizal():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Rizal - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region iv-b
@dlt.resource(name="iv_b_marinduque2")
def iv_b_marinduque2():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Marinduque - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_b_occ_mindoro")
def iv_b_occ_mindoro():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Occidental Mindoro - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_b_or_mindoro")
def iv_b_or_mindoro():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Oriental Mindoro - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_b_palawan2")
def iv_b_palawan2():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Palawan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_b_puerto2")
def iv_b_puerto2():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Puerto Princesa - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iv_b_romblon2")
def iv_b_romblon2():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Romblon - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region v
@dlt.resource(name="v_albay")
def v_albay():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Albay - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="v_cam_norte")
def v_cam_norte():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Camarines Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="v_cam_sur")
def v_cam_sur():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Camarines Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="v_catanduanes")
def v_catanduanes():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Catanduanes - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="v_masbate")
def v_masbate():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Masbate - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="v_sorsogon")
def v_sorsogon():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Sorsogon - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region vi
@dlt.resource(name="vi_aklan")
def vi_aklan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Aklan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vi_antique")
def vi_antique():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Antique - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vi_bacolod")
def vi_bacolod():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Bacolod City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vi_capiz")
def vi_capiz():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Capiz - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vi_guimaras")
def vi_guimaras():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Guimaras - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vi_iloilo")
def vi_iloilo():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Iloilo - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vi_iloilo_c")
def vi_iloilo_c():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Iloilo City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vi_negros_occ")
def vi_negros_occ():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Negros Occidental - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region vii
@dlt.resource(name="vii_bohol")
def vii_bohol():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Bohol - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vii_cebu")
def vii_cebu():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Cebu - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vii_cebu_c")
def vii_cebu_c():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Cebu City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vii_lapu")
def vii_lapu():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Lapu Lapu City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vii_mandaue")
def vii_mandaue():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Mandaue City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vii_negros_or")
def vii_negros_or():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Negros Oriental - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="vii_siquijor")
def vii_siquijor():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Siquijor - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region viii
@dlt.resource(name="viii_biliran")
def viii_biliran():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Biliran - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="viii_esamar")
def viii_esamar():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Eastern Samar - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="viii_leyte")
def viii_leyte():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Leyte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="viii_nsamar")
def viii_nsamar():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Northern Samar - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="viii_samar")
def viii_samar():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Samar - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="viii_sleyte")
def viii_sleyte():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Southern Leyte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="viii_tacloban")
def viii_tacloban():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Tacloban City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region ix
@dlt.resource(name="ix_isabela")
def ix_isabela():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Isabela City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ix_zamboanga_c")
def ix_zamboanga_c():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Zamboanga City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ix_zamboanga_dn")
def ix_zamboanga_dn():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Zamboanga Del Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ix_zamboanga_ds")
def ix_zamboanga_ds():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Zamboanga Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ix_zamboanga_s")
def ix_zamboanga_s():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Zamboanga Sibugay - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region x
@dlt.resource(name="x_bukidnon")
def x_bukidnon():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Bukidnon - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="x_cagayan_do")
def x_cagayan_do():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Cagayan De Oro City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="x_camiguin")
def x_camiguin():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Camiguin - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="x_iligan")
def x_iligan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Iligan City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="x_lanao_dn")
def x_lanao_dn():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Lanao Del Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="x_misamis_occ")
def x_misamis_occ():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Misamis Occidental - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="x_misamis_or")
def x_misamis_or():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Misamis Oriental - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region xi
@dlt.resource(name="xi_compostela")
def xi_compostela():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Compostela Valley - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xi_davao_c")
def xi_davao_c():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Davao City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xi_davao_dn")
def xi_davao_dn():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Davao Del Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xi_davao_ds")
def xi_davao_ds():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Davao Del Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xi_davao_occ")
def xi_davao_occ():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Davao Occidental - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xi_davao_or")
def xi_davao_or():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Davao Oriental - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region xii
@dlt.resource(name="xii_gensan")
def xii_gensan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 General Santos City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xii_ncotabato")
def xii_ncotabato():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 North Cotabato - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xii_sarangani")
def xii_sarangani():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Sarangani - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xii_scotabato")
def xii_scotabato():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 South Cotabato - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xii_sultan")
def xii_sultan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Sultan Kudarat - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)
    
# dim tables
@dlt.resource(name="citizenship")
def citizenship():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "citizenship.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="education")
def education():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "education.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="geography")
def geography():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "geography_id.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="marital_stat")
def marital_stat():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "marital_status.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="sex")
def sex():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "sex.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="urban")
def urban():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "urban-rural.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="region")
def region():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "region.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

def run():
    p = dlt.pipeline(
        pipeline_name="01-dlt-capstone-pipeline",
        destination="clickhouse",
        dataset_name="psa_census_2020",
    )
    print("Fetching and loading...")

    # barmm
    info1 = p.run(barmm_basilan())
    print("records loaded:", info1)
    info1 = p.run(barmm_interim())
    print("records loaded:", info1)
    info1 = p.run(barmm_lanaods())
    print("records loaded:", info1)
    info1 = p.run(barmm_maguindanao())
    print("records loaded:", info1)
    info1 = p.run(barmm_sulu())
    print("records loaded:", info1)
    info1 = p.run(barmm_tawi())
    print("records loaded:", info1)

    # car
    info1 = p.run(car_abra())
    print("records loaded:", info1)
    info1 = p.run(car_apayao())
    print("records loaded:", info1)
    info1 = p.run(car_baguio())
    print("records loaded:", info1)
    info1 = p.run(car_benguet())
    print("records loaded:", info1)
    info1 = p.run(car_ifugao())
    print("records loaded:", info1)
    info1 = p.run(car_kalinga())
    print("records loaded:", info1)
    info1 = p.run(car_mountain())
    print("records loaded:", info1)

    # xiii
    info1 = p.run(xiii_agusan_dn())
    print("records loaded:", info1)
    info1 = p.run(xiii_agusan_ds())
    print("records loaded:", info1)
    info1 = p.run(xiii_butuan())
    print("records loaded:", info1)
    info1 = p.run(xiii_dinagat())
    print("records loaded:", info1)
    info1 = p.run(xiii_surigao_dn())
    print("records loaded:", info1)
    info1 = p.run(xiii_surigao_ds())
    print("records loaded:", info1)

    # ncr
    info1 = p.run(ncr_caloocan())
    print("records loaded:", info1)
    info1 = p.run(ncr_laspinas())
    print("records loaded:", info1)
    info1 = p.run(ncr_makati())
    print("records loaded:", info1)
    info1 = p.run(ncr_malabon())
    print("records loaded:", info1)
    info1 = p.run(ncr_mandaluyong())
    print("records loaded:", info1)
    info1 = p.run(ncr_manila())
    print("records loaded:", info1)
    info1 = p.run(ncr_marikina())
    print("records loaded:", info1)
    info1 = p.run(ncr_muntinlupa())
    print("records loaded:", info1)
    info1 = p.run(ncr_navotas())
    print("records loaded:", info1)
    info1 = p.run(ncr_paranaque())
    print("records loaded:", info1)
    info1 = p.run(ncr_pasay())
    print("records loaded:", info1)
    info1 = p.run(ncr_pasig())
    print("records loaded:", info1)
    info1 = p.run(ncr_pateros())
    print("records loaded:", info1)
    info1 = p.run(ncr_quezon())
    print("records loaded:", info1)
    info1 = p.run(ncr_sanjuan())
    print("records loaded:", info1)
    info1 = p.run(ncr_taguig())
    print("records loaded:", info1)
    info1 = p.run(ncr_valenzuela())
    print("records loaded:", info1)

# region i
    info1 = p.run(i_ilocos_n())
    print("records loaded:", info1)
    info1 = p.run(i_ilocos_s())
    print("records loaded:", info1)
    info1 = p.run(i_launion())
    print("records loaded:", info1)
    info1 = p.run(i_pangasinan())
    print("records loaded:", info1)

# region ii
    info1 = p.run(ii_batanes())
    print("records loaded:", info1)
    info1 = p.run(ii_cagayan())
    print("records loaded:", info1)
    info1 = p.run(ii_isabela())
    print("records loaded:", info1)
    info1 = p.run(ii_nueva_v())
    print("records loaded:", info1)
    info1 = p.run(ii_quirino())
    print("records loaded:", info1)

# region iii
    
# region iv-a
    info1 = p.run(iv_a_batangas())
    print("records loaded:", info1)
    info1 = p.run(iv_a_cavite())
    print("records loaded:", info1)
    info1 = p.run(iv_a_laguna())
    print("records loaded:", info1)
    info1 = p.run(iv_a_lucena())
    print("records loaded:", info1)
    info1 = p.run(iv_a_quezon())
    print("records loaded:", info1)
    info1 = p.run(iv_a_rizal())
    print("records loaded:", info1)

# region iv-b
    info1 = p.run(iv_b_marinduque())
    print("records loaded:", info1)
    info1 = p.run(iv_b_occidental_m())
    print("records loaded:", info1)
    info1 = p.run(iv_b_oriental_m())
    print("records loaded:", info1)
    info1 = p.run(iv_b_palawan())
    print("records loaded:", info1)
    info1 = p.run(iv_b_puerto())
    print("records loaded:", info1)
    info1 = p.run(iv_b_romblon())
    print("records loaded:", info1)

# region v
    info1 = p.run(v_albay())
    print("records loaded:", info1)
    info1 = p.run(v_cam_norte())
    print("records loaded:", info1)
    info1 = p.run(v_cam_sur())
    print("records loaded:", info1)
    info1 = p.run(v_catanduanes())
    print("records loaded:", info1)
    info1 = p.run(v_masbate())
    print("records loaded:", info1)
    info1 = p.run(v_sorsogon())
    print("records loaded:", info1)

# region vi
    info1 = p.run(vi_aklan())
    print("records loaded:", info1)
    info1 = p.run(vi_antique())
    print("records loaded:", info1)
    info1 = p.run(vi_bacolod())
    print("records loaded:", info1)
    info1 = p.run(vi_capiz())
    print("records loaded:", info1)
    info1 = p.run(vi_guimaras())
    print("records loaded:", info1)
    info1 = p.run(vi_iloilo())
    print("records loaded:", info1)
    info1 = p.run(vi_iloilo_c())
    print("records loaded:", info1)
    info1 = p.run(vi_negros_occ())
    print("records loaded:", info1)

# region vii
    info1 = p.run(vii_bohol())
    print("records loaded:", info1)
    info1 = p.run(vii_cebu())
    print("records loaded:", info1)
    info1 = p.run(vii_cebu_c())
    print("records loaded:", info1)
    info1 = p.run(vii_lapu())
    print("records loaded:", info1)
    info1 = p.run(vii_mandaue())
    print("records loaded:", info1)
    info1 = p.run(vii_negros_or())
    print("records loaded:", info1)
    info1 = p.run(vii_siquijor())
    print("records loaded:", info1)

# region viii



# region ix
    info1 = p.run(ix_isabela())
    print("records loaded:", info1)
    info1 = p.run(ix_zamboanga_c())
    print("records loaded:", info1)
    info1 = p.run(ix_zamboanga_dn())
    print("records loaded:", info1)
    info1 = p.run(ix_zamboanga_ds())
    print("records loaded:", info1)
    info1 = p.run(ix_zamboanga_s())
    print("records loaded:", info1)

# region x
    info1 = p.run(x_bukidnon())
    print("records loaded:", info1)
    info1 = p.run(x_cagayan_do())
    print("records loaded:", info1)
    info1 = p.run(x_camiguin())
    print("records loaded:", info1)
    info1 = p.run(x_iligan())
    print("records loaded:", info1)
    info1 = p.run(x_lanao_dn())
    print("records loaded:", info1)
    info1 = p.run(x_misamis_occ())
    print("records loaded:", info1)
    info1 = p.run(x_misamis_or())
    print("records loaded:", info1)

# region xi
    info1 = p.run(xi_compostela())
    print("records loaded:", info1)
    info1 = p.run(xi_davao_c())
    print("records loaded:", info1)
    info1 = p.run(xi_davao_dn())
    print("records loaded:", info1)
    info1 = p.run(xi_davao_ds())
    print("records loaded:", info1)
    info1 = p.run(xi_davao_occ())
    print("records loaded:", info1)
    info1 = p.run(xi_davao_or())
    print("records loaded:", info1)

# region xii
    info1 = p.run(xii_gensan())
    print("records loaded:", info1)
    info1 = p.run(xii_ncotabato())
    print("records loaded:", info1)
    info1 = p.run(xii_sarangani())
    print("records loaded:", info1)
    info1 = p.run(xii_scotabato())
    print("records loaded:", info1)
    info1 = p.run(xii_sultan())
    print("records loaded:", info1)

# dim tables
    info1 = p.run(citizenship())
    print("records loaded:", info1)
    info1 = p.run(education())
    print("records loaded:", info1)
    info1 = p.run(geography())
    print("records loaded:", info1)
    info1 = p.run(marital_stat())
    print("records loaded:", info1)
    info1 = p.run(sex())
    print("records loaded:", info1)
    info1 = p.run(urban())
    print("records loaded:", info1)
    info1 = p.run(region())
    print("records loaded:", info1)

if __name__ == "__main__":
    run()

    # to run
    # docker compose --profile jobs run --rm \  
    # dlt python extract-loads/01-dlt-capstone-pipeline.py
