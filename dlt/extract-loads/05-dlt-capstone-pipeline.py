# dlt/pipeline.py
import dlt, pandas as pd
import os

def file_path(filename):
    # Direct path to your CSV files in the staging directory
    return f"/home/chin00/-FTW-WISE-Capstone-/dlt/extract-loads/staging/{filename}"

@dlt.resource(name="first_second_class_municipalities")
def first_second_class_municipalities():
    yield pd.read_csv(file_path("cmci_1st_2nd_class_municipalities_2022.csv"))

@dlt.resource(name="third_fourth_class_municipalities")
def third_fourth_class_municipalities():
    yield pd.read_csv(file_path("cmci_3rd_4th_class_municipalities_2022.csv"))

@dlt.resource(name="component_cities")
def component_cities():
    yield pd.read_csv(file_path("cmci_component_cities_2022.csv"))

@dlt.resource(name="highly_urbanized_cities")
def highly_urbanized_cities():
    yield pd.read_csv(file_path("cmci_highly_urbanized_cities_2022.csv"))

def run():
    p = dlt.pipeline(
        pipeline_name="cmci_2022_pipeline",
        destination="clickhouse", 
        dataset_name="cmci_2022_data",
    )
    
    print("Loading 1st & 2nd Class Municipalities...")
    info1 = p.run(first_second_class_municipalities())
    print("1st & 2nd Class Municipalities loaded:", info1)
    
    print("Loading 3rd & 4th Class Municipalities...")
    info2 = p.run(third_fourth_class_municipalities())
    print("3rd & 4th Class Municipalities loaded:", info2)
    
    print("Loading Component Cities...")
    info3 = p.run(component_cities())
    print("Component Cities loaded:", info3)
    
    print("Loading Highly Urbanized Cities...")
    info4 = p.run(highly_urbanized_cities())
    print("Highly Urbanized Cities loaded:", info4)

if __name__ == "__main__":
    run()