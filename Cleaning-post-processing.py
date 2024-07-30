import pandas as pd
import xml.etree.ElementTree as ET
import glob
import os
import argparse

def process_command_line_arguments():
    """Processes command line arguments for the script."""
    parser = argparse.ArgumentParser(description='Post Processing Process For LDL Content Migration Using Islandora Workbench')
    parser.add_argument('-c', '--csv_directory', type=str, help='Path to metadata', required=True)
    parser.add_argument('-f', '--files_directory', type=str, help='Path to the files', required=True)
    parser.add_argument('-o', '--output_directory', type=str, help='Path to the output CSV', required=True)
    return parser.parse_args()

def load_csv_data(csv_path):
    """Loads CSV data and returns a DataFrame with renamed columns."""
    df = pd.read_csv(csv_path, encoding='utf-8')
    df.rename(columns={'PID': 'id'}, inplace=True)
    return df

def extract_collection_and_file_names(ids):
    """Extracts collection names and file names from a list of IDs."""
    collections = []
    numbers = []
    file_names = []
    for id_value in ids:
        coll_name, coll_num = id_value.split(':')
        collections.append(coll_name)
        numbers.append(coll_num)
        file_names.append(f"{coll_name}_{coll_num}_OBJ")
    return file_names

def find_object_files(file_directory):
    """Finds object files in the given directory and returns their names and file format."""
    object_files = []
    file_format = ""
    for file in os.listdir(file_directory):
        if "OBJ" in file:
            object_files.append(file.split(".")[0])
            file_format = f".{file.split('.')[1]}"
    return object_files, file_format

def update_dataframe_with_file_info(df, file_names, object_files, file_format):
    """Updates the DataFrame with file paths where object files exist."""
    df['file'] = [
        f"Data/{file}{file_format}" if file in object_files else "" 
        for file in file_names
    ]
    return df

def add_additional_columns(df):
    """Adds additional required columns to the DataFrame."""
    df['parent_id'] = ""
    df['field_weight'] = ""
    df['field_member_of'] = ""
    df['field_model'] = "32"
    df['field_access_terms'] = "14"
    df['field_resource_type'] = "4"
    df.drop(["field_date_captured", "field_is_preceded_by", "field_is_succeeded_by"], inplace=True, axis=1, errors='ignore')
    df.fillna('', inplace=True)
    return df

def input_directory(csv_path, files_directory):
    """Processes the input directory, updating the DataFrame with file information."""
    df = load_csv_data(csv_path)
    file_names = extract_collection_and_file_names(df['id'].tolist())
    object_files, file_format = find_object_files(files_directory)
    df = update_dataframe_with_file_info(df, file_names, object_files, file_format)
    df = add_additional_columns(df)
    return df

def parse_rdf_data(rdf_path):
    """Parses RDF data and returns tags, attributes, and text."""
    tags, attrib, text = [], [], []
    for rdf_file in glob.glob(f"{rdf_path}/*.rdf"):
        rdf = ET.parse(rdf_file)
        for elem in rdf.iter():
            tags.append(elem.tag)
            attrib.append(elem.attrib)
            text.append(elem.text)
    return tags, attrib, text

def process_rdf_tags(tags, attrib, text):
    """Processes RDF tags and extracts necessary information."""
    tag_name, attributes, weights = [], [], []
    for tag, attr, txt in zip(tags, attrib, text):
        tag_name.append(tag.split('}')[-1])
        attributes.append(list(attr.values()))
        weights.append(txt if "isSequenceNumberOf" in tag else "")
    return tag_name, attributes, weights

def input_rdf(rdf_directory, df):
    """Processes RDF files to update DataFrame with 'parent_id', 'field_weight', and other fields."""
    tags, attrib, text = parse_rdf_data(rdf_directory)
    tag_name, attributes, weights = process_rdf_tags(tags, attrib, text)

    field_member_of, parent_id, weight = [], [], []

    for tag, attr, wt in zip(tag_name, attributes, weights):
        if tag == "isMemberOfCollection":
            collection = attr[0].split("/")[-1]
            field_member_of.append(collection)
            parent_id.append(collection)
        elif tag == "isPageOf" or tag == "isSequenceNumberOf":
            collection_name = rdf_directory.split("/")[-1]
            parent_number = attr[0].split(":")[-1]
            parent_id.append(f"{collection_name}:{parent_number}")
            weight.append(wt)
        else:
            field_member_of.append("")
            parent_id.append("")
            weight.append("")

    df["parent_id"] = parent_id
    df["field_weight"] = weight
    df["field_member_of"] = field_member_of
    df["field_edtf_date_created"] = ""
    df["field_linked_agent"] = ""

    return df

def write_output(df, output_directory, original_filename):
    """Writes the processed DataFrame to a CSV file."""
    output_filename = f"{output_directory}/post-processed-{original_filename}"
    df.to_csv(output_filename, index=False)
    print(f'Data post processed and written to CSV: {output_filename}')

def main():
    args = process_command_line_arguments()
    df = input_directory(args.csv_directory, args.files_directory)
    df = input_rdf(args.files_directory, df)
    original_filename = os.path.basename(args.csv_directory)
    write_output(df, args.output_directory, original_filename)

if __name__ == "__main__":
    main()
