import xml.etree.ElementTree as ET
import csv
from os import listdir
import re
import pandas as pd
import argparse
import datetime

# Constants
ATTRIBUTES_KEY = 'atributes'
TAGS_KEY = 'tags'
FREQUENCY_KEY = 'frequency'

# Global variables
paths_counts = {}
check = set()
all_tags = []
all_attributes = []
unique_tag_dict = {}
unique_attrib_dict = {}

def process_command_line_arguments():
    parser = argparse.ArgumentParser(description='Attribute and Tag finder for all the collections')
    parser.add_argument('-i', '--input_directory', type=str, required=True, help='Path to the input directory')
    parser.add_argument('-oat', '--output_attribs_tags', type=str, required=True, help='Path to the output attribute and tag list text file')
    parser.add_argument('-c', '--input_csv', type=str, required=False, help='Path to the input CSV file')
    parser.add_argument('-cc', '--input_clear_csv', type=str, required=False, help='Path to the input CSV file with needed fields')
    parser.add_argument('-o', '--output_directory', type=str, required=False, help='Path to the output CSV containing paths, frequency, and error reports')
    return parser.parse_args()

def process_xml_files(args, dataframe=None):
    files = listdir(args.input_directory)
    files.sort()
    for file in files:
        if file.endswith(".xml"):
            file_path = f"{args.input_directory}/{file}"
            print(f"Parsing {file}...")
            root = ET.iterparse(file_path, events=('start', 'end'))
            yield parse_xml(root, dataframe, args)

def unique_tag_attrib(tags, attributes):
    tag_check = []
    for tag in tags:
        if tag not in tag_check:
            tag_check.append(tag)
            unique_tag_dict[tag] = 0
        else:
            unique_tag_dict[tag] += 1

    attrib_check = []
    for attrib in attributes:
        if attrib not in attrib_check:
            attrib_check.append(attrib)
            unique_attrib_dict[attrib] = 0
        else:
            unique_attrib_dict[attrib] += 1

def uniq_data_to_dict():
    data = {
        ATTRIBUTES_KEY: [],
        f'{ATTRIBUTES_KEY} {FREQUENCY_KEY}': [],
        TAGS_KEY: [],
        f'{TAGS_KEY} {FREQUENCY_KEY}': []
    }

    for attrib, count in unique_attrib_dict.items():
        data[ATTRIBUTES_KEY].append(attrib)
        data[f'{ATTRIBUTES_KEY} {FREQUENCY_KEY}'].append(count)

    for tag, count in unique_tag_dict.items():
        data[TAGS_KEY].append(tag)
        data[f'{TAGS_KEY} {FREQUENCY_KEY}'].append(count)

    max_length = max(len(data[ATTRIBUTES_KEY]), len(data[TAGS_KEY]))

    for _ in range(max_length - len(data[ATTRIBUTES_KEY])):
        data[ATTRIBUTES_KEY].append("NONE")
        data[f'{ATTRIBUTES_KEY} {FREQUENCY_KEY}'].append("")

    for _ in range(max_length - len(data[TAGS_KEY])):
        data[TAGS_KEY].append("NONE")
        data[f'{TAGS_KEY} {FREQUENCY_KEY}'].append("")

    return data

def load_csv_data(file_path):
    return pd.read_csv(file_path)

def get_paths_counts_and_errors(paths):
    paths_counts = {}
    errors = []
    for path in paths:
        if path not in paths_counts:
            paths_counts[path] = 1
        else:
            paths_counts[path] += 1
        if path not in check:
            check.add(path)
        else:
            errors.append(path)
    return paths_counts, errors

def save_to_csv(data, file_path):
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")

def parse_xml(root, dataframe, args):
    all_generated_paths = {}
    path_name = []
    path_list = []
    result_dict_temp = {}
    result_dict_final = {}

    for event, elem in root:
        if event == 'start':
            all_tags.append(elem.tag.split("}")[-1])
            attributes = elem.attrib
            all_attributes.extend(attributes.keys())

            if args.input_csv or args.input_clear_csv:
                handle_csv_paths(attributes, dataframe, elem, all_generated_paths, path_name, path_list, args)

        if event == 'end':
            path_name.pop()

    if dataframe is None and not args.input_csv:
        unique_tag_attrib(all_tags, all_attributes)

    if args.input_csv:
        return path_list

    if args.input_clear_csv:
        result_dict_final = {k: '|'.join(v) for k, v in result_dict_final.items()}
        return compare_and_write(result_dict_final, dataframe)

def handle_csv_paths(attributes, dataframe, elem, all_generated_paths, path_name, path_list, args):
    write_attributes = []

    for key, value in attributes.items():
        write_attributes.append([key, value])
        if key not in dataframe[ATTRIBUTES_KEY].values:
            errors.append(key)
        if elem.tag.split("}")[-1] not in dataframe[TAGS_KEY].values:
            errors.append(elem.tag.split("}")[-1])

    if len(attributes) > 1:
        all_generated_paths = {}
        for attr in write_attributes:
            path = f"{elem.tag.split('}')[-1]} [@{attr[0]}= '{attr[1]}']"
            path_list.append(path)
            all_generated_paths[path] = attr[1]

    elif len(attributes) == 1:
        path = f"{elem.tag.split('}')[-1]} [{write_attributes[0][0]}= '{write_attributes[0][1]}']"
        path_list.append(path)

    if len(attributes) == 0:
        path = elem.tag.split("}")[-1]
        path_list.append(path)

def compare_and_write(final_dict, dataframe):
    field_with_text = {field: [] for field in dataframe["Fields"]}

    for paths, values in final_dict.items():
        if paths in dataframe["XMLPath"].values:
            field_name = dataframe.loc[dataframe["XMLPath"] == paths, "Fields"].values[0]
            if values and field_name in field_with_text:
                field_with_text[field_name].append(values)

    field_with_text.pop("nan", None)

    test_result(field_with_text)
    return field_with_text

def test_result(field_with_text):
    print("TEST RESULT(fields with data) IN CSV:\n")
    for counter, (field, values) in enumerate(field_with_text.items(), start=1):
        if values:
            print(f"{counter}) Field: {field} \n Values: {values}\n")

def main():
    args = process_command_line_arguments()

    if args.input_directory and args.output_attribs_tags:
        data_generator = process_xml_files(args, dataframe=None)
        for _ in data_generator:
            data = uniq_data_to_dict()
            save_to_csv(data, args.output_attribs_tags)

    elif args.input_directory and args.output_directory and args.input_csv:
        dataframe = load_csv_data(args.input_csv)
        data_generator = process_xml_files(args, dataframe)
        paths, errors = get_paths_counts_and_errors(list(data_generator))
        data = {'XMLPath': list(paths.keys()), 'Repeated': list(paths.values()), 'errors': errors}
        save_to_csv(data, args.output_directory)

    elif args.input_clear_csv and args.input_directory and args.output_directory:
        dataframe = load_csv_data(args.input_clear_csv)
        xml_set = xmlSet()
        xml_set.process_xml_files(args, dataframe)
        with open(args.output_directory, 'w') as output_file:
            xml_set.print(output_file)

if __name__ == "__main__":
    main()
