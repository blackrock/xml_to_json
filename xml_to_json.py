"""
Author: David Lee
"""
import argparse

from xml_to_json.convert_xml_to_json import convert_xml_to_json

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="XML To JSON Parser")
    parser.add_argument("-x", "--xsd_file", required=True, help="xsd file name")
    parser.add_argument("-o", "--output_format", default="jsonl", help="output format json or jsonl. Default is jsonl.")
    parser.add_argument("-t", "--target_path", help="target path. hdfs targets require hadoop client installation. Examples: /proj/test, hdfs:///proj/test, hdfs://halfarm/proj/test")
    parser.add_argument("-z", "--zip", action="store_true", help="gzip output file")
    parser.add_argument("-p", "--xpath", help="xpath to parse out.")
    parser.add_argument("-m", "--multi", default="1", help="number of parsers. Default is 1.")
    parser.add_argument("-l", "--log", help="log file")
    parser.add_argument("-v", "--verbose", default="DEBUG", help="verbose output level. INFO, DEBUG, etc.")

    # added this so I can run this parser on multiple boxes to generate JSON and then use this option to convert JSON to Parquet later on the box with Drill installed
    parser.add_argument("-n", "--no_overwrite", action="store_true", help="do not overwrite output file if it exists already")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="xml files to convert")

    args = vars(parser.parse_args())

    convert_xml_to_json(args)
