"""
Author: David Lee
"""
import xml.etree.cElementTree as ET
import xmlschema
from collections import OrderedDict
import decimal
import json
import glob
from multiprocessing import Pool
import subprocess
import os
import gzip
import logging

_logger = logging.getLogger("xml_to_json")


def decimal_default(obj):
    """
    :param obj: python data
    :return: a float
    :raises:
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


class ParqConverter(xmlschema.XMLSchemaConverter):
    """
    XML Schema based converter class for Parquet friendly json.
    """

    def __init__(self, namespaces=None, dict_class=None, list_class=None, text_key=None, attr_prefix=None, cdata_prefix=None, **kwargs):
        """
        :param namespaces: Map from namespace prefixes to URI.
        :param dict_class: Dictionary class to use for decoded data. Default is `dict`.
        :param list_class: List class to use for decoded data. Default is `list`.
        :param text_key: is the key to apply to element"s decoded text data.
        :param attr_prefix: controls the mapping of XML attributes, to the same name or \
        with a prefix. If `None` the converter ignores attributes.
        :param cdata_prefix: is used for including and prefixing the CDATA parts of a \
        mixed content, that are labeled with an integer instead of a string. \
        CDATA parts are ignored if this argument is `None`.
        """

        super(ParqConverter, self).__init__(
            namespaces, dict_class or OrderedDict, list_class,
            attr_prefix=None, text_key=None, cdata_prefix=None
        )

    def element_decode(self, data, xsd_element):
        """
        :param data: Decoded ElementData from an Element node.
        :param xsd_element: The `XsdElement` associated to decoded the data.
        :return: A dictionary-based data structure containing the decoded data.
        """

        if data.attributes:
            self.attr_prefix = data.tag
            result_dict = self.dict([(k, v) for k, v in self.map_attributes(data.attributes)])
        else:
            result_dict = self.dict()

        if xsd_element.type.is_simple() or xsd_element.type.has_simple_content():
            result_dict[data.tag] = data.text if data.text is not None and data.text != "" else None

        if data.content:
            for name, value, xsd_child in self.map_content(data.content):
                if value:
                    if xsd_child.is_single():
                        if xsd_child.type.is_simple() or xsd_child.type.has_simple_content():
                            for k in value.keys():
                                result_dict[k] = value[k]
                        else:
                            result_dict[name] = value
                    else:
                        if (xsd_child.type.is_simple() or xsd_child.type.has_simple_content()) and len(xsd_element.findall("*")) == 1:
                            try:
                                result_dict.append(value)
                            except AttributeError:
                                result_dict = self.list([value])
                        else:
                            try:
                                result_dict[name].append(value)
                            except KeyError:
                                result_dict[name] = self.list([value])
                            except AttributeError:
                                result_dict[name] = self.list([value])

        return result_dict


def open_file(zip, filename):
    """
    :param zip: whether to open a new file using gzip
    :param filename: name of new file
    :return: file handlers
    """
    if zip:
        return gzip.open(filename, "wb")
    else:
        return open(filename, "wb")


def parse_file(xml_file, output_format, zip, xsd_file, xpath, no_overwrite, target_path):
    """
    :param xml_file: xml file name
    :param output_format: jsonl or json
    :param zip: zip save file
    :param xsd_file: xsd file name
    :param xpath: whether to parse a specific xml path
    :param no_overwrite: overwrite target file
    :param target_path: directory to save file
    """

    logging.debug("Generating schema from " + xsd_file)

    my_schema = xmlschema.XMLSchema(xsd_file, converter=ParqConverter)

    path, xml_file = os.path.split(os.path.realpath(xml_file))

    if not target_path:
        tgt_path = path
    elif target_path[:5] == "hdfs:":
        tgt_path = path
    else:
        tgt_path = target_path

    logging.debug("Parsing " + xml_file)

    if xml_file[-4:] == ".xml":
        if output_format == "jsonl":
            output_file = xml_file[:-4] + ".jsonl"
        else:
            output_file = xml_file[:-4] + ".json"
    else:
        if output_format == "jsonl":
            output_file = xml_file + ".jsonl"
        else:
            output_file = xml_file + ".json"

    if zip:
        output_file = output_file + ".gz"

    if no_overwrite:
        if target_path and target_path[:5] == "hdfs:":
            if subprocess.call(["hadoop", "fs", "-test", "-e", target_path]) == 0:
                logging.debug("No overwrite. Skipping " + xml_file)
                return
        elif os.path.isfile(tgt_path + "/" + output_file):
            logging.debug("No overwrite. Skipping " + xml_file)
            return

    logging.debug("Writing to file " + output_file)

    context = ET.iterparse(path + "/" + xml_file, events=("start", "end"))

    xpath_list = None

    if xpath:
        xpath_list = xpath.split("/")
        del xpath_list[0]
        # If xpath is just the root then use other routine
        if len(xpath_list) == 1:
            xpath_list = None

    if xpath_list:

        root = ET.XML("<" + "><".join(xpath_list[:-1]) + "></" + "></".join(xpath_list[:-1][::-1]) + ">")
        parent = root
        for i in xpath_list[:-2]:
            parent = parent[0]

        first_record = True
        elem_active = False
        currentxpath = []

        isjsonarray = False
        xsd_elem = my_schema.find(xpath)
        if xsd_elem.max_occurs is None or xsd_elem.max_occurs > 1:
            isjsonarray = True

        with open_file(zip, tgt_path + "/" + output_file) as json_file:

            # 2nd pass open xml file and get elements in the list
            logging.debug("Parsing " + xpath_list[-1] + " from " + xml_file)

            if isjsonarray and output_format != "jsonl":
                json_file.write(bytes("[\n", "utf-8"))

            # Start parsing items out of XML
            for event, elem in context:
                if event == "start":
                    currentxpath.append(elem.tag)
                    if currentxpath == xpath_list:
                        elem_active = True
                if event == "end":
                    if currentxpath == xpath_list:
                        elem_active = False
                        parent.append(elem)
                        my_dict = my_schema.to_dict(root, path=xpath)
                        parent.remove(elem)
                        my_json = json.dumps(my_dict, default=decimal_default)
                        if first_record:
                            first_record = False
                            json_file.write(bytes(my_json, "utf-8"))
                        else:
                            if output_format == "jsonl":
                                json_file.write(bytes("\n" + my_json, "utf-8"))
                            else:
                                json_file.write(bytes(",\n" + my_json, "utf-8"))

                        # my_json = json.dumps(my_dict, default=decimal_default)
                        # logging.info(my_json)
                        # logging.info("\n")
                        # input("Press Enter to continue...")

                    if not elem_active:
                        elem.clear()

                    del currentxpath[-1]

            if isjsonarray and output_format != "jsonl":
                json_file.write(bytes("\n]", "utf-8"))

        if first_record:
            os.remove(tgt_path + "/" + output_file)
            return

    else:
        elem, root = next(context)
        my_dict = my_schema.to_dict(path + "/" + xml_file)
        my_json = '{"' + root.tag + '": ' + json.dumps(my_dict, default=decimal_default) + "}"
        with open_file(zip, tgt_path + "/" + output_file) as json_file:
            json_file.write(bytes(my_json, "utf-8"))

    del context

    if target_path and target_path[:5] == "hdfs:":
        subprocess.call(["hadoop", "fs", "-mkdir", "-p", target_path])
        subprocess.call(["hadoop", "fs", "-rm", "-f", target_path + "/" + output_file])
        subprocess.call(["hadoop", "fs", "-put", tgt_path + "/" + output_file, target_path])
        os.remove(tgt_path + "/" + output_file)

    logging.debug("Completed " + xml_file)


def convert_xml_to_json(args):
    """
    :param args: args passed in from command line
    """

    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(logging.getLevelName(args['verbose']))
    _logger.addHandler(handler)

    if args['log']:
        # create log file handler and set level to debug
        handler = logging.FileHandler(args['log'])
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        _logger.addHandler(handler)

    logging.info("Parsing XML Files..")

    if args['target_path']:
        if args['target_path'][:1] == "/" and not os.path.exists(args['target_path']):
            os.makedirs(args['target_path'])
        elif args['target_path'][:5] == "hdfs:":
            if subprocess.call(["hadoop", "fs", "-test", "-e", args['target_path']]) != 0:
                logging.error("--target_path not valid or hadoop client not installed on server")
                return
        else:
            logging.error("invalid --target_path specified")
            return

    # open target files
    file_list = list(set([f for _files in [glob.glob(args['args'][x]) for x in range(0, len(args['args']))] for f in _files]))
    file_count = len(file_list)

    parse_queue_pool = Pool(processes=int(args['multi']))

    logging.info("Processing " + str(file_count) + " files")

    if len(file_list) <= 1000:
        file_list.sort(key=os.path.getsize, reverse=True)
        logging.info("Parsing files in the following order:")
        logging.info(file_list)

    for filename in file_list:
        parse_queue_pool.apply_async(parse_file, args=(filename, args['output_format'], args['zip'], args['xsd_file'], args['xpath'], args['no_overwrite'], args['target_path']), error_callback=logging.info)
        # parse_file(filename, args["output_format"], args["zip"], args["xsd_file"], args["xpath"], args["no_overwrite"], args['target_path'])

    parse_queue_pool.close()
    parse_queue_pool.join()
