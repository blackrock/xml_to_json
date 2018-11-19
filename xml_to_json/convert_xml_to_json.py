"""
(c) 2018 David Lee

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
import shutil
import sys
from zipfile import ZipFile

from xmlschema.exceptions import XMLSchemaValueError
from xmlschema.compat import ordered_dict_class

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


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

    def __init__(self, namespaces=None, dict_class=None, list_class=None, **kwargs):
        """
        :param namespaces: map from namespace prefixes to URI.
        :param dict_class: dictionary class to use for decoded data. Default is `dict`.
        :param list_class: list class to use for decoded data. Default is `list`.
        """
        kwargs.update(attr_prefix='', text_key=None, cdata_prefix=None)
        super(ParqConverter, self).__init__(
            namespaces, dict_class or ordered_dict_class, list_class, **kwargs
        )

    def __setattr__(self, name, value):
        """
        :param name: attribute name.
        :param value: attribute value.
        :raises XMLSchemaValueError: Schema validation error for this converter
        """
        if name in ('text_key', 'cdata_prefix') and value is not None:
            raise XMLSchemaValueError('Wrong value %r for the attribute %r of a %r.' % (value, name, type(self)))
        super(xmlschema.XMLSchemaConverter, self).__setattr__(name, value)

    @property
    def lossless(self):
        """
        :return: Returns back lossless property for this converter
        """
        return False

    def element_decode(self, data, xsd_element, level=0):
        """
        :param data: Decoded ElementData from an Element node.
        :param xsd_element: The `XsdElement` associated to decoded the data.
        :param level: 0 for root
        :return: A dictionary-based data structure containing the decoded data.
        """
        map_qname = self.map_qname

        if data.attributes:
            self.attr_prefix = map_qname(data.tag)
            result_dict = self.dict([(k, v) for k, v in self.map_attributes(data.attributes)])
        else:
            result_dict = self.dict()

        if xsd_element.type.is_simple() or xsd_element.type.has_simple_content():
            result_dict[map_qname(data.tag)] = data.text if data.text is not None and data.text != "" else None

        if data.content:
            for name, value, xsd_child in self.map_content(data.content):
                if value:
                    if xsd_child.is_single():
                        if xsd_child.type.is_simple() or xsd_child.type.has_simple_content():
                            for k in value:
                                result_dict[k] = value[k]
                        else:
                            result_dict[name] = value
                    else:
                        if (xsd_child.type.is_simple() or xsd_child.type.has_simple_content()) and not data.attributes and len(xsd_element.findall("*")) == 1:
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


def parse_xml(xml_file, json_file, my_schema, output_format, xpath, xpath_list, attribpaths_list, attribpaths_dict, excludepaths_list, excludeparents_list, root, parent, elem_active, processed):
    """
    :param xml_file: xml file
    :param json_file: json file
    :param my_schema: xmlschema object
    :param output_format: jsonl or json
    :param xpath: whether to parse a specific xml path
    :param xpath_list: xpath in array format
    :param attribpaths_list: paths to capture attributes when used with xpath
    :param attribpaths_dict: captured parent root elements for attributes
    :param excludepaths_list: paths to exclude
    :param excludeparents_list: parent paths of excludes
    :param root: root path
    :param parent: parent path
    :param elem_active: keep or clear elem
    :param processed: data found and processed previously
    :return: data found and processed
    """

    excludeparent = None
    currentxpath = []

    context = ET.iterparse(xml_file, events=("start", "end"))
    # Start parsing items out of XML
    for event, elem in context:
        if event == "start":
            currentxpath.append(elem.tag.split('}', 1)[-1])
            if currentxpath == xpath_list:
                elem_active = True
            if currentxpath in attribpaths_list:
                i = attribpaths_list.index(currentxpath)
                new_elem = ET.Element(elem.tag, elem.attrib)
                attribpaths_dict[i]['parent'].append(new_elem)
                attribpaths_dict[i]['attributes'] = my_schema.to_dict(attribpaths_dict[i]['root'], namespaces=my_schema.namespaces, process_namespaces=True, path=attribpaths_dict[i]['path'], validation='skip')
                attribpaths_dict[i]['parent'].remove(new_elem)
            if currentxpath in excludeparents_list:
                excludeparent = elem

        if event == "end":
            if currentxpath == xpath_list:
                elem_active = False

                parent.append(elem)
                try:
                    if len(attribpaths_dict) > 0:
                        attrib_dict = dict()
                        for i in range(len(attribpaths_dict)):
                            for k, v in attribpaths_dict[i]['attributes'].items():
                                attrib_dict[k] = v
                        elem_dict = my_schema.to_dict(root, namespaces=my_schema.namespaces, process_namespaces=True, path=xpath)
                        my_dict = {**attrib_dict, **elem_dict}
                    else:
                        my_dict = my_schema.to_dict(root, namespaces=my_schema.namespaces, process_namespaces=True, path=xpath)

                    my_json = json.dumps(my_dict, default=decimal_default)

                    if not processed:
                        processed = True
                        json_file.write(bytes(my_json, "utf-8"))
                    else:
                        if output_format == "json":
                            json_file.write(bytes(",\n" + my_json, "utf-8"))
                        else:
                            json_file.write(bytes("\n" + my_json, "utf-8"))
                except Exception as ex:
                    _logger.debug(ex)
                    pass
                parent.remove(elem)

            if not elem_active:
                elem.clear()

            if currentxpath in attribpaths_list:
                i = attribpaths_list.index(currentxpath)
                if attribpaths_dict[i]['inline']:
                    attribpaths_dict[i]['attributes'] = {}

            if currentxpath in excludepaths_list:
                excludeparent.remove(elem)

            del currentxpath[-1]

    if not xpath:
        my_dict = my_schema.to_dict(elem, namespaces=my_schema.namespaces, process_namespaces=True)
        try:
            my_json = '{"' + elem.tag.split('}', 1)[-1] + '": ' + json.dumps(my_dict, default=decimal_default) + "}"
        except Exception as ex:
            _logger.debug(ex)
            pass
        if len(my_json) > 0:
            if not processed:
                processed = True
                json_file.write(bytes(my_json, "utf-8"))
            else:
                if output_format == "json":
                    json_file.write(bytes(",\n" + my_json, "utf-8"))
                else:
                    json_file.write(bytes("\n" + my_json, "utf-8"))

    del context

    return processed


def parse_file(input_file, output_file, xsd_file, output_format, zip, xpath, attribpaths, excludepaths):
    """
    :param input_file: input file
    :param output_file: output file
    :param xsd_file: xsd file
    :param output_format: jsonl or json
    :param zip: zip save file
    :param xpath: whether to parse a specific xml path
    :param attribpaths: paths to capture attributes when used with xpath
    :param excludepaths: paths to exclude
    """

    _logger.debug("Generating schema from " + xsd_file)

    my_schema = xmlschema.XMLSchema(xsd_file, converter=ParqConverter)

    _logger.debug("Parsing " + input_file)

    _logger.debug("Writing to file " + output_file)

    xpath_list = None
    attribpaths_list = []
    attribpaths_dict = {}
    excludepaths_list = []
    excludeparents_list = []

    root = None
    parent = None

    isjsonarray = False

    if excludepaths:
        excludepaths = excludepaths.split(",")
        excludepaths_list = [v.split("/")[1:] for v in excludepaths]
        excludeparents_list = [v[:-1] for v in excludepaths_list]

    if xpath:
        xpath_list = xpath.split("/")[1:]

        root_elem = "<" + "><".join(xpath_list[:-1]) + "></" + "></".join(xpath_list[:-1][::-1]) + ">"

        if my_schema.namespaces[''] != '':
            root_elem = root_elem[:len(xpath_list[0]) + 1] + ' xmlns="' + my_schema.namespaces[''] + '"' + root_elem[len(xpath_list[0]) + 1:]

        root = ET.XML(root_elem)
        parent = root
        for k in xpath_list[:-2]:
            parent = parent[0]

        if attribpaths:
            attribpaths = attribpaths.split(",")
            attribpaths_list = [v.split("/")[1:] for v in attribpaths]
            for i in range(len(attribpaths_list)):
                root_elem = "<" + "><".join(attribpaths_list[i][:-1]) + "></" + "></".join(attribpaths_list[i][:-1][::-1]) + ">"
                if my_schema.namespaces[''] != '':
                    root_elem = root_elem[:len(attribpaths_list[i][0]) + 1] + ' xmlns="' + my_schema.namespaces[''] + '"' + root_elem[len(attribpaths_list[i][0]) + 1:]
                attribroot = ET.XML(root_elem)
                attribparent = attribroot
                for k in attribpaths_list[i][:-2]:
                    attribparent = attribparent[0]
                attribpaths_dict[i] = {'root': attribroot, 'parent': attribparent, 'path': attribpaths[i], 'inline': False, 'attributes': {}}
                if xpath_list == attribpaths_list[i][:len(xpath_list)]:
                    attribpaths_dict[i]['inline'] = True

        xsd_elem = my_schema.find(xpath, namespaces=my_schema.namespaces)
        if xsd_elem.occurs[1] is None or xsd_elem.occurs[1] > 1:
            isjsonarray = True
        elem_active = False
    else:
        elem_active = True

    if input_file.endswith(".zip"):
        isjsonarray = True

    processed = False

    with open_file(zip, output_file) as json_file:

        if isjsonarray and output_format == "json":
            json_file.write(bytes("[\n", "utf-8"))

        if input_file.endswith(".zip"):
            zip_file = ZipFile(input_file, 'r')
            zip_file_list = zip_file.infolist()
            for i in range(len(zip_file_list)):
                xml_file = zip_file.open(zip_file_list[i].filename)
                processed = parse_xml(xml_file, json_file, my_schema, output_format, xpath, xpath_list, attribpaths_list, attribpaths_dict, excludepaths_list, excludeparents_list, root, parent, elem_active, processed)
        else:
            processed = parse_xml(input_file, json_file, my_schema, output_format, xpath, xpath_list, attribpaths_list, attribpaths_dict, excludepaths_list, excludeparents_list, root, parent, elem_active, processed)

        if isjsonarray and output_format == "json":
            json_file.write(bytes("\n]", "utf-8"))

    # Remove file if no json is generated
    if not processed:
        os.remove(output_file)
        _logger.debug("No data found in " + input_file)
        return

    _logger.debug("Completed " + input_file)


def convert_xml_to_json(xsd_file=None, output_format="jsonl", server=None, target_path=None, zip=False, xpath=None, attribpaths=None, excludepaths=None, multi=1, no_overwrite=False, verbose="DEBUG", log=None, xml_files=None):
    """
    :param xsd_file: xsd file name
    :param output_format: jsonl or json
    :param server: optional server with hadoop client installed if current server does not have hadoop installed
    :param target_path: directory to save file
    :param zip: zip save file
    :param xpath: whether to parse a specific xml path
    :param attribpaths: path to capture attributes when used with xpath
    :param excludepaths: paths to exclude
    :param multi: how many files to convert concurrently
    :param no_overwrite: overwrite target file
    :param verbose: stdout log messaging level
    :param log: optional log file
    :param xml_files: list of xml_files

    """

    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.getLevelName(verbose))
    _logger.addHandler(ch)

    if log:
        # create log file handler and set level to debug
        fh = logging.FileHandler(log)
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        _logger.addHandler(fh)

    _logger.info("Parsing XML Files..")

    if target_path:
        if target_path.startswith("hdfs:"):
            if server:
                if subprocess.call(["ssh", server, "hadoop fs -test -e " + target_path]) != 0:
                    _logger.error("invalid target_path: " + target_path + " using hadoop server: " + server)
                    sys.exit(1)
            elif shutil.which("hadoop"):
                if subprocess.call(["hadoop", "fs", "-test", "-e", target_path]) != 0:
                    _logger.error("invalid target_path: " + target_path)
                    sys.exit(1)
            else:
                _logger.error("no hadoop client found")
                sys.exit(1)
        else:
            if not os.path.exists(target_path):
                _logger.error("invalid target_path specified")
                sys.exit(1)

    # open target files
    file_list = list(set([f for _files in [glob.glob(xml_files[x]) for x in range(0, len(xml_files))] for f in _files]))
    file_count = len(file_list)

    parse_queue_pool = Pool(processes=multi)

    _logger.info("Processing " + str(file_count) + " files")

    if len(file_list) <= 1000:
        file_list.sort(key=os.path.getsize, reverse=True)
        _logger.info("Parsing files in the following order:")
        _logger.info(file_list)

    for filename in file_list:

        path, xml_file = os.path.split(os.path.realpath(filename))

        if xml_file.endswith(".xml") or xml_file.endswith(".zip"):
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

        if not target_path:
            output_file = os.path.join(path, output_file)
            if no_overwrite and os.path.isfile(output_file):
                _logger.debug("No overwrite. Skipping " + xml_file)
                continue
        elif target_path.startswith("hdfs:"):
            if no_overwrite and subprocess.call(["hadoop", "fs", "-test", "-e", os.path.join(target_path, output_file)]) == 0:
                _logger.debug("No overwrite. Skipping " + xml_file)
                continue
            output_file = os.path.join(path, output_file)
        else:
            output_file = os.path.join(target_path, output_file)
            if no_overwrite and os.path.isfile(output_file):
                _logger.debug("No overwrite. Skipping " + xml_file)
                continue

        parse_queue_pool.apply_async(parse_file, args=(filename, output_file, xsd_file, output_format, zip, xpath, attribpaths, excludepaths), error_callback=_logger.info)

        if target_path and target_path.startswith("hdfs:") and os.path.isfile(output_file):
            _logger.debug("Moving " + output_file + " to " + target_path)
            if server:
                if subprocess.call(["ssh", server, "hadoop fs -put -f " + output_file + " " + target_path]) != 0:
                    _logger.error("invalid target_path specified")
                    sys.exit(1)
            else:
                if subprocess.call(["hadoop", "fs", "-put", "-f", output_file, target_path]) != 0:
                    _logger.error("invalid target_path specified")
                    sys.exit(1)

            os.remove(output_file)

    parse_queue_pool.close()
    parse_queue_pool.join()
