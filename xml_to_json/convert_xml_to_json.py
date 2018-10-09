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
import shutil
import sys

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

        super().__init__(
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
                            for k in value:
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


def parse_file(xml_file, output_file, xsd_file, output_format, zip, xpath):
    """
    :param xml_file: xml file
    :param output_file: output file
    :param xsd_file: xsd file
    :param output_format: jsonl or json
    :param zip: zip save file
    :param xpath: whether to parse a specific xml path
    """

    _logger.debug("Generating schema from " + xsd_file)

    my_schema = xmlschema.XMLSchema(xsd_file, converter=ParqConverter)

    _logger.debug("Parsing " + xml_file)

    _logger.debug("Writing to file " + output_file)

    context = ET.iterparse(xml_file, events=("start", "end"))

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

        with open_file(zip, output_file) as json_file:

            # 2nd pass open xml file and get elements in the list
            _logger.debug("Parsing " + xpath_list[-1] + " from " + xml_file)

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
                        # _logger.info(my_json)
                        # _logger.info("\n")
                        # input("Press Enter to continue...")

                    if not elem_active:
                        elem.clear()

                    del currentxpath[-1]

            if isjsonarray and output_format != "jsonl":
                json_file.write(bytes("\n]", "utf-8"))"""
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
        kwargs.update(attr_prefix='', text_key=None, cdata_prefix=None)
        super(ParqConverter, self).__init__(
            namespaces, dict_class or ordered_dict_class, list_class, **kwargs
        )

    def __setattr__(self, name, value):
        if name in ('text_key', 'cdata_prefix') and value is not None:
            raise XMLSchemaValueError('Wrong value %r for the attribute %r of a %r.' % (value, name, type(self)))
        super(xmlschema.XMLSchemaConverter, self).__setattr__(name, value)

    @property
    def lossless(self):
        return False

    def element_decode(self, data, xsd_element, level=0):
        """
        :param data: Decoded ElementData from an Element node.
        :param xsd_element: The `XsdElement` associated to decoded the data.
        :paran level: 0 for root
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


def parse_file(xml_file, output_file, xsd_file, output_format, zip, xpath):
    """
    :param xml_file: xml file
    :param output_file: output file
    :param xsd_file: xsd file
    :param output_format: jsonl or json
    :param zip: zip save file
    :param xpath: whether to parse a specific xml path
    """

    _logger.debug("Generating schema from " + xsd_file)

    my_schema = xmlschema.XMLSchema(xsd_file, converter=ParqConverter)

    _logger.debug("Parsing " + xml_file)

    _logger.debug("Writing to file " + output_file)

    context = ET.iterparse(xml_file, events=("start", "end"))

    xpath_list = None

    if xpath:
        xpath_list = xpath.split("/")
        del xpath_list[0]
        # If xpath is just the root then use other routine
        if len(xpath_list) == 1:
            xpath_list = None

    if xpath_list:

        first_record = True
        elem_active = False
        currentxpath = []

        isjsonarray = False
        xsd_elem = my_schema.find(xpath, namespaces=my_schema.namespaces)
        if xsd_elem.max_occurs is None or xsd_elem.max_occurs > 1:
            isjsonarray = True

        with open_file(zip, output_file) as json_file:

            # 2nd pass open xml file and get elements in the list
            _logger.debug("Parsing " + xpath_list[-1] + " from " + xml_file)

            if isjsonarray and output_format != "jsonl":
                json_file.write(bytes("[\n", "utf-8"))

            # Start parsing items out of XML
            for event, elem in context:
                if event == "start":
                    currentxpath.append(elem.tag.split('}', 1)[1])
                    if currentxpath == xpath_list[:len(currentxpath)]:
                        elem_active = True
                        if len(currentxpath) == 1:
                            root = elem
                        if len(currentxpath) == len(xpath_list) - 1:
                            parent = elem

                if event == "end":
                    if currentxpath == xpath_list:
                        elem_active = False
                        parent.append(elem)
                        my_dict = my_schema.to_dict(root, namespaces=my_schema.namespaces, process_namespaces=True, path=xpath)
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
                        # _logger.info(my_json)
                        # _logger.info("\n")
                        # input("Press Enter to continue...")

                    if not elem_active:
                        elem.clear()

                    del currentxpath[-1]

            if isjsonarray and output_format != "jsonl":
                json_file.write(bytes("\n]", "utf-8"))

        # Remove file if no json is generated
        if first_record:
            os.remove(output_file)
            return

    else:
        event, elem = next(context)
        my_dict = my_schema.to_dict(xml_file, namespaces=my_schema.namespaces, process_namespaces=True)
        my_json = '{"' + elem.tag.split('}', 1)[1] + '": ' + json.dumps(my_dict, default=decimal_default) + "}"
        with open_file(zip, output_file) as json_file:
            json_file.write(bytes(my_json, "utf-8"))

    del context

    _logger.debug("Completed " + xml_file)


def convert_xml_to_json(xsd_file=None, output_format="jsonl", server=None, target_path=None, zip=False, xpath=None, multi=1, no_overwrite=False, verbose="DEBUG", log=None, xml_files=None):
    """
    :param xsd_file: xsd file name
    :param output_format: jsonl or json
    :param server: optional server with hadoop client installed if current server does not have hadoop installed
    :param target_path: directory to save file
    :param zip: zip save file
    :param xpath: whether to parse a specific xml path
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

        if xml_file.endswith(".xml"):
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

        parse_queue_pool.apply_async(parse_file, args=(filename, output_file, xsd_file, output_format, zip, xpath), error_callback=_logger.info)
        # parse_file(parse_file(xml_file, output_file, xsd_file, output_format, zip, xpath)

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


        # Remove file if no json is generated
        if first_record:
            os.remove(output_file)
            return

    else:
        elem, root = next(context)
        my_dict = my_schema.to_dict(xml_file)
        my_json = '{"' + root.tag + '": ' + json.dumps(my_dict, default=decimal_default) + "}"
        with open_file(zip, output_file) as json_file:
            json_file.write(bytes(my_json, "utf-8"))

    del context

    _logger.debug("Completed " + xml_file)


def convert_xml_to_json(xsd_file=None, output_format="jsonl", server=None, target_path=None, zip=False, xpath=None, multi=1, no_overwrite=False, verbose="DEBUG", log=None, xml_files=None):
    """
    :param xsd_file: xsd file name
    :param output_format: jsonl or json
    :param server: optional server with hadoop client installed if current server does not have hadoop installed
    :param target_path: directory to save file
    :param zip: zip save file
    :param xpath: whether to parse a specific xml path
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

        if xml_file.endswith(".xml"):
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

        parse_queue_pool.apply_async(parse_file, args=(filename, output_file, xsd_file, output_format, zip, xpath), error_callback=_logger.info)
        # parse_file(parse_file(xml_file, output_file, xsd_file, output_format, zip, xpath)

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
