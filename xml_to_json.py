from __future__ import with_statement
import xml.etree.cElementTree as ET
import xmlschema
from collections import OrderedDict
import decimal
import argparse
from datetime import datetime
import json
import glob
from multiprocessing import Manager, Pool
import os
import gzip
from time import sleep
import requests
import logging

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


class ParqConverter(xmlschema.XMLSchemaConverter):
    """
    XML Schema based converter class for Parquet friendly json.
    :param namespaces: Map from namespace prefixes to URI.
    :param dict_class: Dictionary class to use for decoded data. Default is `OrderedDict`.
    :param list_class: List class to use for decoded data. Default is `list`.
    """

    def __init__(self, namespaces=None, dict_class=None, list_class=None):
        super(ParqConverter, self).__init__(
            namespaces, dict_class or OrderedDict, list_class,
            attr_prefix=None, text_key=None, cdata_prefix=None
        )

    def map_attributes(self, tag, attributes):
        """
        Creates an iterator for converting decoded attributes to a data structure with
        appropriate prefixes. If the instance has a not-empty map of namespaces registers
        the mapped URIs and prefixes.
        :param attributes: A sequence or an iterator of couples with the name of \
        the attribute and the decoded value. Default is `None` (for `simpleType` \
        elements, that don't have attributes).
        """
        if not attributes:
            return
        else:
            for name, value in attributes:
                yield u'%s%s' % (tag, self.map_qname(name)), value

    def element_decode(self, data, xsd_element):

        if hasattr(xsd_element.type, 'attributes'):
            result_dict = self.dict([(k, v) for k, v in self.map_attributes(data.tag, data.attributes)])
        else:
            result_dict = self.dict()

        if xsd_element.type.is_simple() or xsd_element.type.has_simple_content():
            result_dict[data.tag] = data.text if data.text is not None and data.text != '' else None

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

# starts a process to write to a file.
# listens to a queue to get data to write to a file.
# if we have 10 files to write to then there will be 10 of these processes running at the same time listening on 10 queues.

def write_parquet(queue, url, target_path, file_count):
    headers = {'Content-type': 'application/json'}

    if target_path and target_path[:5] == 'hdfs:':
        tmp = 'hdfs.tmp.'
        os.system('hadoop fs -mkdir -p ' + target_path)
    else:
        tmp = 'dfs.tmp.'

    while True:
        output_record = queue.get()

        if output_record == '!!!STOP!!!':
            del output_record
            file_count -= 1
            if file_count == 0:
                return
            continue

        path = output_record[0]
        output_file = output_record[1]
        del output_record

        logging.debug('Converting ' + output_file + ' to parquet')

        pq_dir = output_file[0:-8]

        if not target_path:
            tgt_path = path
        else:
            tgt_path = target_path

        if tmp == 'dfs.tmp.':
            if os.path.exists('/tmp/' + pq_dir):
                os.system('rm -r /tmp/' + pq_dir)
            if os.path.exists(tgt_path + '/' + pq_dir):
                os.system('rm -r ' + tgt_path + '/' + pq_dir)
        else:
            os.system('hadoop fs -rm -f -r /tmp/' + pq_dir)
            os.system('hadoop fs -rm -f -r ' + tgt_path + '/' + pq_dir)

        drill_query = "create table " + tmp + "`/" + pq_dir + "` as select a.* from dfs.`" + path + "/" + output_file + "` a"
        drill_query = json.dumps({"queryType": "SQL", "query": drill_query})

        r = requests.post(url, data=drill_query, headers=headers)
        if "errorMessage" in r.json():
            logging.error(str(r.json()))
            logging.error(str(drill_query))
        else:
            sleep(5)

            if tmp == 'dfs.tmp.':
                os.system('mv /tmp/' + pq_dir + ' ' + tgt_path)
            else:
                os.system('hadoop fs -mv /tmp/' + pq_dir + ' ' + tgt_path)

            os.remove(path + '/' + output_file)

            logging.debug('Converted ' + output_file + ' to parquet')

        file_count -= 1
        if file_count == 0:
            return

def open_file(zip, filename):
    if zip:
        return gzip.open(filename, 'wb')
    else:
        return open(filename, 'wb')

# starts a process to parse to a file.
def parse_file(xml_file, output_format, zip, xsd_file, xpath, no_overwrite, drill_queue, target_path):

    logging.debug('Generating schema from ' + xsd_file)

    my_schema = xmlschema.XMLSchema(xsd_file, converter=ParqConverter)

    path, xml_file = os.path.split(os.path.realpath(xml_file))

    if not target_path:
        tgt_path = path
    elif target_path[:5] == 'hdfs:':
        tgt_path = path
    else:
        tgt_path = target_path

    logging.debug('Parsing ' + xml_file)

    if xml_file[-4:] == '.xml':
        if output_format == 'jsonl':
            output_file = xml_file[:-4] + '.jsonl'
        else:
            output_file = xml_file[:-4] + '.json'
    else:
        if output_format == 'jsonl':
            output_file = xml_file + '.jsonl'
        else:
            output_file = xml_file + '.json'

    if output_format == 'parquet':
        zip = True

    if zip:
        output_file = output_file + '.gz'

    if no_overwrite:
        if target_path and target_path[:5] == 'hdfs:':
            if os.system('hadoop fs -ls ' + target_path) == 0:
                logging.debug('No overwrite. Skipping ' + xml_file)
                return
        elif os.path.isfile(tgt_path + '/' + output_file):
            logging.debug('No overwrite. Skipping ' + xml_file)
            return

    logging.debug('Writing to file ' + output_file)

    context = ET.iterparse(path + '/' + xml_file, events=('start', 'end'))

    xpath_list = None

    if xpath:
        xpath_list = xpath.split("/")
        del xpath_list[0]
        # If xpath is just the root then use other routine
        if len(xpath_list) == 1:
            xpath_list = None

    if xpath_list:

        root = ET.XML('<' + '><'.join(xpath_list[:-1]) + '></' + '></'.join(xpath_list[:-1][::-1]) + '>')
        parent = root
        for i in xpath_list[:-2]:
          parent = parent[0]

        first_record = True
        elem_active = False
        CurrentXPath = []

        isJsonArray = False
        xsd_elem = my_schema.find(xpath)
        if xsd_elem.max_occurs is None or xsd_elem.max_occurs > 1:
            isJsonArray = True

        with open_file(zip, tgt_path + '/' + output_file) as json_file:

            # 2nd pass open xml file and get elements in the list
            logging.debug('Parsing ' + xpath_list[-1] + ' from ' + xml_file)

            if isJsonArray and output_format != 'jsonl':
                json_file.write(bytes('[\n', "utf-8"))

            # Start parsing items out of XML
            for event, elem in context:
                if event == 'start':
                    CurrentXPath.append(elem.tag)
                    if CurrentXPath == xpath_list:
                        elem_active = True
                if event == 'end':
                    if CurrentXPath == xpath_list:
                        elem_active = False
                        parent.append(elem)
                        my_dict = my_schema.to_dict(root, path=xpath)
                        parent.remove(elem)
                        my_json = json.dumps(my_dict, default=decimal_default)
                        if first_record:
                            first_record = False
                            json_file.write(bytes(my_json, "utf-8"))
                        else:
                            if output_format == 'jsonl':
                                json_file.write(bytes('\n' + my_json, "utf-8"))
                            else:
                                json_file.write(bytes(',\n' + my_json, "utf-8"))


                        # my_json = json.dumps(my_dict, default=decimal_default)
                        # logging.info(my_json)
                        # logging.info('\n')
                        # input("Press Enter to continue...")

                    if not elem_active:
                        elem.clear()

                    del CurrentXPath[-1]

            if isJsonArray and output_format != 'jsonl':
                json_file.write(bytes('\n]', "utf-8"))

        if first_record:
            os.remove(tgt_path + '/' + output_file)
            if drill_queue:
                drill_queue.put('!!!STOP!!!')
            return

    else:
        elem, root = next(context)
        my_dict = my_schema.to_dict(path + '/' + xml_file)
        my_json = '{"' + root.tag + '": ' + json.dumps(my_dict, default=decimal_default) + '}'
        with open_file(zip, tgt_path + '/' + output_file) as json_file:
            json_file.write(bytes(my_json, "utf-8"))

    del context

    if output_format == 'parquet' and drill_queue:
        drill_queue.put([tgt_path, output_file])
    elif target_path and target_path[:5] == 'hdfs:':
        os.system('hadoop fs -mkdir -p ' + target_path)
        os.system('hadoop fs -rm -f ' + target_path + '/' + output_file)
        os.system('hadoop fs -put ' + tgt_path + '/' + output_file + ' ' + target_path)
        os.remove(tgt_path + '/' + output_file)

    logging.debug('Completed ' + xml_file)

def main(args):

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # create console handler and set level to info
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler.setLevel(logging.getLevelName(args['verbose']))
    logger.addHandler(handler)

    if args['log']:
        # create log file handler and set level to debug
        handler = logging.FileHandler(args['log'])
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)

    logging.info("Parsing XML Files..")

    if args['target_path']:
        if args['target_path'][:1] == '/' and not os.path.exists(args['target_path']):
            os.makedirs(args['target_path'])
        elif args['target_path'][:5] == 'hdfs:':
            hdfs_server = args['target_path'].split('/')
            hdfs_server = hdfs_server[0] + '/' +  hdfs_server[1] + '/' +  hdfs_server[2] + '/'
            if os.system('hadoop fs -ls ' + hdfs_server) > 0:
                logging.error('--target_path not valid or hadoop client not installed on server')
                return
        else:
            logging.error('invalid --target_path specified')
            return

    # open target files
    manager = Manager()

    file_list = list(set([f for _files in [glob.glob(args['args'][x]) for x in range(0, len(args['args']))] for f in _files]))
    file_count = len(file_list)

    parse_queue_pool = Pool(processes=int(args['multi']))

    logging.info('Processing ' + str(file_count) + ' files')

    if len(file_list) <= 100:
        file_list.sort(key=os.path.getsize, reverse=True)
        logging.info('Parsing files in the following order:')
        logging.info(file_list)

    # Can only convert one json file at time to avoid JVM too many files open limits
    drill_queue = None
    if args['output_format'] == 'parquet':
        if not args['drill_service']:
            drill_config = json.loads(open(os.path.dirname(os.path.realpath(__file__)) + "/xml_to_json.conf").read())["drill_service"]

            args['drill_service'] = drill_config['host'] + ':' + str(drill_config['port'])

        url = 'http://' + args['drill_service'] + '/query.json'

        drill_queue_pool = Pool(processes=1)
        drill_queue = manager.Queue()
        drill_queue_pool.apply_async(write_parquet, args=(drill_queue, url, args['target_path'], file_count), error_callback=logging.info)

    for filename in file_list:
        parse_queue_pool.apply_async(parse_file, args=(filename, args['output_format'], args['zip'], args['xsd_file'], args['xpath'], args['no_overwrite'], drill_queue, args['target_path']), error_callback=logging.info)
        #parse_file, args=(filename, args['output_format'], args['zip'], args['xsd_file'], args['xpath'], args['no_overwrite'], drill_queue, args['target_path'])

    parse_queue_pool.close()
    parse_queue_pool.join()

    if drill_queue:
        drill_queue_pool.close()
        drill_queue_pool.join()

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='XML To JSON Parser')
    parser.add_argument('-x', '--xsd_file', required=True, help='xsd file name')
    parser.add_argument('-o', '--output_format', default='json', help='output format json, jsonl or parquet (experimental). Default is json.')
    parser.add_argument('-t', '--target_path', help='target path. hdfs targets require hadoop client installation. Examples: /proj/test, hdfs:///tmp/test, hdfs://halfarm/tmp/test')
    parser.add_argument('-z', '--zip', action='store_true', help='gzip output file')
    parser.add_argument('-p', '--xpath', help='xpath to parse out.')
    parser.add_argument('-m', '--multi', default='1', help='number of parsers. Default is 1.')
    parser.add_argument('-l', '--log', help='log file')
    parser.add_argument('-v', '--verbose', default='DEBUG', help='verbose output level. INFO, DEBUG, etc.')

    # added this so I can run this parser on multiple boxes to generate JSON and then use this option to convert JSON to Parquet later on the box with Drill installed
    parser.add_argument('-n', '--no_overwrite', action='store_true', help='do not overwrite output file if it exists already')
    parser.add_argument('-d', '--drill_service', help='apache drill service use to create parquet files. Example: halfarm:8047')
    parser.add_argument('args', nargs=argparse.REMAINDER, help='xml files to convert')

    args = vars(parser.parse_args())

    main(args)
