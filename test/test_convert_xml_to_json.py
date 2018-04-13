import unittest
import json
import os
import tempfile

from xml_to_json.convert_xml_to_json import parse_file


class MyTest(unittest.TestCase):

    def test_json(self):

        realpath = os.path.dirname(os.path.realpath(__file__))

        xml_file = os.path.join(realpath, "PurchaseOrder.xml")
        output_file = os.path.join(tempfile.gettempdir(), "PurchaseOrder.json")
        xsd_file = os.path.join(realpath, "PurchaseOrder.xsd")
        output_format = "json"
        zip = False
        xpath = None

        parse_file(xml_file, output_file, xsd_file, output_format, zip, xpath)
        with open(os.path.join(realpath,"PurchaseOrder.json")) as f:
            test_json = json.loads(f.read())
        with open(output_file) as f:
            target_json = json.loads(f.read())
        os.remove(output_file)
        print("Original")
        print("=================================")
        print(test_json)
        print("Test")
        print("=================================")
        print(target_json)
        self.assertEqual(target_json, test_json)

    def test_jsonl(self):

        realpath = os.path.dirname(os.path.realpath(__file__))

        xml_file = os.path.join(realpath, "PurchaseOrder.xml")
        output_file = os.path.join(tempfile.gettempdir(), "PurchaseOrder.jsonl")
        xsd_file = os.path.join(realpath, "PurchaseOrder.xsd")
        output_format = "jsonl"
        zip = False
        xpath = "/purchaseOrder/items/item"

        test_json = list()
        target_json = list()

        parse_file(xml_file, output_file, xsd_file, output_format, zip, xpath)
        with open(os.path.join(realpath, "PurchaseOrder.jsonl")) as f:
            for line in f:
                test_json.append(json.loads(line))
        with open(output_file) as f:
            for line in f:
                target_json.append(json.loads(line))
        os.remove(output_file)
        print("Original")
        print("=================================")
        print(test_json)
        print("Test")
        print("=================================")
        print(target_json)
        self.assertEqual(target_json, test_json)

if __name__ == '__main__':
    unittest.main()
