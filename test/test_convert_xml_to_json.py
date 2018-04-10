import unittest
import json
import os

from xml_to_json.convert_xml_to_json import parse_file


class MyTest(unittest.TestCase):

    def test_json(self):

        realpath = os.path.dirname(os.path.realpath(__file__))

        xml_file = os.path.join(realpath, "PurchaseOrder.xml")
        output_format = "json"
        zip = False
        xsd_file = os.path.join(realpath, "PurchaseOrder.xsd")
        xpath = None
        no_overwrite = False
        target_path = "/tmp"

        parse_file(xml_file, output_format, zip, xsd_file, xpath, no_overwrite, target_path)
        with open(os.path.join(realpath,"PurchaseOrder.json")) as f:
            test_json = json.loads(f.read())
        with open("/tmp/PurchaseOrder.json") as f:
            target_json = json.loads(f.read())
        os.remove("/tmp/PurchaseOrder.json")
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
        output_format = "jsonl"
        zip = False
        xsd_file = os.path.join(realpath, "PurchaseOrder.xsd")
        xpath = "/purchaseOrder/items/item"
        no_overwrite = False
        target_path = "/tmp"

        test_json = list()
        target_json = list()

        parse_file(xml_file, output_format, zip, xsd_file, xpath, no_overwrite, target_path)
        with open(os.path.join(realpath,"PurchaseOrder.jsonl")) as f:
            for line in f:
                test_json.append(json.loads(line))
        with open("/tmp/PurchaseOrder.jsonl") as f:
            for line in f:
                target_json.append(json.loads(line))
        os.remove("/tmp/PurchaseOrder.jsonl")
        print("Original")
        print("=================================")
        print(test_json)
        print("Test")
        print("=================================")
        print(target_json)
        self.assertEqual(target_json, test_json)

if __name__ == '__main__':
    unittest.main()