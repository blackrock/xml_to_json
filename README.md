# **XML To JSON Converter**

This repository contains code for the XML to JSON Converter.
This converter is written in Python and will convert one or more XML files into JSON / JSONL files

# Key Features

Converts XML to valid JSON or JSONL 
Requires only two files to get started. Your XML file and the XSD schema file for that XML file.
Multiprocessing enabled to parse XML files concurrently if the XML files are in the same format. Call with -m # option.
Uses Python's iterparse event based methods which enables parsing very large files with low memory requirements. This is very similar to Java's SAX parser
Files are processed in order with the largest files first to optimize overall parsing time
Option to write results to either Linux or HDFS folders

# How to run?
```python
python xml_to_json.py
```

# Parameters
```python
usage: xml_to_json.py [-h] -x XSD_FILE [-o OUTPUT_FORMAT] [-s SERVER]
                      [-t TARGET_PATH] [-z] [-p XPATH] [-a ATTRIBPATH]
                      [-e EXCLUDEPATHS] [-m MULTI] [-l LOG] [-v VERBOSE] [-n]
                      ...

XML To JSON Parser

positional arguments:
  xml_files             xml files to convert

optional arguments:
  -h, --help            show this help message and exit
  -x XSD_FILE, --xsd_file XSD_FILE
                        xsd file name
  -o OUTPUT_FORMAT, --output_format OUTPUT_FORMAT
                        output format json or jsonl. Default is jsonl.
  -s SERVER, --server SERVER
                        server with hadoop client installed if hadoop not
                        installed
  -t TARGET_PATH, --target_path TARGET_PATH
                        target path. hdfs targets require hadoop client
                        installation. Examples: /proj/test, hdfs:///proj/test,
                        hdfs://halfarm/proj/test
  -z, --zip             gzip output file
  -p XPATH, --xpath XPATH
                        xpath to parse out.
  -a ATTRIBPATH, --attribpath ATTRIBPATH
                        extra element attributes to parse out.
  -e EXCLUDEPATHS, --excludepaths EXCLUDEPATHS
                        elements to exclude. pass in comma separated string.
                        /path/exclude1,/path/exclude2
  -m MULTI, --multi MULTI
                        number of parsers. Default is 1.
  -l LOG, --log LOG     log file
  -v VERBOSE, --verbose VERBOSE
                        verbose output level. INFO, DEBUG, etc.
  -n, --no_overwrite    do not overwrite output file if it exists already

```

# Convert a small XML file to a JSON file
```python
python xml_to_json.py -x PurchaseOrder.xsd PurchaseOrder.xml

INFO - 2018-03-20 11:10:24 - Parsing XML Files..
INFO - 2018-03-20 11:10:24 - Processing 1 files
INFO - 2018-03-20 11:10:24 - Parsing files in the following order:
INFO - 2018-03-20 11:10:24 - ['PurchaseOrder.xml']
DEBUG - 2018-03-20 11:10:24 - Generating schema from PurchaseOrder.xsd
DEBUG - 2018-03-20 11:10:24 - Parsing PurchaseOrder.xml
DEBUG - 2018-03-20 11:10:24 - Writing to file PurchaseOrder.json
DEBUG - 2018-03-20 11:10:24 - Completed PurchaseOrder.xml
```
Original XML
```xml
<?xml version="1.0"?>
<purchaseOrder orderDate="1999-10-20">
    <shipTo country="US">
        <name>Alice Smith</name>
        <street>123 Maple Street</street>
        <city>Mill Valley</city>
        <state>CA</state>
        <zip>90952</zip>
    </shipTo>
    <billTo country="US">
        <name>Robert Smith</name>
        <street>8 Oak Avenue</street>
        <city>Old Town</city>
        <state>PA</state>
        <zip>95819</zip>
    </billTo>
    <comment>Hurry, my lawn is going wild!</comment>
    <items>
        <item partNum="872-AA">
            <productName>Lawnmower</productName>
            <quantity>1</quantity>
            <USPrice>148.95</USPrice>
            <comment>Confirm this is electric</comment>
        </item>
        <item partNum="926-AA">
            <productName>Baby Monitor</productName>
            <quantity>1</quantity>
            <USPrice>39.98</USPrice>
            <shipDate>1999-05-21</shipDate>
        </item>
    </items>
</purchaseOrder>
```

JSON output
(zip looks funny, but blame Microsoft which says zip is a decimal in the XSD file spec <xs:element name="zip" type="xs:decimal"/>)
```json
{   
   "purchaseOrderorderDate":"1999-10-20",
   "shipTo":{   
      "shipTocountry":"US",
      "name":"Alice Smith",
      "street":"123 Maple Street",
      "city":"Mill Valley",
      "state":"CA",
      "zip":90952.0
   },
   "billTo":{   
      "billTocountry":"US",
      "name":"Robert Smith",
      "street":"8 Oak Avenue",
      "city":"Old Town",
      "state":"PA",
      "zip":95819.0
   },
   "comment":"Hurry, my lawn is going wild!",
   "items":{   
      "item":[   
         {   
            "itempartNum":"872-AA",
            "productName":"Lawnmower",
            "quantity":1,
            "USPrice":148.95,
            "comment":"Confirm this is electric"
         },
         {   
            "itempartNum":"926-AA",
            "productName":"Baby Monitor",
            "quantity":1,
            "USPrice":39.98,
            "shipDate":"1999-05-21"
         }
      ]
   }
}
```

# Convert an entire directory of XML files to JSONL
Also zip output files, parse 3 files concurrently, only extract /PurchaseOrder/items/item elements and incrementally
process one XML path at a time to save memory instead of trying to read the entire XML file into memory.
```python
cp PurchaseOrder.xml 1.xml
cp 1.xml 2.xml
cp 1.xml 3.xml
cp 1.xml 4.xml

python xml_to_json.py -o jsonl -m 3 -z -p /purchaseOrder/items/item -x PurchaseOrder.xsd *.xml

INFO - 2018-03-20 16:33:50 - Parsing XML Files..
INFO - 2018-03-20 16:33:50 - Processing 5 files
INFO - 2018-03-20 16:33:50 - Parsing files in the following order:
INFO - 2018-03-20 16:33:50 - ['1.xml', '2.xml', 'PurchaseOrder.xml', '4.xml', '3.xml']
DEBUG - 2018-03-20 16:33:50 - Generating schema from PurchaseOrder.xsd
DEBUG - 2018-03-20 16:33:50 - Generating schema from PurchaseOrder.xsd
DEBUG - 2018-03-20 16:33:50 - Generating schema from PurchaseOrder.xsd
DEBUG - 2018-03-20 16:33:50 - Parsing PurchaseOrder.xml
DEBUG - 2018-03-20 16:33:50 - Writing to file PurchaseOrder.jsonl.gz
DEBUG - 2018-03-20 16:33:50 - Parsing 1.xml
DEBUG - 2018-03-20 16:33:50 - Parsing 2.xml
DEBUG - 2018-03-20 16:33:50 - Writing to file 1.jsonl.gz
DEBUG - 2018-03-20 16:33:50 - Writing to file 2.jsonl.gz
DEBUG - 2018-03-20 16:33:51 - Parsing item from 1.xml
DEBUG - 2018-03-20 16:33:51 - Parsing item from 2.xml
DEBUG - 2018-03-20 16:33:51 - Parsing item from PurchaseOrder.xml
DEBUG - 2018-03-20 16:33:51 - Completed 2.xml
DEBUG - 2018-03-20 16:33:51 - Generating schema from PurchaseOrder.xsd
DEBUG - 2018-03-20 16:33:51 - Completed PurchaseOrder.xml
DEBUG - 2018-03-20 16:33:51 - Completed 1.xml
DEBUG - 2018-03-20 16:33:51 - Generating schema from PurchaseOrder.xsd
DEBUG - 2018-03-20 16:33:51 - Parsing 4.xml
DEBUG - 2018-03-20 16:33:51 - Writing to file 4.jsonl.gz
DEBUG - 2018-03-20 16:33:51 - Parsing 3.xml
DEBUG - 2018-03-20 16:33:51 - Writing to file 3.jsonl.gz
DEBUG - 2018-03-20 16:33:51 - Parsing item from 3.xml
DEBUG - 2018-03-20 16:33:51 - Parsing item from 4.xml
DEBUG - 2018-03-20 16:33:51 - Completed 3.xml
DEBUG - 2018-03-20 16:33:51 - Completed 4.xml
```
JSON output
```json
ls -l *.gz
-rw-r--r-- 1 leed users 191 Mar 20 16:26 1.jsonl.gz
-rw-r--r-- 1 leed users 191 Mar 20 16:26 2.jsonl.gz
-rw-r--r-- 1 leed users 191 Mar 20 16:26 3.jsonl.gz
-rw-r--r-- 1 leed users 191 Mar 20 16:26 4.jsonl.gz
-rw-r--r-- 1 leed users 203 Mar 20 16:26 PurchaseOrder.jsonl.gz

zcat *.jsonl.gz

{"itempartNum": "872-AA", "productName": "Lawnmower", "quantity": 1, "USPrice": 148.95, "comment": "Confirm this is electric"}
{"itempartNum": "926-AA", "productName": "Baby Monitor", "quantity": 1, "USPrice": 39.98, "shipDate": "1999-05-21"}

{"itempartNum": "872-AA", "productName": "Lawnmower", "quantity": 1, "USPrice": 148.95, "comment": "Confirm this is electric"}
{"itempartNum": "926-AA", "productName": "Baby Monitor", "quantity": 1, "USPrice": 39.98, "shipDate": "1999-05-21"}

{"itempartNum": "872-AA", "productName": "Lawnmower", "quantity": 1, "USPrice": 148.95, "comment": "Confirm this is electric"}
{"itempartNum": "926-AA", "productName": "Baby Monitor", "quantity": 1, "USPrice": 39.98, "shipDate": "1999-05-21"}

{"itempartNum": "872-AA", "productName": "Lawnmower", "quantity": 1, "USPrice": 148.95, "comment": "Confirm this is electric"}
{"itempartNum": "926-AA", "productName": "Baby Monitor", "quantity": 1, "USPrice": 39.98, "shipDate": "1999-05-21"}

{"itempartNum": "872-AA", "productName": "Lawnmower", "quantity": 1, "USPrice": 148.95, "comment": "Confirm this is electric"}
{"itempartNum": "926-AA", "productName": "Baby Monitor", "quantity": 1, "USPrice": 39.98, "shipDate": "1999-05-21"}
```

