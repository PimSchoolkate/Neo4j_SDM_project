The full original data set is too big to store on github!

the DBLP data is available in XML format (https://dblp.uni-trier.de/).
You can use the following tool to convert DBLP datafrom XML to CSV:
https://github.com/ThomHurks/dblp-to-csv, 
which allows youto generate a CSV file that is Neo4j compatible providing some options, e.g.:
python XMLToCSV.py --annotate --neo4jdata/dblp.xml data/dblp.dtd dblp.csv --relationsauthor:authoredby journal:publishedin