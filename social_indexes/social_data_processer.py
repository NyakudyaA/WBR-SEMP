import os
import pandas as pd
from optparse import OptionParser

parser = OptionParser()
parser.add_option(
    "-r", "--directory", dest="directory", help="directory with shapefiles",
    metavar="DIRECTORY")
(options, args) = parser.parse_args()

root_directory = options.directory
target = r'%s' % root_directory
# Download stata data from https://www.datafirst.uct.ac.za/dataportal/index.php
for stata_data in os.listdir(target):
    if stata_data.endswith('.dta'):
        file_name = os.path.splitext(stata_data)[0]
        csv_output = os.path.join(target, file_name + '.csv')
        stata_lyr = os.path.join(target, stata_data)
        data = pd.io.stata.read_stata(stata_lyr)
        data.to_csv(csv_output)
