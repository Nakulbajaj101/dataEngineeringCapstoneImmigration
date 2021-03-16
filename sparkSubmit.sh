#/bin/bash
/usr/bin/spark-submit --py-files ./helperFunctions.py,./qualityTests.py,./i94MetadataMappings.py,./etl.py --master yarn ./etl.py --driver-memory 12G
