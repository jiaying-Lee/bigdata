# -*- coding: utf-8 -*-
import re
import sys
import string, unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
from csv import reader
from operator import add

# Read in and setup. DO NOT CHANGE.
sc = SparkContext()
spark = SparkSession.builder.appName("hw3").config("spark.some.config.option", "some-value").getOrCreate()

# Load Data
parking_df = spark.read.format('csv').options(header = 'true', inferschema = 'true').load(sys.argv[1])

##################
##### Task1 ######
##################
# Task 1-a
parking_df.createOrReplaceTempView("df")

# tmp1 = spark.sql("SELECT summons_number, COUNT(*) as counts\
#                     FROM df \
#                     GROUP BY summons_number\
#                     ORDER BY counts DESC")

repeat_sn = spark.sql("SELECT summons_number\
                    FROM df \
                    GROUP BY summons_number\
                    HAVING COUNT(*) >1")

null_sn = spark.sql("SELECT summons_number\
                    FROM df\
                    WHERE summons_number IS NULL")

task1a_result = repeat_sn.count() + null_sn.count()

##################
# Task 1-b
parking_df.createOrReplaceTempView("df")
task1b_result = spark.sql("SELECT plate_type, COUNT(*) AS plate_counts\
                    FROM df \
                    GROUP BY plate_type\
                    ORDER BY plate_counts DESC")



##################
# Task 1-c

parking_df.createOrReplaceTempView("df")
task1c_tmp = spark.sql("SELECT plate_type, COUNT(*) AS plate_counts\
                    FROM df\
                    GROUP BY plate_type\
                    ORDER BY plate_counts DESC")

name = 'plate_type'
#udf = UserDefinedFunction(lambda x: 'NULL' if x== '999' else x)
udf1 = udf(lambda x: 'NULL' if x== '999' else x)
task1c_result = task1c_tmp.select(*[udf1(column).alias(name) if column == name else column for column in task1c_tmp.columns])



##################
#Task 1-d
parking_df.createOrReplaceTempView("df")
task1d_tmp = spark.sql("SELECT violation_county\
                    FROM df\
                    WHERE violation_county IS NULL")
task1d_result1 = task1d_tmp.count()

task1d_tmp2 = spark.sql("SELECT violation_county\
                    FROM df\
                    WHERE violation_county IS NOT NULL")
task1d_result2 = task1d_tmp2.count()

##################

"""
Output Method - Do Not Change. UNCOMMENT the following lines when you have the tasks finished
"""
sc.parallelize([str(task1a_result)]).saveAsTextFile("hw3-task1-a.out")

task1b_result.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("hw3-task1-b.out")

task1c_result.coalesce(1).rdd.map(lambda x: x[0] + ',' + str(x[1])).saveAsTextFile("hw3-task1-c.out")

sc.parallelize([str(task1d_result1)]).saveAsTextFile("hw3-task1-d1.out")
sc.parallelize([str(task1d_result2)]).saveAsTextFile("hw3-task1-d2.out")


###################
##### Task2 #######
###################

# Convert relevant columns to RDD here
head = "summons_number"
def delhead(line):
    if head in line:
        return 1
    return 0

#sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
#line1 = line1.mapPartitions(lambda x: reader(x))
line1 = line1.filter(lambda line: delhead(line) != 1)


plate_id_rdd = line1.map(lambda x: x[-8])
street_name_rdd = line1.map(lambda x:x[-5])

# plate_id_rdd = task1b_result.coalesce(1).rdd.map(lambda x: x[0])
# street_name_rdd = parking_df.rdd.map()
##################
# Task 2-a: Implementing Fingerprint.

def fingerprint(value):
    # remove leading and trailing whitespace
    # change all characters to their lowercase representation
    s1 = value.strip().lower()
    # remove all punctuation and control characters
    s2 = s1.translate(str.maketrans('', '', string.punctuation))
    # normalize extended western characters to their ASCII representation (for example "gödel" → "godel")
    s3 = unicodedata.normalize(u'NFKD', s2).encode('ascii', 'ignore').decode('utf8')
    # split the string into whitespace-separated tokens
    s4 = s3.split()
    # sort the tokens and remove duplicates
    s5 = sorted(list(set(s4)))
    # join the tokens back together
    s6 = ' '.join(s5)
    key = s6
    return (key,value)

#################
#Task 2-b: Implementing N-gram Fingerprint

def ngram_fingerprint(value, n = 1): #choose your n-gram
    #change all characters to their lowercase representation
    s1 = value.strip().lower()
    #remove all punctuation, control characters and whitespace
    s2 = s1.translate(str.maketrans('', '', string.punctuation))
    s3 = s2.replace(" ", "")
    #normalize extended western characters to their ASCII representation
    s4 = unicodedata.normalize(u'NFKD', s3).encode('ascii', 'ignore').decode('utf8')
    #obtain all the string n-grams
    s5 = [s4[i:i+n] for i in range(len(s4)-n+1)]
    #sort the n-grams and remove duplicates
    s6= sorted(list(set(s5)))
    #join the sorted n-grams back together
    s7 = ''.join(s6)
    result = s7
    return (result,value)

##################
#Task 2-c: Apply Fingerprint to the plate_id column. Output all output clusters.

task2c_result =  plate_id_rdd.map(lambda line: fingerprint(str(line))).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y).sortByKey()

###################
#Task 2-d: Apply N-Gram Fingerprint to the plate_id column. Output all output clusters.

task2d_result = plate_id_rdd.map(lambda line: ngram_fingerprint(str(line),n=2)).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y).sortByKey()

###################
#Task 2-e: Apply Fingerprint to the street_name column. Output the first 20 clusters.

t2e_tmp =  street_name_rdd.map(lambda line: fingerprint(str(line))).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y).sortByKey()
# get all the output
t2e_tmp.saveAsTextFile('2e-all.out')
# get the top 20
task2e_result = sc.parallelize(t2e_tmp.take(20))

##################
#Task 2-f: Apply N-Gram Fingerprint to the street_name column. Output the first 20 clusters

# # get all output from n=1 to n=5
# for i in range(1,6):
#     t2f_tmp = street_name_rdd.map(lambda line: ngram_fingerprint(str(line), n=i)).distinct().map(
#         lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).sortByKey().saveAsTextFile("2f-"+str(i)+'.out')


t2f_tmp = street_name_rdd.map(lambda line: ngram_fingerprint(str(line),n=2)).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y).sortByKey()
t2f_tmp.saveAsTextFile('2f-all.out')
# get the top 20
task2f_result = sc.parallelize(t2f_tmp.take(20))

##################
# Task 2-g: Provide your qualitative response in template2.txt.
##################
# Task 2-h: Design and perform transformations to the street_name column here. Output all clusters.

def normalizeStreetSuffixes(inputValue):
    '''
    Use common abbreviations -> USPS standardized abbreviation to replace common street suffixes

    Obtains list from https://www.usps.com/send/official-abbreviations.htm
    '''
    usps_street_abbreviations = {'trpk': 'tpke', 'forges': 'frgs', 'bypas': 'byp', 'mnr': 'mnr', 'viaduct': 'via',
                                 'mnt': 'mt',
                                 'lndng': 'lndg', 'vill': 'vlg', 'aly': 'aly', 'mill': 'ml', 'pts': 'pts',
                                 'centers': 'ctrs', 'row': 'row', 'cnter': 'ctr',
                                 'hrbor': 'hbr', 'tr': 'trl', 'lndg': 'lndg', 'passage': 'psge', 'walks': 'walk',
                                 'frks': 'frks', 'crest': 'crst', 'meadows': 'mdws',
                                 'freewy': 'fwy', 'garden': 'gdn', 'bluffs': 'blfs', 'vlg': 'vlg', 'vly': 'vly',
                                 'fall': 'fall', 'trk': 'trak', 'squares': 'sqs',
                                 'trl': 'trl', 'harbor': 'hbr', 'frry': 'fry', 'div': 'dv', 'straven': 'stra',
                                 'cmp': 'cp', 'grdns': 'gdns', 'villg': 'vlg',
                                 'meadow': 'mdw', 'trails': 'trl', 'streets': 'sts', 'prairie': 'pr', 'hts': 'hts',
                                 'crescent': 'cres', 'pass': 'pass',
                                 'ter': 'ter', 'port': 'prt', 'bluf': 'blf', 'avnue': 'ave', 'lights': 'lgts',
                                 'rpds': 'rpds', 'harbors': 'hbrs',
                                 'mews': 'mews', 'lodg': 'ldg', 'plz': 'plz', 'tracks': 'trak', 'path': 'path',
                                 'pkway': 'pkwy', 'gln': 'gln',
                                 'bot': 'btm', 'drv': 'dr', 'rdg': 'rdg', 'fwy': 'fwy', 'hbr': 'hbr', 'via': 'via',
                                 'divide': 'dv', 'inlt': 'inlt',
                                 'fords': 'frds', 'avenu': 'ave', 'vis': 'vis', 'brk': 'brk', 'rivr': 'riv',
                                 'oval': 'oval', 'gateway': 'gtwy',
                                 'stream': 'strm', 'bayoo': 'byu', 'msn': 'msn', 'knoll': 'knl',
                                 'expressway': 'expy', 'sprng': 'spg',
                                 'flat': 'flt', 'holw': 'holw', 'grden': 'gdn', 'trail': 'trl', 'jctns': 'jcts',
                                 'rdgs': 'rdgs',
                                 'tunnel': 'tunl', 'ml': 'ml', 'fls': 'fls', 'flt': 'flt', 'lks': 'lks', 'mt': 'mt',
                                 'groves': 'grvs',
                                 'vally': 'vly', 'ferry': 'fry', 'parkway': 'pkwy', 'radiel': 'radl',
                                 'strvnue': 'stra', 'fld': 'fld',
                                 'overpass': 'opas', 'plaza': 'plz', 'estate': 'est', 'mntn': 'mtn', 'lock': 'lck',
                                 'orchrd': 'orch',
                                 'strvn': 'stra', 'locks': 'lcks', 'bend': 'bnd', 'kys': 'kys', 'junctions': 'jcts',
                                 'mountin': 'mtn',
                                 'burgs': 'bgs', 'pine': 'pne', 'ldge': 'ldg', 'causway': 'cswy', 'spg': 'spg',
                                 'beach': 'bch', 'ft': 'ft',
                                 'crse': 'crse', 'motorway': 'mtwy', 'bluff': 'blf', 'court': 'ct', 'grov': 'grv',
                                 'sprngs': 'spgs',
                                 'ovl': 'oval', 'villag': 'vlg', 'vdct': 'via', 'neck': 'nck', 'orchard': 'orch',
                                 'light': 'lgt',
                                 'sq': 'sq', 'pkwy': 'pkwy', 'shore': 'shr', 'green': 'grn', 'strm': 'strm',
                                 'islnd': 'is',
                                 'turnpike': 'tpke', 'stra': 'stra', 'mission': 'msn', 'spngs': 'spgs',
                                 'course': 'crse',
                                 'trafficway': 'trfy', 'terrace': 'ter', 'hway': 'hwy', 'avenue': 'ave',
                                 'glen': 'gln',
                                 'boul': 'blvd', 'inlet': 'inlt', 'la': 'ln', 'ln': 'ln', 'frst': 'frst',
                                 'clf': 'clf',
                                 'cres': 'cres', 'brook': 'brk', 'lk': 'lk', 'byp': 'byp', 'shoar': 'shr',
                                 'bypass': 'byp',
                                 'mtin': 'mtn', 'ally': 'aly', 'forest': 'frst', 'junction': 'jct', 'views': 'vws',
                                 'wells': 'wls', 'cen': 'ctr',
                                 'exts': 'exts', 'crt': 'ct', 'corners': 'cors', 'trak': 'trak', 'frway': 'fwy',
                                 'prarie': 'pr', 'crossing': 'xing',
                                 'extn': 'ext', 'cliffs': 'clfs', 'manors': 'mnrs', 'ports': 'prts',
                                 'gatewy': 'gtwy', 'square': 'sq', 'hls': 'hls',
                                 'harb': 'hbr', 'loops': 'loop', 'mdw': 'mdw', 'smt': 'smt', 'rd': 'rd',
                                 'hill': 'hl', 'blf': 'blf',
                                 'highway': 'hwy', 'walk': 'walk', 'clfs': 'clfs', 'brooks': 'brks', 'brnch': 'br',
                                 'aven': 'ave',
                                 'shores': 'shrs', 'iss': 'iss', 'route': 'rte', 'wls': 'wls', 'place': 'pl',
                                 'sumit': 'smt', 'pines': 'pnes',
                                 'trks': 'trak', 'shoal': 'shl', 'strt': 'st', 'frwy': 'fwy', 'heights': 'hts',
                                 'ranches': 'rnch',
                                 'boulevard': 'blvd', 'extnsn': 'ext', 'mdws': 'mdws', 'hollows': 'holw',
                                 'vsta': 'vis', 'plains': 'plns',
                                 'station': 'sta', 'circl': 'cir', 'mntns': 'mtns', 'prts': 'prts', 'shls': 'shls',
                                 'villages': 'vlgs',
                                 'park': 'park', 'nck': 'nck', 'rst': 'rst', 'haven': 'hvn', 'turnpk': 'tpke',
                                 'expy': 'expy', 'sta': 'sta',
                                 'expr': 'expy', 'stn': 'sta', 'expw': 'expy', 'street': 'st', 'str': 'st',
                                 'spurs': 'spur', 'crecent': 'cres',
                                 'rad': 'radl', 'ranch': 'rnch', 'well': 'wl', 'shoals': 'shls', 'alley': 'aly',
                                 'plza': 'plz', 'medows': 'mdws',
                                 'allee': 'aly', 'knls': 'knls', 'ests': 'ests', 'st': 'st', 'anx': 'anx',
                                 'havn': 'hvn', 'paths': 'path', 'bypa': 'byp',
                                 'spgs': 'spgs', 'mills': 'mls', 'parks': 'park', 'byps': 'byp', 'flts': 'flts',
                                 'tunnels': 'tunl', 'club': 'clb', 'sqrs': 'sqs',
                                 'hllw': 'holw', 'manor': 'mnr', 'centre': 'ctr', 'track': 'trak', 'hgts': 'hts',
                                 'rnch': 'rnch', 'crcle': 'cir', 'falls': 'fls',
                                 'landing': 'lndg', 'plaines': 'plns', 'viadct': 'via', 'gdns': 'gdns',
                                 'gtwy': 'gtwy', 'grove': 'grv', 'camp': 'cp', 'tpk': 'tpke',
                                 'drive': 'dr', 'freeway': 'fwy', 'ext': 'ext', 'points': 'pts', 'exp': 'expy',
                                 'ky': 'ky', 'courts': 'cts', 'pky': 'pkwy', 'corner': 'cor',
                                 'crssing': 'xing', 'mnrs': 'mnrs', 'unions': 'uns', 'cyn': 'cyn', 'lodge': 'ldg',
                                 'trfy': 'trfy', 'circle': 'cir', 'bridge': 'brg',
                                 'dl': 'dl', 'dm': 'dm', 'express': 'expy', 'tunls': 'tunl', 'dv': 'dv', 'dr': 'dr',
                                 'shr': 'shr', 'knolls': 'knls', 'greens': 'grns',
                                 'tunel': 'tunl', 'fields': 'flds', 'common': 'cmn', 'orch': 'orch', 'crk': 'crk',
                                 'river': 'riv', 'shl': 'shl', 'view': 'vw',
                                 'crsent': 'cres', 'rnchs': 'rnch', 'crscnt': 'cres', 'arc': 'arc', 'btm': 'btm',
                                 'blvd': 'blvd', 'ways': 'ways', 'radl': 'radl',
                                 'rdge': 'rdg', 'causeway': 'cswy', 'parkwy': 'pkwy', 'juncton': 'jct',
                                 'statn': 'sta', 'gardn': 'gdn', 'mntain': 'mtn',
                                 'crssng': 'xing', 'rapid': 'rpd', 'key': 'ky', 'plns': 'plns', 'wy': 'way',
                                 'cor': 'cor', 'ramp': 'ramp', 'throughway': 'trwy',
                                 'estates': 'ests', 'ck': 'crk', 'loaf': 'lf', 'hvn': 'hvn', 'wall': 'wall',
                                 'hollow': 'holw', 'canyon': 'cyn', 'clb': 'clb',
                                 'cswy': 'cswy', 'village': 'vlg', 'cr': 'crk', 'trce': 'trce', 'cp': 'cp',
                                 'cv': 'cv', 'ct': 'cts', 'pr': 'pr', 'frg': 'frg',
                                 'jction': 'jct', 'pt': 'pt', 'mssn': 'msn', 'frk': 'frk', 'brdge': 'brg',
                                 'cent': 'ctr', 'spur': 'spur', 'frt': 'ft', 'pk': 'park',
                                 'fry': 'fry', 'pl': 'pl', 'lanes': 'ln', 'gtway': 'gtwy', 'prk': 'park',
                                 'vws': 'vws', 'stravenue': 'stra', 'lgt': 'lgt',
                                 'hiway': 'hwy', 'ctr': 'ctr', 'prt': 'prt', 'ville': 'vl', 'plain': 'pln',
                                 'mount': 'mt', 'mls': 'mls', 'loop': 'loop',
                                 'riv': 'riv', 'centr': 'ctr', 'is': 'is', 'prr': 'pr', 'vl': 'vl', 'avn': 'ave',
                                 'vw': 'vw', 'ave': 'ave', 'spng': 'spg',
                                 'hiwy': 'hwy', 'dam': 'dm', 'isle': 'isle', 'crcl': 'cir', 'sqre': 'sq',
                                 'jct': 'jct', 'jctn': 'jct', 'mountain': 'mtn',
                                 'keys': 'kys', 'parkways': 'pkwy', 'drives': 'drs', 'tunl': 'tunl', 'jcts': 'jcts',
                                 'knl': 'knl', 'center': 'ctr',
                                 'driv': 'dr', 'tpke': 'tpke', 'sumitt': 'smt', 'canyn': 'cyn', 'ldg': 'ldg',
                                 'harbr': 'hbr', 'rest': 'rst', 'shoars': 'shrs',
                                 'vist': 'vis', 'gdn': 'gdn', 'islnds': 'iss', 'hills': 'hls', 'cresent': 'cres',
                                 'point': 'pt', 'lake': 'lk', 'vlly': 'vly',
                                 'strav': 'stra', 'crossroad': 'xrd', 'bnd': 'bnd', 'strave': 'stra',
                                 'stravn': 'stra', 'knol': 'knl', 'vlgs': 'vlgs',
                                 'forge': 'frg', 'cntr': 'ctr', 'cape': 'cpe', 'height': 'hts', 'lck': 'lck',
                                 'highwy': 'hwy', 'trnpk': 'tpke', 'rpd': 'rpd',
                                 'boulv': 'blvd', 'circles': 'cirs', 'valleys': 'vlys', 'vst': 'vis',
                                 'creek': 'crk', 'mall': 'mall', 'spring': 'spg',
                                 'brg': 'brg', 'holws': 'holw', 'lf': 'lf', 'est': 'est', 'xing': 'xing',
                                 'trace': 'trce', 'bottom': 'btm',
                                 'streme': 'strm', 'isles': 'isle', 'circ': 'cir', 'forks': 'frks', 'burg': 'bg',
                                 'run': 'run', 'trls': 'trl',
                                 'radial': 'radl', 'lakes': 'lks', 'rue': 'rue', 'vlys': 'vlys', 'br': 'br',
                                 'cors': 'cors', 'pln': 'pln',
                                 'pike': 'pike', 'extension': 'ext', 'island': 'is', 'frd': 'frd', 'lcks': 'lcks',
                                 'terr': 'ter',
                                 'union': 'un', 'extensions': 'exts', 'pkwys': 'pkwy', 'islands': 'iss',
                                 'road': 'rd', 'shrs': 'shrs',
                                 'roads': 'rds', 'glens': 'glns', 'springs': 'spgs', 'missn': 'msn', 'ridge': 'rdg',
                                 'arcade': 'arc',
                                 'bayou': 'byu', 'crsnt': 'cres', 'junctn': 'jct', 'way': 'way', 'valley': 'vly',
                                 'fork': 'frk',
                                 'mountains': 'mtns', 'bottm': 'btm', 'forg': 'frg', 'ht': 'hts', 'ford': 'frd',
                                 'hl': 'hl',
                                 'grdn': 'gdn', 'fort': 'ft', 'traces': 'trce', 'cnyn': 'cyn', 'cir': 'cir',
                                 'un': 'un', 'mtn': 'mtn',
                                 'flats': 'flts', 'anex': 'anx', 'gatway': 'gtwy', 'rapids': 'rpds',
                                 'villiage': 'vlg', 'flds': 'flds',
                                 'coves': 'cvs', 'rvr': 'riv', 'av': 'ave', 'pikes': 'pike', 'grv': 'grv',
                                 'vista': 'vis', 'pnes': 'pnes',
                                 'forests': 'frst', 'field': 'fld', 'branch': 'br', 'grn': 'grn', 'dale': 'dl',
                                 'rds': 'rds', 'annex': 'anx',
                                 'sqr': 'sq', 'cove': 'cv', 'squ': 'sq', 'skyway': 'skwy', 'ridges': 'rdgs',
                                 'hwy': 'hwy', 'tunnl': 'tunl',
                                 'underpass': 'upas', 'cliff': 'clf', 'lane': 'ln', 'land': 'land', 'bch': 'bch',
                                 'dvd': 'dv', 'curve': 'curv',
                                 'cpe': 'cpe', 'summit': 'smt', 'gardens': 'gdns'}
    orient_abbreviations = {'west': 'w', 'east': 'e', 'north': 'n', 'south':'s'}
    words = inputValue.split()
    for w in words:
        # change all the street special noun to the abbreviation. eg. change street to st
        if w in usps_street_abbreviations.keys():
            inputValue = inputValue.replace(w, usps_street_abbreviations[w])
        # change all cardinal direction to abbreviation. eg. change west to w
        if w in orient_abbreviations.keys():
            inputValue = inputValue.replace(w, orient_abbreviations[w])
        # change the ordinal number to cardinal number. eg. change 100th to 100
        if re.search(r'[0-9]+th', w, re.I):
            # delte the "th"
            rep_w = re.sub(r'th', "", w, re.I)
            inputValue = inputValue.replace(w, rep_w)
    return inputValue

def abbre_fingerprint(value):
    # remove leading and trailing whitespace
    # change all characters to their lowercase representation
    s1 = value.strip().lower()
    # remove all punctuation and control characters
    s2 = s1.translate(str.maketrans('', '', string.punctuation))
    # normalize extended western characters to their ASCII representation (for example "gödel" → "godel")
    s3 = unicodedata.normalize(u'NFKD', s2).encode('ascii', 'ignore').decode('utf8')
    # change to the aabbreviation
    s3 = normalizeStreetSuffixes(s3)
    # split the string into whitespace-separated tokens
    s4 = s3.split()
    # sort the tokens and remove duplicates
    s5 = sorted(list(set(s4)))
    # join the tokens back together
    s6 = ' '.join(s5)
    key = s6
    return (key,value)


task2h_result = street_name_rdd.map(lambda line: abbre_fingerprint(str(line))).distinct().map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y).sortByKey()


#################
# Task 2-i: Provide your qualitative response in template2.txt.
#################

"""
Output Methods - Do not Change. UNCOMMENT these lines when you have the tasks finished
"""
#
task2c_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-c.out")
task2d_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-d.out")
task2e_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-e.out")
task2f_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-f.out")
task2h_result.map(lambda x: x[0] + ',' + ','.join(x[1])).saveAsTextFile("hw3-task2-h.out")
