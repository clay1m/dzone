from __future__ import print_function
import datetime
import re


#setup and global use vars
skipParsing = 0
insertionBatchSize = 40
lookAhead = datetime.timedelta(seconds=30, minutes=0, hours=0, days=0)
customer = "test"
# customer = "test"
regex = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-)(?: - "(.*?)" "(.*?)")?'
pattern = re.compile(regex)
# logInputfile = open('/Users/claytonmorgan/Documents/Logs/Logs/WebServer/Unity/Unity-20170613')
logInputfile = open('/Users/claytonmorgan/Documents/MySQL/searchLogs.txt')
now_as_str = str(datetime.datetime.now().timestamp())
# processLineLimit = 500000
processLineLimit = 1E8
logErrorFileName = '/Users/claytonmorgan/Documents/MySQL/searchLogsErrors_' + customer + '_' + now_as_str + '.txt'
try:
    logErrorFile = open(logErrorFileName, 'r')
except IOError:
    logErrorFile = open(logErrorFileName, 'w')

class Timezone(datetime.tzinfo):

    def __init__(self, name="+0000"):
        self.name = name
        seconds = int(name[:-2])*3600+int(name[-2:])*60
        self.offset = datetime.timedelta(seconds=seconds)

    def utcoffset(self, dt):
        return self.offset

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return self.name


requestTableName = customer + '_request'
searchTableName = customer + '_search'
sessionTableName = customer + '_session'
resultTableName = customer + '_results'
SQL = {}

SQL['request'] = (
     "CREATE TABLE IF NOT EXISTS `search`.`" + requestTableName + "` ("
     "  `id` int(11) NOT NULL AUTO_INCREMENT,"
     "  `ip` varchar(15) NOT NULL,"
     "  `time_stamp` timestamp NOT NULL DEFAULT '1970-01-01 00:00:01',"
     "  `last_time_stamp` timestamp NOT NULL DEFAULT '1970-01-01 00:00:01',"
     "  `next_time_stamp` timestamp NOT NULL DEFAULT '1970-01-01 00:00:01',"
     "  `method` varchar(10) NOT NULL,"
     "  `uri` varchar(400) NOT NULL,"
     "  `protocol` varchar(10) NOT NULL,"
     "  `responseCode` varchar(15) NOT NULL,"
     "  `responseSize` int(11) NOT NULL,"
     "  `userAgent` varchar(15) NOT NULL,"
     "  `referrer` varchar(15) NOT NULL,"
     "  `sessionId` varchar(150) NOT NULL,"
     "  `reqType` varchar(1) NOT NULL DEFAULT 'O',"
     "  `lastReqType` varchar(1) NOT NULL DEFAULT 'O',"
     "  `nextReqType` varchar(1) NOT NULL DEFAULT 'O',"
     "  `keepForSearch` int(1)  NOT NULL DEFAULT 1,"
     "  `searchTypeCode` varchar(2) NOT NULL DEFAULT 'N',"
     "  `searchResolutionCode` varchar(2) NOT NULL DEFAULT 'N',"
     "  `searchResultCode` varchar(2) NOT NULL DEFAULT 'N',"
     "  `relatedReqId` int(11) NOT NULL DEFAULT 0,"
    # "  FOREIGN KEY (sessionId) REFERENCES search." + sessionTableName + "(id)"
    # "  ON DELETE CASCADE"
    # "  ON UPDATE CASCADE,"
     "  PRIMARY KEY (id)"
     ") ENGINE=InnoDB")

SQL['request_reqType_Index'] = ()

SQL['req_update'] = (
    "UPDATE  `search`.`" + requestTableName + "`"
    " SET last_time_stamp = (%s), next_time_stamp = (%s), lastReqType = (%s), nextReqType = (%s)"
    " WHERE id = (%s)")

SQL['req_update_keep_for_search'] = (
    "UPDATE  `search`.`" + requestTableName + "`"
    " SET keepForSearch = (%s)"
    " WHERE id = (%s)")

SQL['req_update_search_codes_type'] = (
    "UPDATE  `search`.`" + requestTableName + "`"
    " SET keepForSearch = (%s), searchTypeCode = (%s)"
    " WHERE id = (%s)")

SQL['req_update_search_codes_resolution'] = (
    "UPDATE  `search`.`" + requestTableName + "`"
    " SET searchResolutionCode = (%s)"
    " WHERE id = (%s)")

SQL['req_update_search_codes_result'] = (
    "UPDATE  `search`.`" + requestTableName + "`"
    " SET searchResultCode = (%s), relatedReqId = (%s)"
    " WHERE id = (%s)")

SQL['req_update_sessionId'] = (
    "UPDATE  `search`.`" + requestTableName + "`"
    " SET sessionId = (%s)"
    " WHERE ip = %s AND (time_stamp BETWEEN %s AND %s) ")

SQL['req_insert_single'] = (
    "INSERT IGNORE INTO " + requestTableName + "(ip,time_stamp,method,uri,protocol," 
    "responseCode,responseSize,userAgent,referrer,reqType) "
    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

SQL['req_insert_multi_prefix'] = (
    "INSERT IGNORE INTO " + requestTableName + "(ip,time_stamp,method,uri,protocol,"
    "responseCode,responseSize,userAgent,referrer,reqType) "
    "VALUES ")

SQL['req_indices'] = ("ALTER TABLE " + requestTableName + " ADD INDEX (ip), ADD INDEX (time_stamp)")

SQL['request_distinct_ip'] = (
    "SELECT DISTINCT ip FROM `search`.`" + requestTableName + "`"
    "  ORDER BY ip")

SQL['session'] = (
    "CREATE TABLE IF NOT EXISTS `search`.`" + sessionTableName + "` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `ip` varchar(15) NOT NULL,"
    "  `start_time` timestamp NOT NULL DEFAULT '1970-01-01 00:00:01',"
    "  `end_time` timestamp NOT NULL DEFAULT '1970-01-01 00:00:01',"
    "  `sessionId` varchar(150) NOT NULL,"
    "  `closed` int(1) NOT NULL DEFAULT 0,"
    "  `search_page` int(10)  NOT NULL DEFAULT 0,"
    "  `search_auto` int(10)  NOT NULL DEFAULT 0,"
    "  `search_auto_avg` float NOT NULL DEFAULT 0,"
    "  `result_count` int(10)  NOT NULL DEFAULT 0,"
    "  PRIMARY KEY (id)"
    ") ENGINE=InnoDB")

SQL['session_sessionId_Index'] = ()

SQL['session_sessionId_Index'] = ()

SQL['session_agg_subquery'] = (
    "SELECT ip, min(time_stamp) as start_time, max(time_stamp) as end_time FROM `search`.`" + requestTableName + "`"
    "  WHERE"
    "  ip = %s"
    "  GROUP BY ip"
    "  ORDER BY start_time")

SQL['ip_session_hour_blocks'] = (
    "SELECT * FROM `search`.`" + requestTableName + "`"
    "  WHERE"
    "  ip = %s AND keepForSearch = 1 AND (time_stamp BETWEEN %s AND %s)"
    "  ORDER BY time_stamp")

SQL['session_subquery'] = (
    "SELECT id, ip, reqType, time_stamp FROM `search`.`" + requestTableName + "`"
    "  WHERE"
    "  ip = %s"
    "  ORDER BY time_stamp")

SQL['session_subquery_ip_search_only'] = (
    "SELECT id, ip, reqType, time_stamp FROM `search`.`" + requestTableName + "`"
    "  WHERE"
    "  ip = %s AND keepForSearch = 1"
    "  ORDER BY time_stamp")

SQL['session_insert_first'] = (
    "INSERT IGNORE INTO " + sessionTableName + "(ip,start_time,end_time)"
    " VALUES(%s,%s,%s)")

SQL['session_insert_other'] = (
    "INSERT IGNORE INTO " + sessionTableName + "(ip,start_time,end_time,sessionId,closed,search_page,search_auto,search_auto_avg,result_count)"
    " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)")


SQL['result'] = (
    "CREATE TABLE IF NOT EXISTS `search`.`" + resultTableName + "` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `content` varchar(200) NOT NULL,"
    "  `click_time` timestamp NOT NULL DEFAULT '1970-01-01 00:00:01',"
    "  `clicked` int(1) NOT NULL,"
    "  PRIMARY KEY (id)"
    ") ENGINE=InnoDB")


SQL['search'] = (
    "CREATE TABLE  IF NOT EXISTS `search`.`" + searchTableName + "` AS"
    "  SELECT id, ip, time_stamp, reqType, lastReqType, nextReqType method, uri, protocol, responseCode, responseSize, userAgent, referrer, sessionId"
    "  FROM `search`.`" + requestTableName + "` WHERE"
    "  uri LIKE '%/search.json%'"
    "  OR uri LIKE '%/search.html%'"
    "  OR uri LIKE '%/questions/%/%/.html%'"
    "  OR uri LIKE '%/article/%'"
    "  OR uri LIKE '%/%idea%/%'"
    "  ORDER BY ip, time_stamp")

SQL['req_dups'] = (
    "DELETE " + requestTableName + " FROM " + requestTableName + " INNER JOIN("
    "  SELECT  min(id) minid, ip, time_stamp, uri, responseCode, responseSize"
    "  FROM " + requestTableName + " GROUP BY ip, time_stamp, uri, responseCode, responseSize"
    "  HAVING COUNT(1) > 1) as duplicates"
    "  ON (duplicates.ip = " + requestTableName + ".ip"
    "  AND duplicates.time_stamp = " + requestTableName + ".time_stamp"
    "  AND duplicates.uri = " + requestTableName + ".uri"
    "  AND duplicates.responseCode = " + requestTableName + ".responseCode"
    "  AND duplicates.responseSize = " + requestTableName + ".responseSize"
    "  AND duplicates.minid <> " + requestTableName + ".id)")

#********SEARCH REPORTS*************************************************************
# Total Reqs
SQL['total_reqs'] = ("SELECT COUNT(*) FROM `search`.`" + requestTableName + "`")

# Total Reqs related to search queries
SQL['total_reqs_search'] = (
    "SELECT COUNT(*) FROM `search`.`" + requestTableName + "`"
    " WHERE reqType IN ('S','J')")

# Total Reqs related to search page queries only
SQL['total_reqs_search_page'] = (
    "SELECT COUNT(*) FROM `search`.`" + requestTableName + "`"
    " WHERE reqType IN ('S')")

# Total Reqs related to auto search only
SQL['total_reqs_auto_search'] = (
    "SELECT COUNT(*) FROM `search`.`" + requestTableName + "`"
    " WHERE reqType IN ('J')")

# Total Reqs related to results
SQL['total_reqs_results'] = (
    "SELECT COUNT(*) FROM `search`.`" + requestTableName + "`"
    " WHERE reqType NOT IN ('S','J','O')")

# Total reqs of bad searches from Search Page
SQL['bad_req_searches_sp'] = (
    "SELECT COUNT(*) FROM `search`.`" + requestTableName + "`"
    " WHERE reqType IN ('S') AND nextReqType = 'O'")

#Total reqs of bad searches from Auto Search
SQL['bad_req_searches_as'] = (
    "SELECT COUNT(*) FROM `search`.`" + requestTableName + "`"
    " WHERE reqType IN ('J') AND lastReqType <> 'J' AND nextReqType = 'O'")

#Total reqs of bad searches
SQL['bad_req_searches'] = (
    "SELECT COUNT(*) FROM `search`.`" + requestTableName + "`"
    " WHERE (reqType IN ('J') AND lastReqType <> 'J' AND nextReqType = 'O' AND keepForSearch = 1)"
    " OR"
    "(reqType IN ('S') AND nextReqType IN ('O','J','S') AND keepForSearch = 1)")

#Total reqs of bad searches by type
SQL['bad_req_searches_by_type'] = (
    "SELECT searchResolutionCode, COUNT(searchResolutionCode) FROM `search`.`" + requestTableName + "`"
    " WHERE (searchTypeCode LIKE 'J%' OR  searchTypeCode LIKE 'S%')"
    " GROUP BY searchResolutionCode")


#Total Sessions
SQL['total_sessions'] = ("SELECT COUNT(*) FROM `search`.`" + sessionTableName + "`")

#Total sessions with at least one search page hit
SQL['sessions_with_sp_hits'] = (
    "SELECT COUNT(*) FROM `search`.`" + sessionTableName + "`"
    " WHERE search_page > 0")

#Total sessions with at least one auto search seq
SQL['sessions_with_as_seqs'] = (
    "SELECT COUNT(*) FROM `search`.`" + sessionTableName + "`"
    " WHERE search_auto > 0")

#Total sessions with at least one search page hit and no results
SQL['sessions_with_sp_hits_no_results'] = (
    "SELECT COUNT(*) FROM `search`.`" + sessionTableName + "`"
    " WHERE search_page > 0 AND result_count = 0")

#Total sessions with at least one autosearch seq and no results
SQL['sessions_with_as_seqs_no_results'] = (
    "SELECT COUNT(*) FROM `search`.`" + sessionTableName + "`"
    " WHERE search_auto > 0 AND result_count = 0")

#Avg number of results per session
SQL['session_avg_results'] = ("SELECT AVG(result_count) FROM `search`.`" + sessionTableName + "`")

#Avg number of search page hits per session
SQL['session_avg_sp_hits'] = ("SELECT AVG(search_page) FROM `search`.`" + sessionTableName + "`")

#Avg number of auto search sequences per session
SQL['session_avg_as_seqs'] = ("SELECT AVG(search_auto) FROM `search`.`" + sessionTableName + "`")

