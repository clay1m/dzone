from __future__ import print_function
import mysql.connector
from mysql.connector import Error
import time
import datetime
import re
import sys
from mysql.connector import errorcode
from mysql.connector import IntegrityError
import math
from searchCons import requestTableName, searchTableName, sessionTableName, resultTableName, SQL, Timezone, regex,\
                        processLineLimit, logInputfile, now_as_str, logErrorFile, skipParsing, lookAhead, customer, \
                        insertionBatchSize

# record current time to measure total run time later
startTimeOverall = datetime.datetime.now()
runTimeTotal = 0
startTimeParsing = 0
runTimeParsing = 0

#connect to the mysql search schema and get a live cursor
conn = mysql.connector.connect(host='localhost',
                               database='search',
                               user='root',
                               password='')
cursor = conn.cursor(dictionary=True, buffered=True)


def executeDDL(sql, params = [], cursor = cursor, logFile = logErrorFile, exitOnError = True, printSQL = True):
    if printSQL:
        print(sql)
    try:
        if len(params) > 0:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()
    except Error as e:
        print(e)
        logFile.write('Error when attempting DDL query -> ' + sql + ' with error -> ' + str(e) + '\n')
        print('Error when attempting DDL query -> ' + sql + '\n')
        if exitOnError:
            sys.exit(1)
    return rows

def executeDML(sql, params = [], cursor = cursor, logFile = logErrorFile, exitOnError = True, autoIncrement = True, printSQL = True):
    if printSQL:
        print(sql)
    try:
        if len(params) > 0:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        if autoIncrement:
            if cursor.lastrowid:
                print('last auto insert id into table ', cursor.lastrowid)
            else:
                print('last auto insert id not found in table')
        result = conn.commit()
    except Error as e:
        print(e)
        logFile.write('Error when attempting DML query -> ' + sql + ' with error -> ' + str(e) + '\n')
        print('Error when attempting DML query -> ' + sql + '\n')
        if exitOnError:
            sys.exit(1)
    return result

def divideTimeRange(start, end, period = 30, format = '%Y-%m-%d %H:%M:%S'):
    periodDT = datetime.timedelta(seconds=0, minutes=period, hours=0, days=0)
    # startDT = datetime.datetime.strptime(start, format)
    # endDT = datetime.datetime.strptime(end, format)
    diff = end - start
    numPeriods = math.floor(diff/periodDT)
    dateRange = []
    dateRange.insert(0,start)
    for i in range(1,numPeriods):
        d = dateRange[i-1] + periodDT
        dateRange.insert(i, d)
    dateRange.insert(numPeriods+1,end)
    print(dateRange)
    return dateRange

def executeReports(outputFileName = '/Users/claytonmorgan/Documents/MySQL/searchReports_' + customer + '_' + now_as_str + '.txt'):
    # logErrorFileName = '/Users/claytonmorgan/Documents/MySQL/searchLogsErrors_' + customer + '_' + now_as_str + '.txt'
    try:
        outFile = open(outputFileName, 'r')
    except IOError:
        outFile = open(outputFileName, 'w')

    result = executeDDL(SQL['total_reqs'])
    outFile.write('Requests: ' + str(result) + '\n')
    result = executeDDL(SQL['total_reqs_search'])
    outFile.write('Search Requests: ' + str(result) + '\n')
    result = executeDDL(SQL['total_reqs_search_page'])
    outFile.write('Search Page Requests: ' + str(result) + '\n')
    result = executeDDL(SQL['total_reqs_auto_search'])
    outFile.write('Auto Search Requests: ' + str(result) + '\n')
    result = executeDDL(SQL['total_reqs_results'])
    outFile.write('Results Requests: ' + str(result) + '\n')
    result = executeDDL(SQL['bad_req_searches_sp'])
    outFile.write('Bad Search Page Requests: ' + str(result) + '\n')
    result = executeDDL(SQL['bad_req_searches_as'])
    outFile.write('Auto Search Bad Requests: ' + str(result) + '\n')
    result = executeDDL(SQL['bad_req_searches'])
    outFile.write('Total Bad Search Requests: ' + str(result) + '\n')
    result = executeDDL(SQL['bad_req_searches_by_type'])
    outFile.write('Total Bad Search Requests By Type: ' + str(result) + '\n')
    result = executeDDL(SQL['total_sessions'])
    outFile.write('Total Sessions: ' + str(result) + '\n')
    result = executeDDL(SQL['sessions_with_sp_hits'])
    outFile.write('Sessions with SP Hits: ' + str(result) + '\n')
    result = executeDDL(SQL['sessions_with_as_seqs'])
    outFile.write('Sessions with AS Sequences: ' + str(result) + '\n')
    result = executeDDL(SQL['sessions_with_sp_hits_no_results'])
    outFile.write('Sessions with SP Hits No Results: ' + str(result) + '\n')
    result = executeDDL(SQL['sessions_with_as_seqs_no_results'])
    outFile.write('Sessions with AS Sequences No Results: ' + str(result) + '\n')
    result = executeDDL(SQL['session_avg_results'])
    outFile.write('Average Results per Session: ' + str(result) + '\n')
    result = executeDDL(SQL['session_avg_sp_hits'])
    outFile.write('Average SP Hits per Session: ' + str(result) + '\n')
    result = executeDDL(SQL['session_avg_as_seqs'])
    outFile.write('Average AS Sequences per Session: ' + str(result) + '\n')
    outFile.write('Insertion Batch Size: ' + str(insertionBatchSize) + '\n')
    outFile.write('Parsing Processing Time: ' + str(runTimeParsing) + '\n')
    outFile.write('Total Processing Time: ' + str(runTimeTotal) + '\n')
    outFile.close()

def parseLogsIntoRequestTable(logInputfile = logInputfile, logErrorFile = logErrorFile, processLineLimit = processLineLimit, regex = regex, SQL = SQL, conn = conn):
    #read the raw data in from the file and parse into a mysql db table

    skippedRecords = 0
    processedLines = 0
    addedRequestRecs = 0
    failedReqInserts = 0
    insertionBatchCount = 0
    argMat = [[]]
    rowsToInsert = []
    argString = ''
    lines = logInputfile.readlines()
    numLines = len(lines)
    lineNum = 1
    for line in lines:
        if processedLines > processLineLimit:
            break

        print(line)
        print(re.match(regex, line))

        #use regex to extract the fields for the req table from this line of the log file
        try:
            lineT = re.match(regex, line).groups(0)
        except AttributeError as e:
            print(e)
            logErrorFile.write(line + '\n')
            continue

        #put the time stamp in a suitable format for mysql
        date_time = lineT[1]
        tt = time.strptime(date_time[:-6], "%d/%b/%Y:%H:%M:%S")
        tt = list(tt[:6]) + [ 0, Timezone(date_time[-5:]) ]
        date_time = datetime.datetime(*tt)

        #extract the http verb, uri, and protocol from the string Tomcat log combines them in
        try:
            method, uri, protocol = lineT[2].split()
        except ValueError as e:
            print(e)
            logErrorFile.write(line + ' -> ' + str(e) + '\n')
            skippedRecords += 1
            continue

        #determine the value of the request type from the uri string
        reqType = 'O'
        if re.match('/search.*json', uri) is not None:
            reqType = 'J'
        elif re.match('/search.*html', uri) is not None:
            reqType = 'S'
        elif re.match('/questions/', uri) is not None:
            reqType = 'Q'
        elif re.match('/article/', uri) is not None:
            reqType = 'A'
        elif re.match('/.*idea.*/', uri) is not None:   #I think we want /idea/<number/ here but what about /content/idea/list.html
            reqType = 'I'
        elif re.match('/users/', uri) is not None and re.match('/users/.*/photo.view.htm', uri) is None and re.match('/users/.*logout', uri) is None:
            reqType = 'U'
        elif re.match('/topics/', uri) is not None:
            reqType = 'T'


        #convert the response size to an integer so we can do math on it in mysql
        responseSizeStr = lineT[4]
        try:
            responseSize = int(responseSizeStr)
        except ValueError as e:
            print(e)
            responseSize = 0

        ip = lineT[0];

        # insert the new req record for this line of log input
        sql = SQL['req_insert_single']
        args = (lineT[0], date_time, method, uri, protocol, lineT[3], responseSize, lineT[5], lineT[6], reqType)
        argMat.append(args)

        # if insertionBatchCount == insertionBatchSize-1 or lineNum == numLines-1:
        #     argString += str(args) + ')'
        #     insertionBatchCount += 1
        if insertionBatchCount == insertionBatchSize or lineNum == numLines:
            sql = SQL['req_insert_multi_prefix']
            sql = SQL['req_insert_single']
            # executeBatchDML(sql,argString)
            try:
                affected_count = cursor.executemany(sql, rowsToInsert)
                conn.commit()
            except Error as e:
                print(e)
                logErrorFile.write(line + ' -> ' + str(e) + '\n')
            insertionBatchCount = 0
            argString = ''
            rowsToInsert = []
        else:
            argString += str(args) + '), ('
            rowsToInsert.append(args)
            insertionBatchCount += 1

        # executeDML(sql,args)


        processedLines += 1
        print(processedLines)
        lineNum += 1


def executeBatchDML(sqlPrefix, argString, cursor = cursor, logFile = logErrorFile, exitOnError = True, printSQL = True):
    sql = sqlPrefix + argString
    if printSQL:
        print(sql)
    try:
        cursor.execute(sql)
        result = conn.commit()
    except Error as e:
        print(e)
        logFile.write('Error when attempting batch DML query -> ' + sql + ' with error -> ' + str(e) + '\n')
        print('Error when attempting batch DML query -> ' + sql + '\n')
        if exitOnError:
            sys.exit(1)
    return result



# create table for session data
sql = SQL['session']
test = executeDML(sql)

# create table for result data
sql = SQL['result']
test = executeDML(sql)

#create request data table for this customer if it does not already exist
sql = SQL['request']
test = executeDML(sql)

# process the request logs into the request table
if skipParsing != 1:
    startTimeParsing = datetime.datetime.now()
    parseLogsIntoRequestTable()
    endTimeParsing = datetime.datetime.now()
    runTimeParsing = (endTimeParsing - startTimeParsing).total_seconds()
    print(runTimeParsing)


#$$$$$!!!!!!!!!!!!!!!!FIXXXXX THISSSSSSSS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# # make sure we have not added duplicate records and if so delete all but 1 copy
# # sql = SQL['req_dups']
# # try:
# #     cursor.execute(sql)
# #     conn.commit()
# # except Error as e:
# #     print(e)
# #     logErrorFile.write('Error when attempting to delete duplicate records -> ' + str(e) + '\n')
#$$$$$!!!!!!!!!!!!!!!!FIXXXXX THISSSSSSSS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# add indices to the req table for faster searching
sql = SQL['req_indices']
executeDML(sql)
try:
    cursor.execute(sql)
    conn.commit()
except Error as e:
    print(e)
    logErrorFile.write('Error when attempting to create indices on req table -> ' + str(e) + '\n')



# populate the session table
sql = SQL['request_distinct_ip']
unique_ips = executeDDL(sql)

for unique_ip in unique_ips:
    ip = unique_ip["ip"]
    sessionCount = 0

    # execute an aggregate query to get min and max times for the ip and update the session record (maybe change this later?)
    sql = SQL['session_agg_subquery']
    rs = executeDDL(sql, (ip,))
    # query should produce exactly 1 row
    req_agg_result_by_ip = rs[0]
    ipBlockStartTime = req_agg_result_by_ip["start_time"]
    endTime = req_agg_result_by_ip["end_time"]

    # divide ip blocks into smaller "session" blocks
    times = divideTimeRange(ipBlockStartTime, endTime)

    # execute a query to get all reqs for the current ip ordered by time
    sql = SQL['session_subquery']
    reqs_by_ip = executeDDL(sql, (ip,))

    # mark records for inclusion or exclusion in search analysis
    j = 0
    numReqs = len(reqs_by_ip)

    while j <= numReqs-1:
        currentReq = reqs_by_ip[j]
        currentId = currentReq['id']
        currentReqType = currentReq['reqType']
        currentTime = currentReq['time_stamp']
        keepCurrentRecord = 1
        currentSearchResult = 'N'
        searchTypeCode = 'N'
        searchResolutionCode = 'N'
        searchResultCode = 'N'

        if currentReqType == 'S':
            searchTypeCode = 'S'
            keepCurrentRecord = 1
            sql = SQL['req_update_search_codes_type']
            args = (keepCurrentRecord, searchTypeCode, currentId)
            executeDML(sql, args, autoIncrement=False)

            # update current record and do a look ahead
            lookAheadCount = 1
            timeDiff = 0
            keepLooking = 1
            while keepLooking:
                keepLookAheadReq = 1
                if (j+lookAheadCount) <= numReqs-1:
                    lookAheadReq = reqs_by_ip[j+lookAheadCount]
                else:
                    break
                lookAheadId = lookAheadReq['id']
                lookAheadReqType = lookAheadReq['reqType']
                lookAheadTime = lookAheadReq['time_stamp']
                timeDiff = lookAheadTime - currentTime
                if timeDiff > lookAhead:
                    #too late
                    if lookAheadReqType not in ['O', 'S', 'J']:
                        # outside time range but good result
                        #update search resolution for this req to T (timeout)
                        searchResolutionCode = 'T'
                        #update search Result for this lookahead req to Good Timeout 'TG'
                        searchResultCode = 'TG'
                    elif lookAheadReqType in ['S', 'J']:
                        #update searchResult for this req to Bad 'T' timeout
                        searchResolutionCode = 'T'
                        #update searchResult for this lookahead req to Timeout 'BM'
                        searchResultCode = 'TM'
                    else:
                        #must be type O so just a timeout not having reached a valid result
                        searchResolutionCode = 'T'
                        #update searchResult for this lookahead req to Timeout Other 'TO'
                        searchResultCode = 'TO'
                elif timeDiff <= lookAhead:
                    #still in time

                    if lookAheadReqType not in ['O', 'S', 'J']:
                        # good result and in time
                        #update search resolution for this req to G (Good)
                        searchResolutionCode = 'G'
                        #update search Result for this lookahead req to G (Good)
                        searchResultCode = 'G'
                    elif lookAheadReqType in ['S', 'J']:
                        #update searchResult for this req to Multisearch 'M'
                        searchResolutionCode = 'M'
                        #update searchResult for this lookahead req to Multisearch 'M'
                        searchResultCode = 'M'
                    else:
                        #must be type O
                        # do not keep this record for search analysis
                        keepLookAheadReq = 0
                        sql = SQL['req_update_keep_for_search']
                        args = (keepLookAheadReq, lookAheadId)
                        executeDML(sql, args, autoIncrement=False)
                        lookAheadCount += 1
                        continue

                #make sql updates to search req and result req
                sql = SQL['req_update_search_codes_resolution']
                args = (searchResolutionCode, currentId)
                executeDML(sql, args, autoIncrement=False)
                sql = SQL['req_update_search_codes_result']
                args = (searchResultCode, currentId, lookAheadId)
                executeDML(sql, args, autoIncrement=False)
                keepLooking = 0
                break




        elif currentReqType == 'J':
            if j > 0 and j <= numReqs-2:
                lastReq = reqs_by_ip[j-1]
                nextReq = reqs_by_ip[j+1]
                lastReqType = lastReq['reqType']
                nextReqType = nextReq['reqType']
                if lastReqType == 'J':
                    if nextReqType == 'J':
                        searchTypeCode = 'JM'
                    elif nextReqType == 'S':
                        searchTypeCode = 'JB'
                    else:
                        searchTypeCode = 'JL'
                elif lastReqType != 'J':
                    searchTypeCode = 'JF'
            keepCurrentRecord = 1
            sql = SQL['req_update_search_codes_type']
            args = (keepCurrentRecord, searchTypeCode, currentId)
            executeDML(sql, args, autoIncrement=False)

            # update current record and do a look ahead
            lookAheadCount = 1
            timeDiff = 0
            keepLooking = 1
            while keepLooking:
                keepLookAheadReq = 1
                if (j+lookAheadCount) <= numReqs-1:
                    lookAheadReq = reqs_by_ip[j+lookAheadCount]
                else:
                    break
                lookAheadId = lookAheadReq['id']
                lookAheadReqType = lookAheadReq['reqType']
                lookAheadTime = lookAheadReq['time_stamp']
                timeDiff = lookAheadTime - currentTime
                if timeDiff > lookAhead:
                    #too late
                    #update search resolution for this req to T (timeout)
                    searchResolutionCode = 'T'
                    if lookAheadReqType not in ['O', 'S', 'J']:
                        # outside time range but good result
                        #update search Result for this lookahead req to Good Timeout 'TG'
                        searchResultCode = 'TG'
                    elif lookAheadReqType in ['S', 'J']:
                        # outside time range but valid search continuation result
                        #update searchResult for this lookahead req to Timeout and search type 'TS' or 'TJ'
                        searchResultCode = 'T' + lookAheadReqType
                    else:
                        #must be type O so just a timeout not having reached a valid result
                        #update searchResult for this lookahead req to Timeout Other 'TO'
                        searchResultCode = 'TO'
                elif timeDiff <= lookAhead:
                    #still in time

                    if lookAheadReqType not in ['O', 'S', 'J']:
                        # good result and in time
                        #update search resolution for this req to G (Good)
                        searchResolutionCode = 'G'
                        #update search Result for this lookahead req to G (Good)
                        searchResultCode = 'G'
                    elif lookAheadReqType in ['S']:
                        # valid search continuation
                        #update search resolution for this req to Continuation Searchpage 'CS'
                        searchResolutionCode = 'CS'
                        #update searchResult for this lookahead to Continuation 'C'
                        searchResultCode = 'C'
                    elif lookAheadReqType in ['J']:
                        #update search resolution for this req to Continuation Auto Search 'CJ'
                        searchResolutionCode = 'CJ'
                        #update searchResult for this lookahead to Continuation 'C'
                        searchResultCode = 'C'
                    else:
                        #must be type O (Other)
                        # do not keep this record for search analysis
                        keepLookAheadReq = 0
                        sql = SQL['req_update_keep_for_search']
                        args = (keepLookAheadReq, lookAheadId)
                        executeDML(sql, args, autoIncrement=False)
                        lookAheadCount += 1
                        continue

                #make sql updates to search req and result req
                sql = SQL['req_update_search_codes_resolution']
                args = (searchResolutionCode, currentId)
                executeDML(sql, args, autoIncrement=False)
                sql = SQL['req_update_search_codes_result']
                args = (searchResultCode, currentId, lookAheadId)
                executeDML(sql, args, autoIncrement=False)
                keepLooking = 0
                break

        elif currentReqType == 'O':
            # req can be excluded from search analysis
            keepCurrentRecord = 0
            sql = SQL['req_update_keep_for_search']
            args = (keepCurrentRecord, currentId)
            executeDML(sql, args, autoIncrement=False)

        j += 1


    # for each search related req look ahead 1 record and look behind 1 record to populate last and next fields
    # execute a query to get all search related reqs for the current ip ordered by time
    sql = SQL['session_subquery_ip_search_only']
    reqs_by_ip_search_only = executeDDL(sql, (ip,))
    numReqs = len(reqs_by_ip_search_only)
    i = 0
    if numReqs > 0:
        if numReqs == 1:
            currentReq = reqs_by_ip_search_only[0]
            lastTime = currentReq['time_stamp']
            nextTime = currentReq['time_stamp']
            lastReqType = currentReq['reqType']
            nextReqType = currentReq['reqType']
            currentId = currentReq['id']

        while i < numReqs-1:
            currentReq = reqs_by_ip_search_only[i]
            currentId = currentReq['id']
            if i == 0:
                #first record in the block so no look behind
                nextReq = reqs_by_ip_search_only[i+1]
                lastTime = currentReq['time_stamp']
                nextTime = nextReq['time_stamp']
                lastReqType = currentReq['reqType']
                nextReqType = nextReq['reqType']
            elif i == numReqs-1:
                lastReq = reqs_by_ip_search_only[i-1]
                lastTime = lastReq['time_stamp']
                nextTime = currentReq['time_stamp']
                lastReqType = lastReq['reqType']
                nextReqType = currentReq['reqType']
            else:
                lastReq = reqs_by_ip_search_only[i-1]
                nextReq = reqs_by_ip_search_only[i+1]
                lastTime = lastReq['time_stamp']
                nextTime = nextReq['time_stamp']
                lastReqType = lastReq['reqType']
                nextReqType = nextReq['reqType']

            #update the req record with times and types of reqs immediately before and after the current req by timestamp
            sql = SQL['req_update']
            args = (lastTime, nextTime, lastReqType, nextReqType, currentId)
            executeDML(sql, args, autoIncrement=False)
            i += 1

        # deal with last record in the group as a special case
        #TODO: remove this try catch
        try:
            lastReq = reqs_by_ip_search_only[numReqs-2]
            currentReq = reqs_by_ip_search_only[numReqs-1]
        except Error as e:
            print(e)
            continue

        currentId = currentReq['id']
        lastTime = lastReq['time_stamp']
        nextTime = currentReq['time_stamp']
        lastReqType = lastReq['reqType']
        nextReqType = currentReq['reqType']
        sql = SQL['req_update']
        args = (lastTime, nextTime, lastReqType, nextReqType, currentId)
        executeDML(sql, args, autoIncrement=False)

    # break each block by ip into "session" blocks by ip and time period
    for i in range(0,len(times)-1):
        start = times[i]
        end = times[i+1]
        searchCount = 0
        sql = SQL['ip_session_hour_blocks']
        args = (ip, start, end)
        sessionBlock = executeDDL(sql, args)

        # check if block has any search records
        resultCount = 0
        searchPageCount = 0
        autoSearchSeqCount = 0
        autoSearchSeqLengths = []
        autoSearchCount = 0
        autoSearchAvg = 0
        sessionId = ip + '_' + str(start) + '_' + str(end)
        numReqs = len(sessionBlock)

        if numReqs >= 1:  # we have a valid session block
            try:
                for r in sessionBlock:
                    currentReqType = r['reqType']
                    lastReqType = r['lastReqType']
                    nextReqType = r['nextReqType']
                    currentTime = r['time_stamp']
                    if currentReqType == 'S':
                        searchPageCount += 1
                    if currentReqType == 'J':
                        autoSearchCount += 1
                        if nextReqType != 'J':
                            autoSearchSeqLengths.append(autoSearchCount)
                            autoSearchCount = 0
                            autoSearchSeqCount += 1
                    elif currentReqType != 'O':
                        resultCount += 1
                if len(autoSearchSeqLengths) > 0:
                    autoSearchAvg = sum(autoSearchSeqLengths)/len(autoSearchSeqLengths)
            except Error as e:
                print(e)
                sys.exit(1)

            #update all of the reqs with the sessionId
            sql = SQL['req_update_sessionId']
            args = (sessionId, ip, start, end)
            result = executeDML(sql, args, autoIncrement=False)

            #update the session record
            closed = 1
            sql = SQL['session_insert_other']
            args = (ip, start, end, sessionId, closed, searchPageCount, autoSearchSeqCount, autoSearchAvg, resultCount)
            executeDML(sql, args)

# final logging and cleanup
logInputfile.close()
logErrorFile.close()
executeReports()
cursor.close()
conn.close()

# record the total run time
print(str(datetime.datetime.now() - startTimeOverall))
endTimeOverall = datetime.datetime.now()
runTimeTotal = (endTimeOverall - startTimeOverall).total_seconds()
print('Insertion Batch Size: ' + str(insertionBatchSize) + '\n')
print('Parsing Processing Time: ' + str(runTimeParsing) + ' secs\n')
print('Total Processing Time: ' + str(runTimeTotal) + ' secs\n')





