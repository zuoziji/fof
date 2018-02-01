# -*- coding: utf-8 -*-

from config_fh import get_db_session


import logging

logger = logging.getLogger()

class DataTablesServer(object):
    def __init__(self, request, columns, index, table):

        with get_db_session() as session:
            self.dbh = session
        self.columns = columns
        self.index = index
        self.table = table
        self.request_values = request.values
        self.resultData = None
        self.cadinalityFiltered = 0
        self.cadinality = 0
        self.run_queries()


    def output_result(self):
        output = {}
        output['sEcho'] = str(int(self.request_values['sEcho']))
        output['iTotalRecords'] = str(self.cardinality)
        output['iTotalDisplayRecords'] = str(self.cadinalityFiltered)
        aaData_rows = []
        for row in self.resultData:
            aaData_row = {}
            for i in range(len(self.columns)):
                aaData_row[self.columns[i]] = row[i]
            aaData_rows.append(aaData_row)
        output['aaData'] = aaData_rows
        return output

    def run_queries(self):
        dataCursor = self.dbh

        result = dataCursor.execute("""
            SELECT SQL_CALC_FOUND_ROWS %(columns)s
            FROM   %(table)s %(where)s %(order)s %(limit)s""" % dict(
            columns=', '.join(self.columns), table=self.table, where=self.filtering(), order=self.ordering(),
            limit=self.paging()
        ))
        logger.info("query table  --------> {}".format(self.table))
        self.resultData = result.fetchall()
        logger.info("find records size--------> {}".format(len(self.resultData)))
        cadinalityFilteredCursor = self.dbh
        candinaResult = cadinalityFilteredCursor.execute("""
            SELECT FOUND_ROWS()
        """)
        self.cadinalityFiltered = candinaResult.fetchone()[0]

        cadinalityCursor = self.dbh
        cadinaCurso = cadinalityCursor.execute("""SELECT COUNT(%s) FROM %s""" % (self.index, self.table))
        self.cardinality = cadinaCurso.fetchone()[0]

    def filtering(self):
        if (self.request_values.has_key('sSearch')) and (self.request_values['sSearch'] != ""):
            filter = "WHERE "
            for i in range(len(self.columns)):
                filter += "%s LIKE '%%%s%%' OR " % (self.columns[i], self.request_values['sSearch'])
            filter = filter[:-3]
            print(filter)
            return filter
        elif self.request_values['sSearch_6'] != "" and (self.request_values['sSearch_6'] != "5"):
            tag = self.request_values['sSearch_6']
            key = None
            if self.table == 'fund_info':
                key = "rank"
            elif self.table == "fund_mgrcomp_info":
                key = "review_status"
            filter = "WHERE %s = %s" %(key,tag)
            return filter

    def ordering(self):
        order = ""
        if (self.request_values['iSortCol_0'] != "") and (int(self.request_values['iSortingCols']) > 0):
            order = "ORDER BY  "
            for i in range(int(self.request_values['iSortingCols'])):
                order += "%s %s, " % (self.columns[int(self.request_values['iSortCol_' + str(i)])],
                                      self.request_values['sSortDir_' + str(i)])
        return order[:-2]

    def paging(self):
        limit = ""
        if (self.request_values['iDisplayStart'] != "") and (self.request_values['iDisplayLength'] != -1):
            limit = "LIMIT %s, %s" % (self.request_values['iDisplayStart'], self.request_values['iDisplayLength'])
        return limit

