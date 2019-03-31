import sqlparse
import sys
import tables

table_dict = {} # table_name : Table class


def clean_it(q, ch):
    return map(lambda x: x.strip(), str(q).split(ch))

def do_select(parsed_query, D, is_distinct):
    # raw_cols = str(parsed_query[2]).split(',')
    # raw_cols = map(lambda x: x.strip(), raw_cols)
    # req_tables = str(parsed_query[6]).split(',')
    # req_tables = map(lambda x: x.strip(), req_tables)
    # print str(parsed_query[4])
    raw_cols = clean_it(parsed_query[2], ',')
    # print is_distinct
    if is_distinct == 1:
        # print "hello?#########################################################3"
        raw_cols.append("distinct");
    req_tables = clean_it(parsed_query[6], ',')
    # print "raw cols: ",raw_cols," req_tables: ",req_tables," length: ",len(parsed_query)
    if len(parsed_query) > 8:
        # print "in where"
        where_cond = clean_it(parsed_query[8], ' ')
        # print where_cond
        where_cond = where_cond[1:]
        where_cond = ''.join(where_cond)
        # print "###Before strip###",where_cond
        where_cond = where_cond.strip(';')
        # print "in where 2"
        # print where_cond
        D.post_query(raw_cols, req_tables, where_cond)
    else:
        # print "hello1"
        D.post_query(raw_cols, req_tables)


def main():
    query = sys.argv[1]
    is_distinct = 0
    if 'distinct' in query:
        is_distinct = 1
        # print is_distinct,"88888888888888888888888888888888888888888888888888"
        query = query.replace("distinct","")
    D = tables.DB('metadata.txt')
    parsed_query = sqlparse.parse(query)[0].tokens
    # print parsed_query
    if str(parsed_query[0]).lower() == 'select':
        do_select(parsed_query, D, is_distinct)

main()
