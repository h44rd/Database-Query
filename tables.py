# import pandas as pd
import itertools
import csv

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

class DB:
    def __init__(self, metafile):
        self.db_structure = {}
        self.cart_prod = []
        self.is_agg = 0
        self.is_distinct = 0
        with open(metafile) as f:
            meta = f.readlines()
        meta = map(lambda x: x.strip(), meta)
        flag = 0
        for m in meta:
            if m == '<begin_table>':
                flag = 1
                continue
            elif flag == 1:
                tname = m
                fname = m + '.csv'
                col_list = []
                flag = 2
            elif flag == 2 and m != '<end_table>':
                col_list.append(m)
            elif m == '<end_table>':
                self.db_structure[tname] = Table(tname, fname, col_list)
                flag = 0
        # print self.db_structure

    def post_query(self, raw_cols, req_tables, where_cond = []): #where_cond -> col1=col3ANDcol2=20
        # print "Im here"
        # print where_cond

        if 'distinct' in raw_cols:
            self.is_distinct = 1
            raw_cols = raw_cols[:-1]
        # print raw_cols
        no_tabs = len(req_tables)
        list_of_lens = []
        self.index_of_req_tabs = {}
        i = 0
        for rt in req_tables:
            self.index_of_req_tabs[rt] = i
            i += 1
            tab_len = self.db_structure[rt].get_total_rows()
            list_of_lens.append(range(tab_len))
        self.cart_prod = list(itertools.product(*list_of_lens))
        # print self.cart_prod
        # print self.index_of_req_tabs
        self.flag_cart_table = [1 for i in range(len(self.cart_prod))]
        # print "Im here also"
        if len(where_cond) > 0:
            # print where_cond
            if 'OR' in where_cond:
                self.eval_two_cond(where_cond, 'OR', req_tables)
                # where_cond = where_cond.split('OR')
                # # for c in where_cond: # c -> "col1=10"
                # #     (op, cols) = break_cond(c)
                # (op1, cols1) = break_cond(where_cond[0])
                # (op2, cols2) = break_cond(where_cond[1])
            if 'AND' in where_cond:
                self.eval_two_cond(where_cond, 'AND', req_tables)
            else:
                self.eval_one_cond(where_cond, req_tables)
        # print "wtf i'm here too"
        if '(' in raw_cols[0]:
            self.is_agg = 1
            col = raw_cols[0]
            spcol = col.split('(')
            fn = spcol[0]
            coln = spcol[1].strip('()')
            (table_which_is_not_needed,col) = self.find_the_col(coln, req_tables)
            # print col
            if fn == 'max':
                self.agg = max(col)
            if fn == 'min':
                self.agg = min(col)
            if fn == 'sum':
                self.agg = sum(col)
            if fn == 'average':
                self.agg = sum(col)/len(col)
        self.display_cols(raw_cols, req_tables)

    def display_cols(self, raw_cols, req_tables):  ############################IMPLEMENT AGGREGATE HERE
        if self.is_agg == 1:
            print raw_cols[0]
            print self.agg
            return
        # print "haha"
        disp_cols = []
        # print "before  for"

        if '*' in raw_cols:
            raw_cols = []
            for r in req_tables:
                col_list = self.db_structure[r].get_cols()
                for col in col_list:
                    cname = r+'.'+col
                    raw_cols.append(cname)
        # print raw_cols
        final_list = []
        for i in raw_cols:
            # print i
            c = self.find_the_col(i, req_tables)
            if '.' not in i:
                final_list.append(c[0].get_name()+'.'+i)
            else:
                final_list.append(i)
            disp_cols.append((c[0].get_name(), c[1]))

        i = 0
        # print self.cart_prod
        # print self.flag_cart_table

        for rc in final_list:
            print rc,',',
        print

        if self.is_distinct == 1:
            # print "ITSSS DIDDDIDDIIINNSSSSSTTTIIINNN"
            all_rows = []
            row = []
            for t in self.cart_prod:
                # print "haha"
                row = []
                if self.flag_cart_table[i] == 1:
                    for j in disp_cols:
                        row.append(int(j[1][t[self.index_of_req_tabs[j[0]]]]))    #" ",t[self.index_of_req_tabs[j[0]]], '\t',
                    if row not in all_rows:
                        all_rows.append(row)
                        for j in row:
                            print j,',',
                        print
                i+=1
            return

        for t in self.cart_prod:
            # print "haha"
            if self.flag_cart_table[i] == 1:
                for j in disp_cols:
                    print int(j[1][t[self.index_of_req_tabs[j[0]]]]),',',    #" ",t[self.index_of_req_tabs[j[0]]], '\t',
                print
            i+=1



    def eval_two_cond(self, where_cond, oa, req_tables):
        where_cond = where_cond.split(oa)
        (op1, cols1) = self.break_cond(where_cond[0])
        # print "Pliss work: ",op1," ",cols1
        (op2, cols2) = self.break_cond(where_cond[1])
        # print "Pliss work: ",op2," ",cols2
        (t1, c1) = self.find_the_col(cols1[0], req_tables)
        # print t1, c1
        n1 = int(cols1[1])
        (t2, c2) = self.find_the_col(cols2[0], req_tables)
        n2 = int(cols2[1])
        # print n1, n2
        if oa == "OR":
            oppa = lambda x,y: x or y
        elif oa == "AND":
            oppa = lambda x,y: x and y
        i = 0
        for t in self.cart_prod:
            if not oppa(op1(int(c1[t[self.index_of_req_tabs[t1.get_name()]]]), n1),op2(int(c2[t[self.index_of_req_tabs[t2.get_name()]]]), n2)):
                # print i
                # print "y"
                # print "cols",c1[t[self.index_of_req_tabs[t1.get_name()]]]," ",n1," ",op1(int(c1[t[self.index_of_req_tabs[t1.get_name()]]]), n1)," ",c2[t[self.index_of_req_tabs[t2.get_name()]]]
                self.flag_cart_table[i] = 0
            i+=1

    def eval_one_cond(self, where_cond, req_tables):
        (op1, cols) = self.break_cond(where_cond)
        if RepresentsInt(cols[1]):
            n1 = int(cols[1])
            (t1, c1) = self.find_the_col(cols[0], req_tables)
            i = 0
            for t in self.cart_prod:
                if not op1(int(c1[t[self.index_of_req_tabs[t1.get_name()]]]), n1):
                    # print i
                    # print "y"
                    # print "cols",c1[t[self.index_of_req_tabs[t1.get_name()]]]," ",n1," ",op1(int(c1[t[self.index_of_req_tabs[t1.get_name()]]]), n1)," ",c2[t[self.index_of_req_tabs[t2.get_name()]]]
                    self.flag_cart_table[i] = 0
                i+=1
        else:
            (t1, c1) = self.find_the_col(cols[0], req_tables)
            (t2, c2) = self.find_the_col(cols[1], req_tables)
            i = 0
            for t in self.cart_prod:
                if not op1(int(c1[t[self.index_of_req_tabs[t1.get_name()]]]), int(c2[t[self.index_of_req_tabs[t2.get_name()]]])):
                    # print i
                    # print "y"
                    # print "cols",c1[t[self.index_of_req_tabs[t1.get_name()]]]," ",n1," ",op1(int(c1[t[self.index_of_req_tabs[t1.get_name()]]]), n1)," ",c2[t[self.index_of_req_tabs[t2.get_name()]]]
                    self.flag_cart_table[i] = 0
                i+=1


    def break_cond(self, cond):
        if '<=' in cond:
            return (lambda x, y: x<=y, cond.split('<='))
        elif '>=' in cond:
            return (lambda x, y: x>=y, cond.split('>='))
        elif '=' in cond:
            return (lambda x, y: x==y, cond.split('='))
        elif '>' in cond:
            return (lambda x, y: x>y, cond.split('>'))
        elif '<' in cond:
            return (lambda x, y: x<y, cond.split('<'))

    #check for aggregate functions
    def find_the_col(self, col, req_tables):
        if '.' in col:
            spcol = col.split('.')
            table = self.db_structure[spcol[0]]
            return (table, table.get_column(spcol[1]))
        else:
            # print "herererererererererre"
            for r in req_tables:
                table = self.db_structure[r]
                cl = table.get_cols()
                if col in cl:
                    return (table, table.get_column(col))
    #check for aggregate functions


class Table:
    def __init__(self, name, file, cols):
        self.file = file
        self.cols = cols
        self.name = name
        self.table_structure = {}
        # data = pd.read_csv(file, header=None)
        # print data
        for c in self.cols:
            self.table_structure[c] = []
        with open(file) as f:
            cf = csv.reader(f)
            for r in cf:
                i = 0
                # print r
                for c in self.cols:
                    self.table_structure[c].append(r[i])
                    i+=1
        self.total_rows = len(self.table_structure[self.cols[0]])





        # for d in data:
        #     # print "+++++++++++++++D++++++ ",self.cols[d]
        #     self.table_structure[self.cols[d]] = data[d]
        #     self.total_rows = len(data[d])
        # # print self.table_structure

    def get_cols(self):
        return self.cols

    def get_column(self, col):
        return self.table_structure[col]

    def get_total_rows(self):
        return self.total_rows

    def get_name(self):
        return self.name



# a = DB('metadata.txt')
