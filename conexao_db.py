# -*- coding: utf-8 -*-
"""
/***************************************************************************
 *   Classe para conexao banco de dados                                                                      *
 ***************************************************************************/
 """

import psycopg2
from psycopg2._psycopg import cursor

class Connect:
    def conexao_db(self):
        try:
            conn = psycopg2.connect("\
                        dbname='prod_geop'\
                        user='postgres'\
                        host='localhost'\
                        password='fabio'\
                ");
        except:
            print("Erro ao se conectar a base de dados!");

        cur = conn.cursor()
        return cur

"""
    if __name__ == '__main__':
        cur = conexao_db(self=None)
        cur.execute('SELECT *FROM relacional.tb_cor')
        rows = cur.fetchall()
        print(cur.rowcount)
        for row in rows:
            print(row)
    cur.close()
"""
