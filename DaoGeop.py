# -*- coding: utf-8 -*-
"""
/***************************************************************************
 *                                                                         *
 *   Classe modelo para conexao ao banco de dados                          *
 *                                                                         *
 ***************************************************************************/
"""

from .conexao_db import Connect



global cur;
cur=Connect.conexao_db(self=None)  #Pega cursor da classe de conexão.

class GeopEspacial:

    def getCampo (self, camada):
        tipo_dado = 'character varying'
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='%s' AND data_type='%s';" %(camada,tipo_dado))
        dados = cur.fetchall()
        return dados
        cur.close()

    def atributo(self, atrib, camada):
        sql = 'SELECT "%s" FROM espacial."%s";' % (atrib, camada)
        cur.execute(sql)
        resul = cur.fetchall()
        return resul
     
    def sinalizacao (self, atrib):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        resul = ''
        if atrib == 'N1':
            sql= "SELECT *FROM espacial.vw_via_n1 ORDER BY 1 "
        elif atrib == 'N2':
            sql= "SELECT *FROM espacial.vw_via_n2 ORDER BY 1"
        elif atrib == 'L2 SUL SENT. S/N':
            sql= "SELECT *FROM espacial.vw_l2sulsn ORDER BY 1"
        elif atrib == 'PARQUE DA CIDADE - ESTAC. 11':
            sql = "SELECT nome FROM relacional.tb_tp_vaga"
        
        else:
            sql= "SELECT nome FROM relacional.tb_tp_vaga"
        if sql != '':   
            cursor.execute(sql)
            resul = cursor.fetchall()
        
        return resul 
      
    # Retorna a chave primaria da tabela
    def chave_pri(self,tab):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        if tab == 'seccionada':
            resultado = 'cod_secc' # Esse algoritmo retorna a chave primaria, mas como na tabela seccionada a chave é o gid, então foi necessario fazer este codigo.
            return resultado           
        else:
            sql = "SELECT  DISTINCT column_name FROM information_schema.constraint_column_usage WHERE table_schema='espacial' AND table_name='%s';"%tab        
       
        cursor.execute(sql)
        resultado = cursor.fetchone()                   
        return ''.join(resultado) 
    
        '''Pegar a quantidade de sinalização'''
    def qtde_relatorio(self,camada,atributo,sinal,cod):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        #sql = "SELECT cod_via FROM espacial.via WHERE nome_via ='N1'"
        
        sql = "SELECT count(gid) as Qtde FROM espacial.%s "%sinal + \
            "INNER JOIN relacional.servico s ON (%s=sinalizacao)"%cod + \
            "INNER JOIN relacional.material m ON (s.material=m.cod_material)" + \
            "WHERE (SELECT f_table_name FROM geometry_columns WHERE f_table_schema = 'espacial' AND f_table_name ='%s') = '%s'"%(camada,camada) + \
            "AND cod_via = (SELECT cod_via FROM espacial.via WHERE nome_via = '%s');"%atributo  
                      
        cursor.execute(sql)
        resultado = cursor.fetchone() 
        for resp in resultado:
            resultado=resp;                 
        return resultado
    
    ''' Pegar a metragem de uma sinalização'''
    def metros_relatorio(self,camada,atributo,sinal,cod):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "SELECT round( sum(comprimento * largura * qtde),2) as Metros FROM espacial.%s "%sinal + \
            "INNER JOIN relacional.servico s ON (%s=sinalizacao)"%cod + \
            "INNER JOIN relacional.material m ON (s.material=m.cod_material)" + \
            "WHERE (SELECT f_table_name FROM geometry_columns WHERE f_table_schema = 'espacial' AND f_table_name ='%s') = '%s'"%(camada,camada) + \
            "AND cod_via = (SELECT cod_via FROM espacial.via WHERE nome_via = '%s');"%atributo      
        cursor.execute(sql)
        resultado = cursor.fetchone() 
        for resp in resultado:
            resultado=resp;                 
        return resultado
        
        '''Pegar o material utilizado no serviço''' 
    def mat_relatorio(self,camada,atributo,sinal,cod):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "SELECT m.descricao as material FROM espacial.%s "%sinal + \
            "NNER JOIN relacional.servico s ON (%s=sinalizacao)"%cod + \
            "INNER JOIN relacional.material m ON (s.material=m.cod_material)" + \
            "WHERE (SELECT f_table_name FROM geometry_columns WHERE f_table_schema = 'espacial' AND f_table_name ='%s') = '%s'"%(camada,camada) + \
            "AND cod_via = (SELECT cod_via FROM espacial.via WHERE nome_via = '%s') GROUP BY descricao;"%atributo        
        cursor.execute(sql) 
        resultado = cursor.fetchone() 
        for resp in resultado:
            resultado=resp;                 
        return resultado
      
    ''' Pegar o valor para revitalizar'''
    def val_relatorio(self,camada,atributo,sinal,cod):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "SELECT round((sum(qtde*comprimento*largura)*m.valor),2) As valor FROM espacial.%s "%sinal + \
            "NNER JOIN relacional.servico s ON (%s=sinalizacao)"%cod + \
            "INNER JOIN relacional.material m ON (s.material=m.cod_material)" + \
            "WHERE (SELECT f_table_name FROM geometry_columns WHERE f_table_schema = 'espacial' AND f_table_name ='%s') = '%s'"%(camada,camada) + \
            "AND cod_via = (SELECT cod_via FROM espacial.via WHERE nome_via = '%s')"%atributo + \
            "GROUP BY valor;"        
        cursor.execute(sql)
        resultado = cursor.fetchone() 
        for resp in resultado:
            resultado=resp;                 
        return resultado
      
    
    def cod_via(self,nome_via):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "SELECT cod_via FROM espacial.via WHERE nome_via = '%s'"%nome_via 
        
        cursor.execute(sql)
        resultado = cursor.fetchone() 
        for resp in resultado:
            resultado=resp;                 
        return resultado
    
    #Exibe o relatorio quando a consulta é para toda a via
    def relatorio_td_via(self,cod,ver):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
         
        if ver == 'met_ex':
            sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver) 
        elif ver == 'met_ho':
            sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver)
        elif ver == 'mat_ex':
            sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver)
        elif ver == 'mat_ho':
            sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver)
        elif ver == 'qtde_ex':
            sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver)
        elif ver == 'qtde_ho':
            sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver)
        elif ver == 'val_ex':
            sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver)
        elif ver == 'val_ho':
            sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver)
        else:
            pass
        #    sql= "SELECT *FROM espacial.func_relatorio_via( '%s','%s');"%(cod,ver)
        
        cursor.execute(sql)
        resultado = cursor.fetchone() 
        for resp in resultado:
            resultado=resp;                 
        return resultado                   
     
    # Retorna consulta de estacionamento
    
    def relat_estac(self,camada,atributo,sinal,verif):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        if verif == 'qtde':
            sql= "SELECT count(cod_vaga) AS qtde FROM espacial.vaga_estac v " +\
                "INNER JOIN relacional.servico s ON (v.cod_estac=s.sinalizacao)" +\
                "INNER JOIN relacional.material m ON (s.material=m.cod_material)" +\
                "WHERE cod_estac=(SELECT cod_estac FROM espacial.estacionamento WHERE endereco ='%s') AND tp_vaga = '%s'"%(atributo,sinal) +\
                "GROUP BY m.descricao,valor"
        elif verif == 'mat':
            sql= "SELECT m.descricao FROM espacial.vaga_estac v " +\
                "INNER JOIN relacional.servico s ON (v.cod_estac=s.sinalizacao)" +\
                "INNER JOIN relacional.material m ON (s.material=m.cod_material)" +\
                "WHERE cod_estac=(SELECT cod_estac FROM espacial.estacionamento WHERE endereco ='%s') AND tp_vaga = '%s'"%(atributo,sinal) +\
                "GROUP BY m.descricao,valor"
        elif verif == 'met':
            sql= "SELECT round(SUM(v.comprimento*v.largura*qtde),2)AS metragem FROM espacial.vaga_estac v " +\
                "INNER JOIN relacional.servico s ON (v.cod_estac=s.sinalizacao)" +\
                "INNER JOIN relacional.material m ON (s.material=m.cod_material)" +\
                "WHERE cod_estac=(SELECT cod_estac FROM espacial.estacionamento WHERE endereco ='%s') AND tp_vaga = '%s'"%(atributo,sinal) +\
                "GROUP BY m.descricao,valor"
        elif verif == 'valor':
            sql= "SELECT round((SUM(v.comprimento*v.largura*qtde)*m.valor),2)As valor FROM espacial.vaga_estac v " +\
                "INNER JOIN relacional.servico s ON (v.cod_estac=s.sinalizacao)" +\
                "INNER JOIN relacional.material m ON (s.material=m.cod_material)" +\
                "WHERE cod_estac=(SELECT cod_estac FROM espacial.estacionamento WHERE endereco ='%s') AND tp_vaga = '%s'"%(atributo,sinal) +\
                "GROUP BY m.descricao,valor"                   
        cursor.execute(sql)
        resultado = cursor.fetchone()
        for resp in resultado:
            resultado=resp;
        return resultado 
    
    # Relatorio para todas as sinalizações
    
    def relat_estac_todas(self,atributo,verif):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        if verif == 'qtde':
            sql= "SELECT count(cod_vaga) AS qtde FROM espacial.vaga_estac v " +\
                "INNER JOIN relacional.servico s ON (v.cod_estac=s.sinalizacao)" +\
                "INNER JOIN relacional.material m ON (s.material=m.cod_material)" +\
                "WHERE cod_estac=(SELECT cod_estac FROM espacial.estacionamento WHERE endereco ='%s') AND tp_vaga in ('Normal','Idoso','Deficiente Fisico')"%(atributo) +\
                "GROUP BY m.descricao,valor"
        elif verif == 'mat':
            sql= "SELECT m.descricao FROM espacial.vaga_estac v " +\
                "INNER JOIN relacional.servico s ON (v.cod_estac=s.sinalizacao)" +\
                "INNER JOIN relacional.material m ON (s.material=m.cod_material)" +\
                "WHERE cod_estac=(SELECT cod_estac FROM espacial.estacionamento WHERE endereco ='%s') AND tp_vaga in ('Normal','Idoso','Deficiente Fisico')"%(atributo) +\
                "GROUP BY m.descricao,valor"
        elif verif == 'met':
            sql= "SELECT round(SUM(v.comprimento*v.largura*qtde),2)AS metragem FROM espacial.vaga_estac v " +\
                "INNER JOIN relacional.servico s ON (v.cod_estac=s.sinalizacao)" +\
                "INNER JOIN relacional.material m ON (s.material=m.cod_material)" +\
                "WHERE cod_estac=(SELECT cod_estac FROM espacial.estacionamento WHERE endereco ='%s') AND tp_vaga in ('Normal','Idoso','Deficiente Fisico')"%(atributo) +\
                "GROUP BY m.descricao,valor"
        elif verif == 'valor':
            sql= "SELECT round((SUM(v.comprimento*v.largura*qtde)*m.valor),2)As valor FROM espacial.vaga_estac v " +\
                "INNER JOIN relacional.servico s ON (v.cod_estac=s.sinalizacao)" +\
                "INNER JOIN relacional.material m ON (s.material=m.cod_material)" +\
                "WHERE cod_estac=(SELECT cod_estac FROM espacial.estacionamento WHERE endereco ='%s') AND tp_vaga in ('Normal','Idoso','Deficiente Fisico')"%(atributo) +\
                "GROUP BY m.descricao,valor"                   
        cursor.execute(sql)
        resultado = cursor.fetchone()
        for resp in resultado:
            resultado=resp;
        return resultado 
        
     
     
    # Retorna informações de uma sinalizaçao.
    '''
    def relat_relac(self,camada,atributo,sinal,cod):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "SELECT  count(gid) as Qtde, m.descricao, round(sum(qtde*comprimento*largura),2) as Metros,round((sum(qtde*comprimento*largura))*m.valor,2) As valor  FROM espacial.%s "%sinal + \
            "NNER JOIN relacional.servico s ON (%s=sinalizacao)"%cod + \
            "INNER JOIN relacional.material m ON (s.material=m.cod_material)" + \
            "WHERE (SELECT f_table_name FROM geometry_columns WHERE f_table_schema = 'espacial' AND f_table_name ='%s') = '%s'"%(camada,camada) + \
            "AND cod_via = (SELECT cod_via FROM espacial.via WHERE nome_via = '%s')"%atributo + \
            "GROUP BY valor,m.descricao;"        
        cursor.execute(sql)
        res = cursor.fetchall()               
        return res
     '''

    # Criar views relacional
    def cria_view(self,cod, layer):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "CREATE OR REPLACE VIEW public.vw_%s AS "%layer +\
              "SELECT *FROM espacial.%s WHERE cod_via='%s';"%(layer,cod)
     
        cursor.execute(sql)
        conexao.commit()
        
    #Adiciona a view criada ao mapa tanto relacional como espacial.      
    def lendo_view(self,view,con):
        if con == 'rel':
            sql = "(select *from public.vw_%s)"%view           
        else:
            sql = "(select *from public.vw_esp_%s)"%view
            
        uri = QgsDataSourceURI()
        uri.setConnection("localhost","5432","bd_geop","postgres","fa050708")
        uri.setDataSource("",sql,"the_geom","","gid")
        vlayer = QgsVectorLayer(uri.uri(),"Layer_temp","postgres")
        QgsMapLayerRegistry.instance().addMapLayer(vlayer)


    '''
    /*************************************************************************
    Os codigos a seguir refere-se a parte espacial do plugin
    ***************************************************************************/ 
    '''
    # Pega as regiões administrativa
    def set_ra(self):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "SELECT nome FROM espacial.ra ORDER BY cod_ra"
        
        cursor.execute(sql)
        resultado = cursor.fetchall()                 
        return resultado
    
    # Pega todas as sinalizações
    def set_td_sinalizacao(self):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "SELECT f_table_name FROM geometry_columns " +\
              "WHERE f_table_schema = 'espacial' AND f_table_name != 'ra' AND f_table_name != 'via' ORDER BY 1 "
        
        cursor.execute(sql)
        resultado = cursor.fetchall()                 
        return resultado
        
    # Relatorio espacial
    def relat_esp(self,ra,sinal,data,ref):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "SELECT DISTINCT fx.cod_fx_pedestre,v.nome_via,fx.endereco,fx.complemto,e.nome FROM espacial.ra r " +\
              "INNER JOIN public.escolas e ON (e.ra=r.nome) " +\
              "INNER JOIN espacial.via v ON (v.cod_ra=r.cod_ra) " +\
              "INNER JOIN espacial.%s fx ON (fx.cod_via=v.cod_via) "%sinal +\
              "INNER JOIN relacional.servico s ON (s.sinalizacao=fx.cod_fx_pedestre)" +\
              "GROUP BY fx.cod_fx_pedestre,v.nome_via,fx.endereco,fx.complemto,e.nome,r.the_geom,e.geom,r.nome " +\
              "HAVING (ST_DWithin (r.the_geom, fx.the_geom,1)) " +\
              "AND (ST_DWithin (fx.the_geom, e.geom,%s))"%ref +\
              "AND r.nome= '%s' "%ra +\
              "AND max(s.dt_garantia) < '%s'; "%data
              
        cursor.execute(sql)
        res = cursor.fetchall()               
        return res
    
    # View para consulta espacial
    def cria_view_esp(self,ra,sinal,data,ref):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
        
        sql = "CREATE OR REPLACE VIEW public.vw_esp_%s AS "%sinal +\
              "SELECT DISTINCT fx.cod_fx_pedestre,v.nome_via,fx.endereco,fx.complemto,e.nome,fx.the_geom,fx.gid FROM espacial.ra r " +\
              "INNER JOIN public.escolas e ON (e.ra=r.nome) " +\
              "INNER JOIN espacial.via v ON (v.cod_ra=r.cod_ra) " +\
              "INNER JOIN espacial.%s fx ON (fx.cod_via=v.cod_via) "%sinal +\
              "INNER JOIN relacional.servico s ON (s.sinalizacao=fx.cod_fx_pedestre)" +\
              "GROUP BY fx.cod_fx_pedestre,v.nome_via,fx.endereco,fx.complemto,e.nome,r.the_geom,e.geom,r.nome " +\
              "HAVING (ST_DWithin (r.the_geom, fx.the_geom,1)) " +\
              "AND (ST_DWithin (fx.the_geom, e.geom,%s))"%ref +\
              "AND r.nome= '%s' "%ra +\
              "AND max(s.dt_garantia) < '%s'; "%data
     
        cursor.execute(sql)
        conexao.commit()
                
    
    def teste (self,sinal,cod,atributo):
        conexao = self.connectBanco()
        cursor = conexao.cursor()
         
        sql= "SELECT count(r.gid) as qtde, descricao, sum(r.comprimento+r.largura+r.qtde),(sum(r.comprimento+r.largura+r.qtde)*valor)as valor FROM relacional.material m" +\
             "INNER JOIN relacional.servico se ON (se.material=cod_material)" +\
             "INNER JOIN espacial.%s r ON (se.sinalizacao=r.%s)" %(sinal,cod)+\
             "INNER JOIN espacial.via v ON (v.cod_via=r.cod_via)" +\
             "WHERE r.cod_via='%s'" %atributo +\
             "GROUP BY descricao,valor;"
             
        cursor.execute(sql)
        res = cursor.fetchall()               
        return res
        