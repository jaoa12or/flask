
#Librerias para webservice_request(nit)
from zeep import Client
import os
import codecs

#Funciones para lectura de XML e
#inserción del mismo en BD

import logging
import pyodbc

import pandas as pd
import numpy as np
#xml
from collections import OrderedDict
import xml.etree.ElementTree as ET
import collections
from lxml import etree
#Permite extraer la fecha para colombia
from datetime import datetime
from pytz import timezone
#Sistema Operativo
import os
import glob

#Exportar a Excel
from pandas import ExcelWriter
from flask import send_file
import xlwt
import xlsxwriter
import math

def validate_route():
	""" Valida las rutas de almacenamoento de los archivos """

	Directorio = os.getcwd() + "\\"
	#PathCarpetaConsultas = Directorio +"Consultas\\"
	PathCarpetaConsultas = "/var/www/html/flask/Consultas/"
	#PathCarpetaConsultas = "C:\\Python_Flask\\envDivisa\\Consultas\\"
	#PathCarpetaResultados = Directorio +"Resultados\\"
	#PathCarpetaResultados = "C:\\Python_Flask\\envDivisa\\Resultados\\"

	if not os.path.isdir(PathCarpetaConsultas):
			os.mkdir(PathCarpetaConsultas)

	#if not os.path.isdir(PathCarpetaResultados):
	#		os.mkdir(PathCarpetaResultados)
	return True


def webservice_request(nit):
	""" Realiza petición al web service de Informa Colombia """
	validate_route()
	Directorio = os.getcwd() + "\\"
	#PathCarpetaConsultas = Directorio +"Consultas\\"
	PathCarpetaConsultas = "/var/www/html/flask/Consultas/"
	#PathCarpetaConsultas = "C:\\Python_Flask\\envDivisa\\Consultas\\"

	#Directorio = os.getcwd() + "\\"
	#print("###### LA RUTA ES: #####  "+Directorio)

	#PathCarpetaConsultas = Directorio +"Consultas\\"


	wsdl = "https://www.informacolombia.com/InformaIntWeb/services/ProductoXML?wsdl"
	client = Client(wsdl)

	stringxml = """
		<PETICION_PRODUCTO>
		<IDENTIFICACION>
		<USUARIO>C16134</USUARIO>
	    <PASSWORD>Infor48925</PASSWORD>
		</IDENTIFICACION>
		<USUARIO_ORIGEN>Equipo_BI</USUARIO_ORIGEN>
		<PRODUCTO>
		<NOMBRE>INFORME_FINANCIERO_INTERNACIONAL_XML</NOMBRE>
		<IDENTIFICADOR>{}</IDENTIFICADOR>
		<IDIOMA>01</IDIOMA>
		</PRODUCTO>
		<PROVINCIA>11</PROVINCIA>
		<LOCALIDAD>BOGOTA</LOCALIDAD>
		<CODPOSTAL>0511</CODPOSTAL>
		</PETICION_PRODUCTO>""".format(nit)

	response = client.service.obtenerProductoXMLUncoded(stringxml)
	print(response)

	print("copiando el archivo-----------")

	if not os.path.exists(PathCarpetaConsultas):
			os.makedirs(PathCarpetaConsultas)


	with codecs.open(PathCarpetaConsultas + nit + ".xml", 'w', encoding='latin-1') as f:
			f.write(response)
	return True

def connectionDB():
	""" Conexión a la BD """
	conn = pyodbc.connect(
          'DRIVER=FreeTDS;SERVER=instancia-divisa-sql.cn7njzxefpfs.us-east-1.rds.amazonaws.com;PORT=1433;DATABASE=Divisa;UID=admin;PWD=admindivisa;')
	cursor = conn.cursor()

	return (conn,cursor)

def connectionDB_DM_Comercial():
	""" Conexión a la BD """
	conn = pyodbc.connect(
			'DRIVER=FreeTDS;SERVER=instancia-divisa-sql.cn7njzxefpfs.us-east-1.rds.amazonaws.com;PORT=1433;DATABASE=DM_Comercial_Divisa;UID=admin;PWD=admindivisa;')
	cursor = conn.cursor()

	return (conn,cursor)

def Descarga_Excel():
	conn,cursor=connectionDB_DM_Comercial()
	#Directorio = os.getcwd() + "\\"
	#PathCarpetaReportes = Directorio +"Archivos_Excel\\Reporte.xls"

	sql="SELECT * FROM vw_Reporte_Pagadores"
	df = pd.read_sql_query(sql, conn)
	#print (df)
	path="/var/www/html/flask/Resultados/Reporte.xls"
	# df.to_excel(path, sheet_name="Reporte_Pagadores", header=True, index=False, float_format="%.2f", engine='xlsxwriter')
	writer = pd.ExcelWriter(path, engine='xlsxwriter')
	df.to_excel(writer,sheet_name="Reporte_Pagadores", header=True, index=False, float_format="%.2f")
	workbook  = writer.book
	worksheet = writer.sheets['Reporte_Pagadores']
	format1 = workbook.add_format({'num_format': '#,##0.00'})
	format2 = workbook.add_format({'num_format': 'dd/mm/yyyy'})
	format3 = workbook.add_format({'num_format': 'dd/mm/yyyy hh:mm AM/PM'})
	worksheet.set_column('E:H', 18, format1)
	worksheet.set_column('N:Q', 18, format2)
	worksheet.set_column('X:X', 18, format3)
	#Alternatively, you could specify a range of columns with 'B:D' and 18 sets the column width
	writer.save()

	cursor.close()
	conn.close()

	return True


def Break_conn(conn,cursor):
	"""Rompe conexiones"""
	cursor.execute(f"""USE [master];
		DECLARE @kill varchar(8000) = '';
		SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), session_id) + ';'
		FROM sys.dm_exec_sessions
		WHERE database_id  = db_id('Divisa')
		EXEC(@kill);""")
	# conn.commit()

################
#Funciones Base #
################
def Validar_Formato_Tabla(df, DicFormatoEntrada):
	"""Funcion que recibe un dataframe y un diccionario,
	   compara el df contra el diccionario y lo acomoda a ese formato,
	   si algun campo del df no existe lo crea con null o si le sobra lo elimina para que los
	   campos tengan estrictamente la estructura de entrada
	"""
	dfOrdenado = pd.DataFrame(columns=DicFormatoEntrada)
	df = Append(df1=dfOrdenado, df2=df)
	df = df[dfOrdenado.columns]
	df = df.astype(object).where(pd.notnull(df),None)
	return df

def Combinar_Celdas(df, DicFormatoEntrada):
	"""Funcion que recibe un dataframe y un diccionario,
	   compara el df contra el diccionario y lo acomoda a ese formato,
	   si algun campo del df no existe lo crea con null o si le sobra lo elimina para que los
	   campos tengan estrictamente la estructura de entrada
	"""
	dfOrdenado = pd.DataFrame(columns=DicFormatoEntrada)
	df = Append(df1=dfOrdenado, df2=df)
	values = {'NOMBRE': "", 'APELLIDO1': "", 'APELLIDO2': ""}
	df.fillna(value=values, inplace=True)
	df['NOMBRE_COMPLETO'] = df[['NOMBRE', 'APELLIDO1', 'APELLIDO2']].agg(' '.join, axis=1)
	df = df[dfOrdenado.columns]
	df = df.astype(object).where(pd.notnull(df),None)
	return df


def ConsultaElemento(root,PathElemento):
	""" Extrae todos los elementos dentro del nivel espefifico """
	#PathCarpetaConsultas = "C:\\Python_Flask\\envDivisa\\Consultas\\"
	#PathXml = PathCarpetaConsultas +"8600259002.xml"
	#tree = ET.parse(PathXml)
	#root = tree.getroot()

	for Elemento in root.findall(PathElemento):
		ElementoTexto = Elemento.text
	return (ElementoTexto)


def Extraer_Label(PathEtiqueta):
	""" extrae la etiqueta que será utilizada para guardar el archivo csv """
	temporal_label = PathEtiqueta.split('/')
	temporal_label.reverse()
	return temporal_label[0]

def Extraer_Dataframe(Directorio,tree,PathDataFrame):
	""" Extrae dataframe en csv. NO se debe agregar "/" al final del path """

	tags = []
	output = []

	for root in tree.findall(PathDataFrame + "/"):
		tags.append(root.tag)

	tag = OrderedDict((x, 1) for x in tags).keys()
	df = pd.DataFrame(columns=tag)

	for root in tree.findall(PathDataFrame):
		data = list(root)
		listado = OrderedDict((content.tag, content.text) for content in data)
		df_table = pd.DataFrame(listado, columns = tag, index=["1"]).dropna(axis=1)
		df = df.append(df_table, ignore_index = True, sort=False)
	Path = Directorio +"_"+ Extraer_Label(PathDataFrame) + ".csv"
	return df

def Extraer_Dataframe_Atributos(Directorio,tree,PathDataFrame,Atributo):
	"""Extrae toda la informacion que la funcion Extraer_Dataframe,
	   pero además al pasarle la lista de atributos, extrae los resultados de los mismos
	"""

	#Directorio="C:\\Python_Flask\\envDivisa\\"
	tags = []
	dicAtributo = {}
	d1 = collections.OrderedDict()
	output = []
	df1 = pd.DataFrame()

	for item in Atributo:
		tags.append(item)

	for root in tree.findall(PathDataFrame + "/"):
		tags.append(root.tag)

	tag = OrderedDict((x, 1) for x in tags).keys()
	df = pd.DataFrame(columns=tag)

	for root in tree.findall(PathDataFrame):
		data = list(root)
		listado = OrderedDict((content.tag, content.text) for content in data)

		for item in Atributo:
			atributo = root.get(item)
			if atributo != None:
				dicAtributo[item]  = atributo
				d1.update(dicAtributo)
				dfatributo = pd.DataFrame(data=dicAtributo, columns = [item], index=["1"])
				dicAtributo = {}
				df1 = pd.concat([df1,dfatributo], axis=1)

		listado.update(d1)
		df_table = pd.DataFrame(listado, columns = tag, index=["1"])
		df = df.append(df_table, ignore_index = True, sort=False)

		df1 = pd.DataFrame()

	df.reset_index(drop=True, inplace=True)
	Path = Directorio +"_"+ Extraer_Label(PathDataFrame) + ".csv"

	return df

def Extraer_Dataframe_Atributos_Iterativo(PathDataFrame,Atributo):
	""" Extrae toda la informacion que la funcion Extraer_Dataframe_Atributos,
		pero pero en vez de traer valores extrae de cada child los mismos atributos de la clase madre"""

	df1 = Extraer_Dataframe_Atributos(Directorio,PathDataFrame,Atributo)
	df1.dropna(axis='columns', how='all', inplace=True)

	for root in tree.findall(PathDataFrame + "/"):
		Pathchild= PathDataFrame + "/"+ root.tag
		df2 = Extraer_Dataframe_Atributos(Directorio,Pathchild, Atributo)
		df2.dropna(axis='columns', how='all', inplace=True)
		df2.columns += "/"+ root.tag
		df1 = Concatenar(df1=df1,df2=df2)
		df1.columns += "/"+ Extraer_Label(PathDataFrame)

	return df1

def Extraer_Dataframe_Todos(PathRaiz):
	""" Extrae todos los dataframes dentro de un directorio y los guarda con csv
		NOTA: busca además los campos nulos asumiendo que son subdominios y extrae tambien esos dataframes
		NOTA: NO ESTA FUNCIONANDO PORQUE RETORNA MULTIPLES DATAFRAMES"""

	Listado_Col = pd.DataFrame(Extraer_Dataframe(Directorio,tree,PathRaiz))
	df = pd.DataFrame()
	for col in Listado_Col:
		if Listado_Col[col].isnull().any():
			ruta = PathRaiz +"/"+ str(col)
			df = Extraer_Dataframe(Directorio,tree,ruta)

	return True


def Extraer_Dataframe_iteracion(PathDataFrame,NombreTag):
	"""" Función para extraer un dataframe con los resultados de una ruta que contiene el mismo nombre para todos los elementos
		 por lo tanto se tiene que iterar la respuesta"""

	tags = {}
	output = []
	df = pd.DataFrame(columns = [NombreTag])
	for root in tree.findall(PathDataFrame + "/"):
		for neighbor in root.iter(NombreTag):
			tags[NombreTag] = neighbor.text
			tagsdf = pd.DataFrame(list(tags.items()))
			df = df.append(tags, ignore_index = True, sort=False)

	df.columns += "/"+ Extraer_Label(PathDataFrame)
	Path = Directorio +"_"+ Extraer_Label(PathDataFrame) + ".csv"
	return df

def Extraer_Dataframe_Dic(Directorio,tree,root,PathDataFrame, Dic):
	""" Extrae un dataframe y seleciona solo las columnas asignadas en el diccionario """
	tags = []
	output = []
	for root in tree.findall(PathDataFrame + "/"):
		tags.append(root.tag)

	tag = OrderedDict((x, 1) for x in tags).keys()
	df = pd.DataFrame(columns=tag)

	for root in tree.findall(PathDataFrame):
		data = list(root)
		listado = OrderedDict((content.tag, content.text) for content in data)
		df_table = pd.DataFrame(listado, columns = tag, index=["1"])
		df = df.append(df_table, ignore_index = True, sort=False)
		df

	df = df.filter(items=Dic)
	df
	df.columns += "/"+ Extraer_Label(PathDataFrame)
	Path = Directorio +"_"+ Extraer_Label(PathDataFrame) + ".csv"
	return df


def Extraer_Dataframe_Dic_Atrib(PathDataFrame, Dic, Atributo):
	""" Extrae un dataframe y seleciona solo las columnas asignadas en el diccionario, para el atributo indicado"""
	#Directorio="C:\\Python_Flask\\envDivisa\\"
	Directorio = os.getcwd() + "\\"
	tags = []
	output = []

	for root in tree.findall(PathDataFrame + "/"):
		Atrib = root.get(Atributo)
		print("Depuracion.......")
		print("atrib: ", root.get(Atributo))
		if Atrib == "1":
			tags.append(root.tag)

	tag = OrderedDict((x, 1) for x in tags).keys()
	df = pd.DataFrame(columns=tag)

	for root in tree.findall(PathDataFrame):
		Atrib = root.get(Atributo)
		print(Atrib)
		if Atrib == "1":
			data = list(root)
			listado = OrderedDict((content.tag, content.text) for content in data)
			df_table = pd.DataFrame(listado, columns = tag, index=["1"])
			df = df.append(df_table, ignore_index = True, sort=False)
			df

	df = df.filter(items=Dic)
	df.columns += "/"+ Extraer_Label(PathDataFrame)
	Path = Directorio +"_"+ Extraer_Label(PathDataFrame) + ".csv"
	return df

def Completar_Espacios(df):
	""" Rellena los espacios con el dato anterior """
	df = df.fillna(method='ffill')
	return df

#para ejecutar usar el siguiente metodo-> Concatenar(df1=nombredf1, df2=nombredf2, ... , dfn=nombredfn)
def Concatenar(**kwargs):
	""" Recibe los nombres de los dataframes que se quieren concatenar """
	dfs = list(kwargs.values())
	mergedf = pd.concat(dfs, sort=False, axis=1)
	return mergedf

#para ejecutar usar el siguiente metodo-> Append(df1=nombredf1, df2=nombredf2, ... , dfn=nombredfn)
def Append(**kwargs):
	""" Recibe los nombres de los dataframes que se quieren añadir uno bajo el otro"""

	appenddf = pd.DataFrame()
	dfs = list(kwargs.values())
	appenddf = appenddf.append(dfs, sort=False)
	appenddf.reset_index(drop=True, inplace=True)
	appenddf = appenddf.astype(object).where(pd.notnull(appenddf),None)
	return appenddf

def Eliminar_Columnas(df, Dic):
	""" Recibe el dataframe y el listado de las columnas que se quieren eliminar
 	"""
	df = df.drop(columns=Dic)
	return df

def Convertir_Numeros(df, Dic):
	""" Limpia los numeros del dataframe """

	df = pd.DataFrame(df)
	for item in Dic:
		#print("LISTADO:", item)
		df[item] = df[item].str.replace('.','')
		df[item] = df[item].str.replace(',','.', regex=True)
		df[item] = df[item].astype(float)
	return df

def Extraer_Dataframe_Evolucion(PathDataFrame, Atributo):
	""" extraer un df con la evolucion anual, el atributo "YEAR" es un parametro de la etiqueta
  	"""
	col1 = Extraer_Label(PathDataFrame)
	col2 = col1 + "_" + Atributo
	Dic = [col1, col2]

	df = pd.DataFrame(columns=Dic)
	for root in tree.findall(PathDataFrame):
		AnnoActual = datetime.date.today().year
		Anno = root.get(Atributo)
		#print(int(Anno))

		if int(Anno)>=(AnnoActual-3):
			lista = [(root.text, Anno)]
			df1 = pd.DataFrame(lista, columns = Dic, index=["1"])
			df = df.append(df1, ignore_index = True, sort=False)
    #df.columns += "/"+ Extraer_Label(PathDataFrame)
	return	df

def Combinar_Registros(df, PathDataFrame):
	""" Combina todos los registros del dataframe en una sola columna con el nombre del path"""

	nombrecol = str(Extraer_Label(PathDataFrame))
	df = df.astype(str)
	df2 = pd.DataFrame(columns=[nombrecol])
	listcol = df.columns.values.tolist()
	df2[nombrecol] = df[listcol].apply(lambda x: ' '.join(x), axis = 1)
	return df2

def Extraer_Dataframe_Evolucion_sin_Atrib(PathDataFrame):

	col1 = Extraer_Label(PathDataFrame)
	Dic = [col1]

	df = pd.DataFrame(columns=Dic)
	for root in tree.findall(PathDataFrame):
		lista = [(root.text)]
		df1 = pd.DataFrame(lista, columns = Dic, index=["1"])
		df = df.append(df1, ignore_index = True, sort=False)
	#print("DEPURACION FUNCION EVOLSIN ATRIBUTO ##########################################")
	#print(df)

	return	df

def Extraer_Dataframe_Subtipo(PathInfoFinan):
    df = pd.DataFrame(columns=['SUBTIPO','TIPO'])
    for content in tree.findall(PathInfoFinan+"/"):
        listado = {'SUBTIPO':  content.tag, 'TIPO': content.get("SUBTIPO")}
        df1 = pd.DataFrame(listado, columns = ['SUBTIPO','TIPO'], index=["1"])
        #print(df)
        df = pd.concat([df,df1]).drop_duplicates().reset_index(drop=True)
    df.columns += "/"+ Extraer_Label(PathInfoFinan)
    return  df

def Extraer_Dataframe_ActivoCorriente(PathActivoCorriente):
    temp = []
    dictlist = []
    df = pd.DataFrame()
    for content in tree.findall(PathActivoCorriente+"/"):
        nombres = content.tag
        #print(nombres)
        listado = content.attrib
        #print(listado)
        for key, value in listado.items():
            temp = [key,value]
            dictlist.append(temp)
            #print("listado")
            #print(dictlist)

        #df = pd.DataFrame(listado, columns = [content.tag])

        df1 = pd.DataFrame(columns=[content.tag + "_VALOR", content.tag], data=dictlist)
        dictlist = []
        df = Concatenar(df0=df, df1=df1)
    df.columns += "/"+ Extraer_Label(PathActivoCorriente)

    return df

def Guardar_csv(df, PathCarpeta, NombreArchivo):
    df = df
    #if not os.path.exists(PathCarpetaResultados):
     #   os.makedirs(PathCarpetaResultados)
      #  print("Carpeta Creada")

    #Dataframe que se debe rellenar con la ultima fila
    df.to_csv(PathCarpeta + NombreArchivo, index = False, encoding='utf-8-sig')
    df
    return print("Archivo creado ", PathCarpeta + NombreArchivo)

def Extraer_Dataframe_1Atributo(tree,PathDataFrame, Atributo):
    """ Extraer de una ruta, todos los nombres y los valores de 1 atributo y crear
		un dataframe con estos datos en columnas y filas respectivamente
 	"""
    Diccionario = {}
    columnas = []
    for root in tree.findall(PathDataFrame+"/"):

        Atrib = root.get(Atributo)
        if Atrib != None:
            columnas.append(root.tag)
            #print(columnas)
            Diccionario[root.tag] = Atrib
    df = pd.DataFrame(Diccionario, index=[0])
    df.columns += "/"+ Extraer_Label(PathDataFrame)
    return  df


def Dict_to_Df_Financiero(Dictodf):
    try:
        df = pd.DataFrame([Dictodf["VALOR"]], columns = [Dictodf["DESC"]])
        df.reset_index(drop=True, inplace=True)
        df
    except:
        df = pd.DataFrame([np.nan], columns=["SIN ATRIBUTO"])
        #print("@@@@ERROR@@@@@:Campo sin Atributo Financiero")
        #try:
        #    print("VALOR: ", Dictodf["VALOR"])
        #except:
        #    print()
    return df


def Financiero_Activos(Fecha_Captura,Id_Cliente,tree,PathBalancesPrio, subbalance, Id_Activo, Id_Info_Financiera):
    """	Descarga los dataframes de balance de activos especificos de partidas que se necesiten por ejemplo si se pasa el argumento
    	AC entonces se extrae para cada año toda la informacion dentro de este dominio (ACC y ACL respectivamente para todos los años)"""
    Años = tree.findall(PathBalancesPrio)
    #print("$$$$$$$$$$$$" + Años + "$$$$$$$$$")
    if Años:
    	#print("lleno")
    	df = pd.DataFrame()
    	i = 0
    	for Año in Años:
    		#print(Año)
    		tags = []
    		try:
    			dfNormaContable = pd.DataFrame([Año.attrib['NIIF']], columns=["NIIF"])
    		except:
    			dfNormaContable = pd.DataFrame(["0"], columns=["NIIF"])
    			print("No NIFF")
    		i = i +1
    		for dato in Año:
    			tags.append(dato.tag)

    		dfFinanciero = pd.DataFrame(columns=tags)
    		listado = OrderedDict((dato.tag, dato.text) for dato in Año)
    		df_table = pd.DataFrame(listado, columns = tags, index=["1"])
    		dfFinanciero = dfFinanciero.append(df_table, ignore_index = True, sort=False)

    		try:
    			del dfFinanciero['PARTIDAS']
    		except:
    			print("Sin Columna PARTIDAS")

    		dfFinanciero = Concatenar(df1 = dfNormaContable, df2 = dfFinanciero)
    		dfFinanciero["Id_Cliente"]= Id_Cliente
    		dfFinanciero["Fecha_Captura"]= Fecha_Captura
    		dicFinanciero = ['NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente', 'Id_Cliente', 'Fecha_Captura']
    		dfFinanciero.columns = dicFinanciero
    		dfFinanciero = dfFinanciero[['Id_Cliente', 'Fecha_Captura', 'NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente']]
    		try:
    			partidas = Año.find(".//PARTIDAS")
    			dicpartidas = partidas[0].attrib
    		except:
    			dicpartidas = {'DESC': np.nan, 'VALOR': np.nan}

    		dfpartidas  = Dict_to_Df_Financiero(dicpartidas)

    		dfactivos = Concatenar(df1= dfFinanciero, df2=dfpartidas)
    		try:
    			for partida in partidas.findall('.//' + subbalance):
    				dicpartida = partida[0].attrib
    				dfpartida = Dict_to_Df_Financiero(dicpartida)
    				for child in partida.findall('./'):
    					dicchild = partida.find(child.tag).attrib
    					dfchild = Dict_to_Df_Financiero(dicchild)
    					dfchild.columns += "/" + child.tag
    					dfactivos = Concatenar(df1=dfactivos, df2=dfchild)
    					for part in child:
    						dicpart = part.attrib
    						dfpart = Dict_to_Df_Financiero(dicpart)
    						dfpart.columns += "/" + child.tag
    						dfpart
    						dfactivos = Concatenar(df1=dfactivos, df2=dfpart)
    		except:
    			dfactivos = pd.DataFrame()

    		if i == 1:
    			df1 = dfactivos
    			#print("depurando fi=1")
    			print(list(df1.columns))
    		else:
    			#print("depurando fi=2")
    			#print(list(df1.columns))
    			#print(list(dfactivos.columns))
    			dicdf = ['Id_Cliente', 'Fecha_Captura', 'NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente', 'TOTAL ACTIVO', 'TOTAL ACTIVO CORRIENTE/ACC', 'CUENTAS POR COBRAR - DEUDORES/ACC', 'INVENTARIOS/ACC', 'OTROS ACTIVOS/ACC', 'OTROS ACTIVOS NO FINANCIEROS/ACC', 'ACTIVOS POR IMPUESTOS CORRIENTES/ACC', 'EFECTIVO Y EQUIVALENTES AL EFECTIVO/ACC', 'SIN ATRIBUTO/ACC', 'CUENTAS COMERCIALES POR COBRAR Y OTRAS C/ACC', 'TOTAL ACTIVO NO CORRIENTE/ACL', 'INVERSIONES/ACL', 'INVERSIONES CONTABILIZADAS UTILIZANDO EL/ACL', 'INVERSIONES EN SUBSIDIARIAS, NEGOCIOS CO/ACL', 'PROPIEDADES PLANTA Y EQUIPO/ACL', 'PROPIEDAD DE INVERSIÓN/ACL', 'ACTIVOS INTANGIBLES DISTINTOS DE LA PLUS/ACL', 'INVERSIONES NO CORRIENTES/ACL', 'CUENTAS POR COBRAR NO CORRIENTES/ACL', 'CUENTAS COMERCIALES POR COBRAR Y OTRAS C/ACL']

    			try:
    				dfactivos = Validar_Formato_Tabla(dfactivos, dicdf)
    				df1 = df1.set_index('Fecha_Efecto').combine_first(dfactivos.set_index('Fecha_Efecto')).reset_index()
    			except:
    				dfactivos = pd.DataFrame(columns=dicdf)
    				dfactivos = Validar_Formato_Tabla(dfactivos, dicdf)
    				print("error en df Activo")

    		df = Append(df1=df, df2=dfFinanciero)
    		df1["Id_Activo"] = Id_Activo
    		df1["Id_Info_Financiera"] = Id_Info_Financiera
    		DicActivosOrdenado = ['Id_Info_Financiera', 'Fecha_Efecto', 'TOTAL ACTIVO', 'TOTAL ACTIVO CORRIENTE/ACC', 'CUENTAS POR COBRAR - DEUDORES/ACC', 'INVENTARIOS/ACC', 'DIFERIDOS/ACC', 'GASTOS PAGADOS POR ANTICIPADO/ACC', 'OTROS ACTIVOS/ACC', 'OTROS ACTIVOS FINANCIEROS/ACC', 'OTROS ACTIVOS NO FINANCIEROS/ACC', 'ACTIVOS POR IMPUESTOS CORRIENTES/ACC', 'ACTIVOS CLASIFICADOS COMO MANTENIDOS PAR/ACC', 'EFECTIVO Y EQUIVALENTES AL EFECTIVO/ACC', 'CUENTAS COMERCIALES POR COBRAR Y OTRAS C/ACC', 'CUENTAS POR COBRAR PARTES RELACIONADAS Y/ACC', 'TOTAL ACTIVO NO CORRIENTE/ACL', 'INVERSIONES/ACL', 'INVERSIONES EN SUBSIDIARIAS, NEGOCIOS CO/ACL', 'INVERSIONES CONTABILIZADAS UTILIZANDO EL/ACL', 'PROPIEDADES PLANTA Y EQUIPO/ACL', 'DIFERIDOS/ACL', 'GASTOS PAGADOS POR ANTICIPADO/ACL', 'OTROS ACTIVOS/ACL', 'PROPIEDAD DE INVERSIÓN/ACL', 'PLUSVALÍA/ACL', 'ACTIVOS INTANGIBLES DISTINTOS DE LA PLUS/ACL', 'ACTIVOS POR IMPUESTOS DIFERIDOS/ACL', 'INVERSIONES NO CORRIENTES/ACL', 'CUENTAS POR COBRAR NO CORRIENTES/ACL', 'CUENTAS COMERCIALES POR COBRAR Y OTRAS C/ACL', 'CUENTAS POR COBRAR PARTES RELACIONADAS Y/ACL', 'OTROS ACTIVOS NO FINANCIEROS/ACL', 'OTROS ACTIVOS FINANCIEROS/ACL', 'Fecha_Captura']
    		df1 = Validar_Formato_Tabla(df1,DicActivosOrdenado)
    	return df1

    else:
    	DicActivosOrdenado = ['Id_Info_Financiera', 'Fecha_Efecto', 'TOTAL ACTIVO', 'TOTAL ACTIVO CORRIENTE/ACC', 'CUENTAS POR COBRAR - DEUDORES/ACC', 'INVENTARIOS/ACC', 'DIFERIDOS/ACC', 'GASTOS PAGADOS POR ANTICIPADO/ACC', 'OTROS ACTIVOS/ACC', 'OTROS ACTIVOS FINANCIEROS/ACC', 'OTROS ACTIVOS NO FINANCIEROS/ACC', 'ACTIVOS POR IMPUESTOS CORRIENTES/ACC', 'ACTIVOS CLASIFICADOS COMO MANTENIDOS PAR/ACC', 'EFECTIVO Y EQUIVALENTES AL EFECTIVO/ACC', 'CUENTAS COMERCIALES POR COBRAR Y OTRAS C/ACC', 'CUENTAS POR COBRAR PARTES RELACIONADAS Y/ACC', 'TOTAL ACTIVO NO CORRIENTE/ACL', 'INVERSIONES/ACL', 'INVERSIONES EN SUBSIDIARIAS, NEGOCIOS CO/ACL', 'INVERSIONES CONTABILIZADAS UTILIZANDO EL/ACL', 'PROPIEDADES PLANTA Y EQUIPO/ACL', 'DIFERIDOS/ACL', 'GASTOS PAGADOS POR ANTICIPADO/ACL', 'OTROS ACTIVOS/ACL', 'PROPIEDAD DE INVERSIÓN/ACL', 'PLUSVALÍA/ACL', 'ACTIVOS INTANGIBLES DISTINTOS DE LA PLUS/ACL', 'ACTIVOS POR IMPUESTOS DIFERIDOS/ACL', 'INVERSIONES NO CORRIENTES/ACL', 'CUENTAS POR COBRAR NO CORRIENTES/ACL', 'CUENTAS COMERCIALES POR COBRAR Y OTRAS C/ACL', 'CUENTAS POR COBRAR PARTES RELACIONADAS Y/ACL', 'OTROS ACTIVOS NO FINANCIEROS/ACL', 'OTROS ACTIVOS FINANCIEROS/ACL', 'Fecha_Captura']
    	return pd.DataFrame(columns=DicActivosOrdenado)

def Financiero_Pasivos_Patrimonio(tree,Id_Cliente,Fecha_Captura,PathBalancesPrio, subbalance, Id_PasivoPatrimonio, Id_Info_Financiera):
    Años = tree.findall(PathBalancesPrio)
    try:
        df = pd.DataFrame()
        i = 0
        for Año in Años:
            tags = []
            #print ('_________________________Prioritario Año:', Año.attrib['EJERCICIO'], "NIIF: ", Año.attrib['NIIF'])
            try:
                dfNormaContable = pd.DataFrame([Año.attrib['NIIF']], columns=["NIIF"])
            except:
                dfNormaContable = pd.DataFrame(["0"], columns=["NIIF"])
            i = i +1
            for dato in Año:
                tags.append(dato.tag)

            dfFinanciero = pd.DataFrame(columns=tags)

            listado = OrderedDict((dato.tag, dato.text) for dato in Año)
            df_table = pd.DataFrame(listado, columns = tags, index=["1"])
            dfFinanciero = dfFinanciero.append(df_table, ignore_index = True, sort=False)
            del dfFinanciero['PARTIDAS']

            dfFinanciero = Concatenar(df1 = dfNormaContable, df2 = dfFinanciero)
            dfFinanciero["Id_Cliente"]= Id_Cliente
            dfFinanciero["Fecha_Captura"]= Fecha_Captura
            dicFinanciero = ['NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente', 'Id_Cliente', 'Fecha_Captura']
            dfFinanciero.columns = dicFinanciero
            dfFinanciero = dfFinanciero[['Id_Cliente', 'Fecha_Captura', 'NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente']]

            partidas = Año.find(".//PARTIDAS")
            dicpartidas = partidas[0].attrib
            dfpartidas  = Dict_to_Df_Financiero(dicpartidas)
            dfactivos = Concatenar(df1= dfFinanciero, df2=dfpartidas)
            for partida in partidas.findall('.//' + subbalance):
                #print("Nivel 1________________")
                dicpartida = partida.attrib
                #PASIVO y PATRIMONIO
                dicpartida = partidas.find(partida.tag).attrib
                dfpartida = Dict_to_Df_Financiero(dicpartida)
                #print(dfpartida)
                dfactivos = Concatenar(df1=dfactivos, df2=dfpartida)
                for child in partida.findall('./'):
                    #print("Nivel 2___________")
                    #print(child.tag)
                    dicchild = partida.find(child.tag).attrib
                    #print(child.tag)
                    dfchild = Dict_to_Df_Financiero(dicchild)
                    dfchild.columns += "/" + child.tag
                    #print(dfchild)
                    dfactivos = Concatenar(df1=dfactivos, df2=dfchild)

                    for childsub in child.findall('./'):
                        #print("Nivel 3 ___________________")
                        #print("tag", childsub.tag)
                        dicchildsub = child.find(childsub.tag).attrib
                        dfchildsub = Dict_to_Df_Financiero(dicchildsub)
                        if child.tag == "PT":
                            dfchildsub.columns += "/" + child.tag
                        else:
                            dfchildsub.columns += "/" + child.tag +"/" + childsub.tag
                        #print(dfchildsub)
                        dfactivos = Concatenar(df1=dfactivos, df2=dfchildsub)
                        #for part in childsub:
                        #    dicpart = part.attrib
                        #    dfpart = Dict_to_Df_Financiero(dicpart)
                        #    dfpart.columns += "/" + childsub.tag
                        #    dfpart
                        for part in childsub:
                            #print("Nivel 4 ___________________")
                            dicpart = childsub.find(part.tag).attrib
                            dfpart = Dict_to_Df_Financiero(dicpart)
                            dfpart.columns += "/" + child.tag +"/" + childsub.tag
                            dfpart
                            #print(dfpart)
                            dfactivos = Concatenar(df1=dfactivos, df2=dfpart)
                            #print(dfactivos)

            if i == 1:
                df1 = dfactivos

            else:

                df1 = df1.set_index('Fecha_Efecto').combine_first(dfactivos.set_index('Fecha_Efecto')).reset_index()
                #print(df1["Fecha_Efecto"])
            df = Append(df1=df, df2=dfFinanciero)

            df1["Id_PasivoPatrimonio"] = Id_PasivoPatrimonio
            df1["Id_Info_Financiera"] = Id_Info_Financiera
            #print(df.to_string())
        return df1
    except:
        DicPasivosPatrimonioOrdenado = ['Id_PasivoPatrimonio', 'Id_Info_Financiera', 'Fecha_Efecto', 'PASIVO + PATRIMONIO', 'PASIVO/PS', 'PASIVO A CORTO PLAZO/PS/PSC', 'OBLIGACIONES FINANCIERAS/PS/PSC', 'PASIVOS ESTIMADOS Y PROVISIONES/PS/PSC', 'PROVISIONES DIVERSAS/PS/PSC', 'OTROS PASIVOS FINANCIEROS/PS/PSC', 'OTROS PASIVOS NO FINANCIEROS/PS/PSC', 'CUENTAS POR PAGAR CORRIENTE/PS/PSC', 'CUENTAS COMERCIALES POR PAGAR Y OTRAS CU/PS/PSC', 'CUENTAS POR PAGAR A ENTIDADES RELACIONAD/PS/PSC', 'PASIVOS POR IMPUESTOS CORRIENTES/PS/PSC', 'PROVISIONES CORRIENTES POR BENEFICIOS A/PS/PSC', 'OTROS PASIVOS CORRIENTES/PS/PSC', 'PASIVO A LARGO PLAZO/PS/PSL', 'PASIVOS ESTIMADOS Y PROVISIONES/PS/PSL', 'OTROS PASIVOS FINANCIEROS/PS/PSL', 'OTROS PASIVOS NO FINANCIEROS/PS/PSL', 'PASIVO POR IMPUESTOS DIFERIDOS/PS/PSL', 'OBLIGACIONES FINANCIEROS NO CORRIENTES/PS/PSL', 'PROVISIONES NO CORRIENTES POR BENEFICIOS/PS/PSL', 'OTRAS PROVISIONES/PS/PSL', 'OTROS PASIVOS NO CORRIENTES/PS/PSL', 'PATRIMONIO/PT', 'CAPITAL SOCIAL/PT', 'SUPERµVIT DE CAPITAL/PT', 'RESERVAS/PT', 'RESULTADO EJERCICIO/PT', 'COTIZACIONES-AUXIL./APORTES NO VINC./PT','OTROS RUBROS DEL PATRIMONIO/PT', 'ACCIONES PROPIAS EN CARTERA/PT', 'OTRO RESULTADO INTEGRAL ACUMULADO/PT', 'OTRAS PARTICIPACIONES EN EL PATRIMONIO/PT', 'PRIMA DE EMISIÓN/PT', 'GANANCIAS ACUMULADAS/PT', 'CAPITAL EMITIDO/PT', 'Fecha_Captura']
        return pd.DataFrame(columns=DicPasivosPatrimonioOrdenado)

def Financiero_Resultados(tree,Id_Cliente,Fecha_Captura,PathBalancesPrio, subbalance, Id_Result_Ejercicio, Id_Info_Financiera):
        Años = tree.findall(PathBalancesPrio)
        try:
            df = pd.DataFrame()
            i = 0
            for Año in Años:
                tags = []
                #print ('Prioritario Año____________________________________:', Año.attrib['EJERCICIO'], "NIIF: ", Año.attrib['NIIF'])
                try:
                    dfNormaContable = pd.DataFrame([Año.attrib['NIIF']], columns=["NIIF"])
                except:
                    dfNormaContable = pd.DataFrame(["0"], columns=["NIIF"])
                i = i +1
                for dato in Año:

                    tags.append(dato.tag)

                dfFinanciero = pd.DataFrame(columns=tags)

                listado = OrderedDict((dato.tag, dato.text) for dato in Año)
                df_table = pd.DataFrame(listado, columns = tags, index=["1"])
                dfFinanciero = dfFinanciero.append(df_table, ignore_index = True, sort=False)
                dicFinanciero = ['FEC_CIERRE', 'DURACION', 'COD_DIVISA', 'DESC_FUENTE', 'PARTIDAS']
                dfFinanciero = Validar_Formato_Tabla(dfFinanciero, dicFinanciero)
                del dfFinanciero['PARTIDAS']

                dfFinanciero = Concatenar(df1 = dfNormaContable, df2 = dfFinanciero)
                dfFinanciero["Id_Cliente"]= Id_Cliente
                dfFinanciero["Fecha_Captura"]= Fecha_Captura
                dicFinanciero = ['NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente', 'Id_Cliente', 'Fecha_Captura']
                dfFinanciero.columns = dicFinanciero
                dfFinanciero = dfFinanciero[['Id_Cliente', 'Fecha_Captura', 'NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente']]

                partidas = Año.find(".//PARTIDAS")
                dfactivos = dfFinanciero

                for partida in partidas.findall('.//' + subbalance):
                    dicpartida = partida.attrib
                    dfpartida = Dict_to_Df_Financiero(dicpartida)
                    dfactivos = Concatenar(df1=dfactivos, df2=dfpartida)

                    for child in partida:
                        dicchild = child.attrib
                        dfchild = Dict_to_Df_Financiero(dicchild)
                        dfchild.columns += "/" + partida.tag
                        dfactivos = Concatenar(df1=dfactivos, df2=dfchild)

                if i == 1:
                    df1 = dfactivos
                    #print(df1)
                else:
                    df1 = df1.set_index('Fecha_Efecto').combine_first(dfactivos.set_index('Fecha_Efecto')).reset_index()


                #print(df1)
                df = Append(df1=df, df2=dfFinanciero)
                df1["Id_Result_Ejercicio"] = Id_Result_Ejercicio
                df1["Id_Info_Financiera"] = Id_Info_Financiera
                #print(df.columns)
            return df1
        except:
            DicResultadosOrdenado = ['Id_Info_Financiera', 'Fecha_Efecto', 'RESULTADO DEL EJERCICIO', 'RESULTADO ANTES DE IMPUESTOS/R', 'RESULTADOS OPERACIONALES/R', 'TOTAL GASTOS/R', 'COSTOS Y GASTOS OPERACIONALES/R', 'GASTOS DE ADMINISTRACION/R', 'GASTOS DE VENTAS/R','GASTOS DE DISTRIBUCIÓN/R', 'GASTOS POR BENEFICIOS A LOS EMPLEADOS/R', 'OTROS GASTOS OPERATIVOS/R', 'COSTO DE VENTAS/R', 'NO OPERACIONALES/R', 'GASTOS FINANCIEROS/R', 'TOTAL INGRESOS/R', 'INGRESOS OPERACIONALES/R', 'VENTAS/R', 'OTROS INGRESOS OPERACIONALES/R', 'INGRESOS NO OPERACIONALES/R', 'INGRESOS EXTRAORDINARIOS/R', 'INGRESOS FINANCIEROS/R', 'RESULTADO NO OPERACIONAL/R', 'RESULTADO FINANCIERO/R', 'RESULTADO DE IMPUESTOS/R', 'AJUSTES POR INFLACIàN/R', 'IMPUESTO DE RENTA Y COMPLEMENTARIOS/R', 'Fecha_Captura']
            return pd.DataFrame(columns=DicResultadosOrdenado)

def FinancieroEncabezados(Fecha_Captura,tree,PathBalancesPrio, NIT):
        Años = tree.findall(PathBalancesPrio)
        df = pd.DataFrame()
        i = 0
        for Año in Años:
            tags = []
            #print(Año.attrib)
            #print ('Prioritario Año:', Año.attrib['EJERCICIO'], "NIIF: ", Año.tag[0])
            try:
                dfNormaContable = pd.DataFrame([Año.attrib['NIIF']], columns=["NIIF"])
            except:
                dfNormaContable = pd.DataFrame(["0"], columns=["NIIF"])
            i = i +1
            for dato in Año:
                tags.append(dato.tag)

            dfFinanciero = pd.DataFrame(columns=tags)
            dicFinanciero = ['FEC_CIERRE', 'DURACION', 'COD_DIVISA', 'DESC_FUENTE', 'PARTIDAS']
            dfFinanciero = Validar_Formato_Tabla(dfFinanciero, dicFinanciero)
            listado = OrderedDict((dato.tag, dato.text) for dato in Año)
            df_table = pd.DataFrame(listado, columns = tags, index=["1"])
            dfFinanciero = dfFinanciero.append(df_table, ignore_index = True, sort=False)
            del dfFinanciero['PARTIDAS']

            dfFinanciero = Concatenar(df1 = dfNormaContable, df2 = dfFinanciero)
            dfFinanciero["Nit_Cliente"]= NIT
            dfFinanciero["Fecha_Captura"]= Fecha_Captura
            dicFinanciero = ['NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente', 'Nit_Cliente', 'Fecha_Captura']
            dfFinanciero.columns = dicFinanciero

            dfFinanciero = dfFinanciero[['Nit_Cliente', 'Fecha_Captura', 'NormaContable', 'Fecha_Efecto', 'Duracion', 'Unidades', 'Fuente']]
            df = Append(df1=df, df2=dfFinanciero)
            #df["Id_Info_Financiera"] = Id
        return df

#Actividad_Exterior(tree,path de actividad comercial externa, escoger si es importa o exporta)
def Actividad_Exterior(tree,PathActividad, Actividad):
    Años = tree.findall(PathActividad)
    df = pd.DataFrame()
    i = 0
    for Año in Años:
        tags = []
        texto = []
        #print(Año.text)
        for dato in Año:
            tags.append(dato.tag)
            texto.append(dato.text)
        dictionary = dict(zip(tags, texto))
        dfActividad = pd.DataFrame.from_dict(dictionary, orient = 'index').T
        dicActividad = ["ANYO"]
        dfActividad = Validar_Formato_Tabla(dfActividad, dicActividad)
        #df = Append(df1=df, df2=dfActividad)
        #print(dfActividad)
        for ActivExt in Año.findall('.//' + Actividad):
            #print(ActivExt)
            tags2 = []
            texto2 = []
            for child in ActivExt:
                tags2.append(child.tag)
                texto2.append(child.text)
            dictionary = dict(zip(tags2, texto2))
            dfActividadExt = pd.DataFrame.from_dict(dictionary, orient = 'index').T
            dicActividadExt = ["FEC_CAMBIO","PRODUCTOS","IMPORTE","DIVISA"]
            dfActividadExt = Validar_Formato_Tabla(dfActividadExt, dicActividadExt)
            dfActividad = Concatenar(df1=dfActividad, df2=dfActividadExt)
            #print(dfActividad.to_string())
            tags3 = []
            texto3 = []
            for paises in ActivExt.findall('.//' + "PAISES"):
                dfActividadpais = pd.DataFrame()

                for pais in paises:
                    #print(pais.tag)
                    #print(pais.text)
                    tags3.append(pais.tag)
                    texto3.append(pais.text)

                    dictionary = dict(zip(tags3, texto3))
                    df2 = pd.DataFrame.from_dict(dictionary, orient = 'index').T
                    dfActividadpais = Append(df1=dfActividadpais, df2=df2)
                    dicActividadpais = ["DESC_PAIS"]
                    dfActividadpais = Validar_Formato_Tabla(dfActividadpais, dicActividadpais)
                #print(dfActividadpais)

                dfActividad = Concatenar(df1=dfActividad, df2=dfActividadpais)
                dfActividad = Completar_Espacios(dfActividad)
        df = Append(df1= df,df2=dfActividad)
        dicActivExt = ['ANYO', 'FEC_CAMBIO', 'PRODUCTOS', 'IMPORTE', 'DIVISA', 'DESC_PAIS']
        df = Validar_Formato_Tabla(df,dicActivExt)
    return df

def Extraer_Dataframe_Actividades(tree,PathDataFrame, Dic):
 	""" Extrae todas las actividades comerciales de la empresa SEA PRIMARIA O SECUNDARIA"""
 	"""Extrae un dataframe y seleciona solo las columnas asignadas en el diccionario"""
 	tags = []
 	output = []

 	for root in tree.findall(PathDataFrame + "/ACTIVIDAD/"):
 		tags.append(root.tag)

 	tag = OrderedDict((x, 1) for x in tags).keys()
 	df = pd.DataFrame(columns=tag)

 	df_tipo = pd.DataFrame(columns=['Tipo_Actividad'])
 	for root in tree.findall(PathDataFrame + "/"):
 		for name in root.attrib:
 			tipo_actividad = name

 		df_tipo = df_tipo.append({'Tipo_Actividad': name}, ignore_index=True)

 	df_tipo

 	for root in tree.findall(PathDataFrame + "/ACTIVIDAD"):
 		data = list(root)
 		listado = OrderedDict((content.tag, content.text) for content in data)
 		df_table = pd.DataFrame(listado, columns = tag, index=["1"])
 		df = df.append(df_table, ignore_index = True, sort=False)

 	df = df.filter(items=Dic)
 	df
 	df.columns += "/"+ Extraer_Label(PathDataFrame)
 	df = Concatenar(df1=df,df2=df_tipo)
 	return df

def Extraer_Dataframe_Obligaciones(tree,PathDataFrame, des_situ,des_tipo):
 	""" Extrae todas las obligaciones y los atributos tipo y descripcion de las etiquetas """
 	tags = []
 	output = []

 	for root in tree.findall(PathDataFrame + "/OBLIGACION/"):
 		tags.append(root.tag)

 	tag = OrderedDict((x, 1) for x in tags).keys()
 	df = pd.DataFrame(columns=tag)

 	df_tipo = pd.DataFrame(columns=['situ','tipo'])
 	for root in tree.findall(PathDataFrame + "/"):
 		atrib_tipo = root.get(des_tipo)
 		atrib_situ = root.get(des_situ)
 		df_tipo = df_tipo.append({'situ': atrib_situ, 'tipo': atrib_tipo}, ignore_index=True)

 	for root in tree.findall(PathDataFrame + "/OBLIGACION"):
 		data = list(root)
 		listado = OrderedDict((content.tag, content.text) for content in data)
 		df_table = pd.DataFrame(listado, columns = tag, index=["1"])
 		df = df.append(df_table, ignore_index = True, sort=False)
 		df
 	df.columns += "/"+ Extraer_Label(PathDataFrame)
 	df = Concatenar(df1=df,df2=df_tipo)
 	dicObligaciones = ["PERIODO/OBLIGACIONES","FECHA_EJECUCION/OBLIGACIONES","FUENTE/OBLIGACIONES","situ","tipo"]
 	df = Validar_Formato_Tabla(df,dicObligaciones)
 	return df

def Extraer_Dataframe_Politica_Ccial(NIT,Fecha_Captura,Directorio,tree,PathDataFrame):
 	""" Extrae la informacion de la politica comercial para las ventas o las compras, se debe pasar el path completo de cada uno, determina tambien el porcentaje de nacional e internacional con manejo de errores cuando no hay politicas"""

 	tags = []
 	output = []

 	for root in tree.findall(PathDataFrame + "/"):
 		tags.append(root.tag)

 	tag = OrderedDict((x, 1) for x in tags).keys()
 	df = pd.DataFrame(columns=tag)

 	df_politica = pd.DataFrame(columns=['Porc_Nacional_Pol_Ccial','Porc_Internacional_Pol_Ccial'])
 	for root in tree.findall(PathDataFrame):
 		data = list(root)
 		listado = OrderedDict((content.tag, content.text) for content in data)
 		#print(listado)
 		df_table = pd.DataFrame(listado, columns = tag, index=["1"])
 		df = df.append(df_table, ignore_index = True, sort=False)
 		#df['Nit_Pagador'] = ConsultaElemento(PathNit)
 		#df.set_index('Nit_Pagador')
 		try:
 			nacional = root.find(".//NACIONAL/PORCENTAJE")
 			nacional = nacional.text
 		except:
 			print("Sin Porcentaje Nacional")
 			nacional =np.nan
 		try:
 			internacional = root.find(".//INTERNACIONAL/PORCENTAJE")
 			internacional = internacional.text
 		except:
 			internacional = np.nan
 			print("Sin Porcentaje Internacional")

 		#print(root)
 		df_politica = df_politica.append({'Porc_Nacional_Pol_Ccial': nacional, 'Porc_Internacional_Pol_Ccial': internacional}, ignore_index=True)
 		df = Concatenar(df1=df,df2=df_politica)

 		#print(df)
 	#df.columns += "/"+ Extraer_Label(PathDataFrame)
 	#print("########################################")
 	Path = Directorio +"_"+ Extraer_Label(PathDataFrame) + ".csv"
 	#df.to_csv(Path, index = False, encoding='utf-8-sig')
 	#print("Archivo Creado con Exito en \n" + Path)
 	df["Nit_Cliente"]=NIT
 	df["Tipo_Pol_Ccial"]=Extraer_Label(PathDataFrame)
 	df["Fecha_Captura"]=Fecha_Captura
 	dicPolitica = ["PRODUCTOS","POLITICA","FEC_EFECTO","Porc_Nacional_Pol_Ccial","Porc_Internacional_Pol_Ccial","Nit_Cliente","Tipo_Pol_Ccial","Fecha_Captura"]
 	#print(dicPolitica)
 	df = Validar_Formato_Tabla(df,dicPolitica)
 	dicPolitica = ["Producto_Pol_Ccial","Politica_Pol_CCial","Fecha_Efecto_Pol_Ccial","Porc_Nacional_Pol_Ccial","Porc_Internacional_Pol_Ccial","Nit_Cliente","Tipo_Pol_Ccial","Fecha_Captura"]
 	df.columns = dicPolitica

 	return df

def Financiero_Indicadores(tree,PathIndicadoresFinancieros, dic):
    Años = tree.findall(PathIndicadoresFinancieros)
    #print("LOS AÑOS SON......")
    #print(Años)
    dic = dic
    df = pd.DataFrame(columns = dic)
    i = 0
    for Año in Años:
        tags = []
        try:
            dfAnno = pd.DataFrame([Año.attrib['ANYO']], columns=["Fecha_Efecto_Indicador_Fro"])
            #print("EL DF ANNO ES...")
            #print(dfAnno)
        except:
            print("error al intentar extraer el año")
            dfAnno = pd.DataFrame([np.nan], columns=["Fecha_Efecto_Indicador_Fro"])
        dfdato =  pd.DataFrame()
        for dato in Año:
            tags.append(dato.tag)

            df1 = pd.DataFrame()
            for child in dato.findall('./'):
                #print(child.tag)
                #print(child.text)
                dfchild = pd.DataFrame([child.text], columns = [child.tag])
                df1 = Concatenar(df1=df1,df2=dfchild)

            df1.columns += "/" + dato.tag
            dfdato = Concatenar(df1=dfdato,df2=df1)
            #print(dfdato.to_string())
            #print("for item")
            df2 = Concatenar(df1=dfAnno,df2=dfdato)
            df2 = Validar_Formato_Tabla(df2,dic)
        #print(df2.to_string())
        df = Append(df1=df,df2=df2)
    #print(df.to_string())
    return df

""" FUNCIONES DE INSERCIÓN A BD"""

def ing_tbl_F_Info_Consulta(conn,cursor,tbl_F_Info_Consulta):
 	if tbl_F_Info_Consulta.empty:
 		print("tbl_F_Info_Consulta esta vacio")
 		#return True
 	else:
 		for index,row in tbl_F_Info_Consulta.iterrows():
 			cursor.execute("INSERT INTO dbo.tbl_F_Info_Consulta([Nit_Cliente],[Nombre_Consulta],[Usuario],[Fecha_Captura]) values (?,?,?,?)",
 				row['Nit_Cliente'],
 				row['Nombre_Consulta'],
 				row['Usuario'],
 				row['Fecha_Captura']
 				)
 		conn.commit()
 	#conn.close()
 	return True

def ing_tbl_F_Referencias_Cciales(conn,cursor,tbl_F_Referencias_Cciales):
 	if tbl_F_Referencias_Cciales.empty:
 		print("tbl_F_Referencias_Cciales esta vacio")
 	else:
 		for index,row in tbl_F_Referencias_Cciales.iterrows():
 			cursor.execute("INSERT INTO dbo.tbl_F_Referencias_Cciales([Fecha_Efecto], [Nombre_Proveedor], [Importe_Proveedor], [Forma_Pago], [Plazo_Pago], [Fecha_Ultimo_Pago], [Producto], [Opinion_Proveedor], [Nit_Proveedor], [Nit_Cliente], [Fecha_Captura]) values (?,?,?,?,?,?,?,?,?,?,?)",
 				row['Fecha_Efecto'],
 				row['Nombre_Proveedor'],
 				row['Importe_Proveedor'],
 				row['Forma_Pago'],
 				row['Plazo_Pago'],
 				row['Fecha_Ultimo_Pago'],
 				row['Producto'],
 				row['Opinion_Proveedor'],
 				row['Nit_Proveedor'],
 				row['Nit_Cliente'],
 				row['Fecha_Captura']
 				)
 			conn.commit()
 	#conn.close()
 	return True


def ing_tbl_F_Evolucion_Empleados(conn,cursor,tbl_F_Evolucion_Empleados):
	if tbl_F_Evolucion_Empleados.empty:
		print("tbl_F_Evolucion_Empleados esta vacio")
	else:
		for index,row in tbl_F_Evolucion_Empleados.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Evolucion_Empleados([Fecha_Efecto], [Nit_Cliente], [Cantidad_Empleados], [Fecha_Captura]) values (?,?,?,?)",
				row['Fecha_Efecto'],
				row['Nit_Cliente'],
				row['Cantidad_Empleados'],
				row['Fecha_Captura']
				)
			conn.commit()
	#conn.close()
	return True

def ing_tbl_F_Riesgo_Comercial(conn,cursor,tbl_F_Riesgo_Comercial):
	if tbl_F_Riesgo_Comercial.empty:
		print("tbl_F_Riesgo_Comercial esta vacio")
	else:
		for index,row in tbl_F_Riesgo_Comercial.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Riesgo_Comercial([Fecha_Efecto], [Nit_Cliente], [Situacion_Financiera], [Evolucion_Empresa], [Calificacion_Informa], [Riesgo_Informa], [Incidentes], [Info_Complementaria], [Fecha_Captura]) values (?,?,?,?,?,?,?,?,?)",
				row['Fecha_Efecto'],
				row['Nit_Cliente'],row['Situacion_Financiera'],
				row['Evolucion_Empresa'],
				row['Calificacion_Informa'],
				row['Riesgo_Informa'],
				row['Incidentes'],
				row['Info_Complementaria'],
				row['Fecha_Captura']
				)
			conn.commit()
	#conn.close()
	return True

def ing_tbl_D_Clientes(conn,cursor,tbl_D_Clientes):
	if tbl_D_Clientes.empty:
		print("tbl_D_Clientes esta vacio")
	else:
		for index,row in tbl_D_Clientes.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_D_Clientes([Nit_Cliente],[Duns_Cliente],[Nombre_Cliente],[Direccion_Cliente],[Municipio_Cliente],[Departamento_Cliente],[Pais_Cliente],[Telefono_Cliente],[Email_Cliente],[Direccion_Web_Cliente],[Fecha_Constitucion],[Forma_Juridica_Cliente],[Cod_ICI_Cliente],[Estado_Empresa],[Cod_Actividad_Ccial],[Actividad_Ccial],[Objeto_Social],[Tipo_Empresa], [Tamano_Empresa]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			row['Nit_Cliente'],
			row['Duns_Cliente'],
			row['Nombre_Cliente'],
			row['Direccion_Cliente'],
			row['Municipio_Cliente'],
			row['Departamento_Cliente'],
			row['Pais_Cliente'],
			row['Telefono_Cliente'],
			row['Email_Cliente'],
			row['Direccion_Web_Cliente'],
			row['Fecha_Constitucion'],
			row['Forma_Juridica_Cliente'],
			row['Cod_ICI_Cliente'],
			row['Estado_Empresa'],
			row['Cod_Actividad_Ccial'],
			row['Actividad_Ccial'],
			row['Objeto_Social'],
			row['Tipo_Empresa'],
            row['Tamano_Empresa']
			)
			conn.commit()
	#conn.close()
	return True

def ing_tbl_F_Info_Financiera(conn,cursor,tbl_F_Info_Financiera):
	for index,row in tbl_F_Info_Financiera.iterrows():
		cursor.execute("INSERT INTO dbo.tbl_F_Info_Financiera([Nit_Cliente], [Unidades], [NormaContable], [Fecha_Efecto], [Duracion], [Fuente], [Fecha_Captura]) values(?,?,?,?,?,?,?)",
			row['Nit_Cliente'],
			row['Unidades'],
			row['NormaContable'],
			row['Fecha_Efecto'],
			row['Duracion'],
			row['Fuente'],
			row['Fecha_Captura']
			)
	conn.commit()
	return True

def ing_tbl_F_Activos(conn,cursor,tbl_F_Activos):
	for index,row in tbl_F_Activos.iterrows():
		cursor.execute("INSERT INTO dbo.tbl_F_Activos([Id_Info_Financiera],[Nit_Cliente],[Fecha_Efecto],[Total_Activos],[Total_Activos_Cte],[Cuentas_x_Cobrar_Cte],[Inventarios_Cte],[Diferidos_Cte],[Gastos_Pagados_Ant_Cte],[Otros_Activos_Cte],[Otros_Activos_Financ_Cte],[Otros_Activos_No_Financ_Cte],[Activos_Imptos_Cte],[Activos_Calsif_Mantenido_Venta_Cte],[Efectivo_Equivalente_Cte],[Cuentas_x_Cobrar_Otras_Cte],[Cuentas_x_Cobrar_Partes_Rel_Cte],[Total_Activos_No_Cte],[Inversiones_No_Cte],[Inversiones_Asociadas_No_Cte],[Inversiones_Contabilizadas_No_Cte],[Propiedad_Planta_Equipo_No_Cte],[Diferidos_No_Cte],[Gastos_Pagados_Anticipado_No_Cte],[Otros_Activos_No_Cte],[Propiedad_Inversion_No_Cte],[Plusvalia_No_Cte],[Activos_Intangibles_No_Plusv_No_Cte],[Activos_Imptos_Diferido_No_Cte],[Inv_No_Cte],[Cuentas_x_Cobrar_No_Cte],[Cunetas_x_Cobrar_Otras_No_Cte],[Cuentas_x_Cobrar_Partes_Rel_No_Cte],[Otros_Activos_No_Fro],[Otros_Activos_Fro],[Fecha_Captura]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			row['Id_Info_Financiera'],
			row['Nit_Cliente'],
			row['Fecha_Efecto'],
			row['Total_Activos'],
			row['Total_Activos_Cte'],
			row['Cuentas_x_Cobrar_Cte'],
			row['Inventarios_Cte'],
			row['Diferidos_Cte'],
			row['Gastos_Pagados_Ant_Cte'],
			row['Otros_Activos_Cte'],
			row['Otros_Activos_Financ_Cte'],
			row['Otros_Activos_No_Financ_Cte'],
			row['Activos_Imptos_Cte'],
			row['Activos_Calsif_Mantenido_Venta_Cte'],
			row['Efectivo_Equivalente_Cte'],
			row['Cuentas_x_Cobrar_Otras_Cte'],
			row['Cuentas_x_Cobrar_Partes_Rel_Cte'],
			row['Total_Activos_No_Cte'],
			row['Inversiones_No_Cte'],
			row['Inversiones_Asociadas_No_Cte'],
			row['Inversiones_Contabilizadas_No_Cte'],
			row['Propiedad_Planta_Equipo_No_Cte'],
			row['Diferidos_No_Cte'],
			row['Gastos_Pagados_Anticipado_No_Cte'],
			row['Otros_Activos_No_Cte'],
			row['Propiedad_Inversion_No_Cte'],
			row['Plusvalia_No_Cte'],
			row['Activos_Intangibles_No_Plusv_No_Cte'],
			row['Activos_Imptos_Diferido_No_Cte'],
			row['Inv_No_Cte'],
			row['Cuentas_x_Cobrar_No_Cte'],
			row['Cunetas_x_Cobrar_Otras_No_Cte'],
			row['Cuentas_x_Cobrar_Partes_Rel_No_Cte'],
			row['Otros_Activos_No_Fro'],
			row['Otros_Activos_Fro'],
			row['Fecha_Captura']
			)
		conn.commit()
	return True

def ing_tbl_F_Pasivos_Patrimonio(conn,cursor,tbl_F_Pasivos_Patrimonio):
	for index,row in tbl_F_Pasivos_Patrimonio.iterrows():
		cursor.execute("INSERT INTO dbo.tbl_F_Pasivos_Patrimonio([Id_Info_Financiera],[Nit_Cliente],[Fecha_Efecto],[Total_Pasivo_Patrimonio],[Total_Pasivo],[Total_Pasivo_Cte],[Obligaciones_Fra],[Pasivo_Est_Provi],[Provi_Diversa],[Otro_Pasivo_Fro],[Otro_Pasivo_No_Fro],[Cuentas_x_Pagar_Cte],[Otras_Cuentas_x_Pagar_Cte],[Cuentas_x_Pagar_Ent_Rel],[Pasivo_Impto_Cte],[Provi_Cte_Empleado],[Otro_Pasivo_Cte],[Total_Pasivo_No_Cte],[Pasivo_Estimado_Provisiones_No_Cte],[Otro_Pasivo_Fro_No_Cte],[Otro_Pasivo_No_Fro_No_Cte],[Pasivo_Impto_Diferido_No_Cte],[Obligaciones_Fro_No_Cte],[Provisiones_Beneficios_No_Cte],[Otras_Provisiones_No_Cte],[Otro_Pasivo_No_Cte],[Patrimonio],[Capital_Social_Pt],[Superavit_Capital_Pt],[Reserva_Pt],[Resultado_Ejercicio_Pt],[Cotiza_Aux_Aporte_No_Vinc_Pt],[Otros_Rubros_Pt],[Acciones_Propias_Cartera_Pt],[Otro_Resultado_Integral_Acum_Pt],[Otras_Participaciones_Pt],[Primas_Emision_Pt],[Ganancias_Acum_Pt],[Capital_Emitido_Pt],[Fecha_Captura]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			row['Id_Info_Financiera'],
			row['Nit_Cliente'],
			row['Fecha_Efecto'],
			row['Total_Pasivo_Patrimonio'],
			row['Total_Pasivo'],
			row['Total_Pasivo_Cte'],
			row['Obligaciones_Fra'],
			row['Pasivo_Est_Provi'],
			row['Provi_Diversa'],
			row['Otro_Pasivo_Fro'],
			row['Otro_Pasivo_No_Fro'],
			row['Cuentas_x_Pagar_Cte'],
			row['Otras_Cuentas_x_Pagar_Cte'],
			row['Cuentas_x_Pagar_Ent_Rel'],
			row['Pasivo_Impto_Cte'],
			row['Provi_Cte_Empleado'],
			row['Otro_Pasivo_Cte'],
			row['Total_Pasivo_No_Cte'],
			row['Pasivo_Estimado_Provisiones_No_Cte'],
			row['Otro_Pasivo_Fro_No_Cte'],
			row['Otro_Pasivo_No_Fro_No_Cte'],
			row['Pasivo_Impto_Diferido_No_Cte'],
			row['Obligaciones_Fro_No_Cte'],
			row['Provisiones_Beneficios_No_Cte'],
			row['Otras_Provisiones_No_Cte'],
			row['Otro_Pasivo_No_Cte'],
			row['Patrimonio'],
			row['Capital_Social_Pt'],
			row['Superavit_Capital_Pt'],
			row['Reserva_Pt'],
			row['Resultado_Ejercicio_Pt'],
			row['Cotiza_Aux_Aporte_No_Vinc_Pt'],
			row['Otros_Rubros_Pt'],
			row['Acciones_Propias_Cartera_Pt'],
			row['Otro_Resultado_Integral_Acum_Pt'],
			row['Otras_Participaciones_Pt'],
			row['Primas_Emision_Pt'],
			row['Ganancias_Acum_Pt'],
			row['Capital_Emitido_Pt'],
			row['Fecha_Captura']
			)
	conn.commit()
	return True

def ing_tbl_F_Resultados_Ejercicio(conn,cursor,tbl_F_Resultados_Ejercicio):
	for index,row in tbl_F_Resultados_Ejercicio.iterrows():
		cursor.execute("INSERT INTO dbo.tbl_F_Resultados_Ejercicio([Id_Info_Financiera],[Nit_Cliente],[Fecha_Efecto],[Resultado_Ejercicio],[Resultado_Antes_Impto],[Resultado_Op],[Total_Gastos],[Costos_Gastos_Op],[Gastos_Op_Admin],[Gastos_Op_Venta],[Gastos_Dist],[Gastos_Beneficio_Empl],[Otros_Gastos_Op],[Costos_Venta],[Gastos_No_Op],[Gastos_Fro],[Total_Ingresos],[Ingresos_Operacional],[Ventas],[Otros_Ingresos_Op],[Ingresos_No_Op],[Ingresos_Extraordinarios],[Ingresos_Fro],[Resultados_No_Op],[Resultados_Fro],[Resultados_Impuesto],[Ajuste_Inflacion],[Impto_Renta],[Fecha_Captura]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			row['Id_Info_Financiera'],
			row['Nit_Cliente'],
			row['Fecha_Efecto'],
			row['Resultado_Ejercicio'],
			row['Resultado_Antes_Impto'],
			row['Resultado_Op'],
			row['Total_Gastos'],
			row['Costos_Gastos_Op'],
			row['Gastos_Op_Admin'],
			row['Gastos_Op_Venta'],
			row['Gastos_Dist'],
			row['Gastos_Beneficio_Empl'],
			row['Otros_Gastos_Op'],
			row['Costos_Venta'],
			row['Gastos_No_Op'],
			row['Gastos_Fro'],
			row['Total_Ingresos'],
			row['Ingresos_Operacional'],
			row['Ventas'],
			row['Otros_Ingresos_Op'],
			row['Ingresos_No_Op'],
			row['Ingresos_Extraordinarios'],
			row['Ingresos_Fro'],
			row['Resultados_No_Op'],
			row['Resultados_Fro'],
			row['Resultados_Impuesto'],
			row['Ajuste_Inflacion'],
			row['Impto_Renta'],
			row['Fecha_Captura']
			)
	conn.commit()
	return True

def ing_tbl_F_Participantes(conn,cursor,tbl_F_Participantes):
	if tbl_F_Participantes.empty:
		print("tbl_F_Participantes esta vacio")
	else:
		for index,row in tbl_F_Participantes.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Participantes([Nit_Cliente], [Nombre_Participante], [Doc_Participante], [Porcentaje], [Fecha_Efecto], [Fecha_Captura]) values (?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Nombre_Participante'],
				row['Doc_Participante'],
				row['Porcentaje'],
				row['Fecha_Efecto'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Accionistas(conn,cursor,tbl_F_Accionistas):
	if tbl_F_Accionistas.empty:
		print("tbl_F_Accionistas esta vacio")
	else:
		for index,row in tbl_F_Accionistas.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Accionistas([Nit_Cliente],[Doc_Accionista],[Nombre_Accionista], [Razon_Social],[Fecha_Efecto],[Fecha_Captura]) values (?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Doc_Accionista'],
				row['Nombre_Accionista'],
				row['Razon_Social'],
				row['Fecha_Efecto'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Capital(conn,cursor,tbl_F_Capital):
	if tbl_F_Capital.empty:
		print("tbl_F_Capital esta vacio")
	else:
		for index,row in tbl_F_Capital.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Capital([Nit_Cliente],[Importe],[Fecha_Efecto],[Fecha_Captura]) values (?,?,?,?)",
				row['Nit_Cliente'],
				row['Importe'],
				row['Fecha_Efecto'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Administradores(conn,cursor,tbl_F_Administradores):
	if tbl_F_Administradores.empty:
		print("tbl_F_Administradores esta vacio")
	else:
		for index,row in tbl_F_Administradores.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Administradores([Nit_Cliente],[Fecha_Actualizacion],[Doc_Administrador],[Nombre_Administrador],[Cargo_Administrador],[Fecha_Efecto],[Fecha_Captura]) values (?,?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Fecha_Actualizacion'],
				row['Doc_Administrador'],
				row['Nombre_Administrador'],
				row['Cargo_Administrador'],
				row['Fecha_Efecto'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Establecimientos(conn,cursor,tbl_F_Establecimientos):
	if tbl_F_Establecimientos.empty:
		print("tbl_F_Establecimientos esta vacio")
	else:
		for index,row in tbl_F_Establecimientos.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Establecimientos([Nit_Cliente],[Nombre_Establecimiento],[Tipo_Explotacion],[Departamento],[Fecha_Efecto],[Fecha_Captura]) values (?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Nombre_Establecimiento'],
				row["Tipo_Explotacion"],
				row["Departamento"],
				row['Fecha_Efecto'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Incidencias(conn,cursor,tbl_F_Incidencias):
	if tbl_F_Incidencias.empty:
		print("tbl_F_Incidencias esta vacio")
	else:
		for index,row in tbl_F_Incidencias.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Incidencias([Nit_Cliente],[Fecha_Efecto],[Estado_Incidencia],[Municipio],[Cod_Incidencia],[Tipo_Incidencia],[Descripcion_Incidencia],[Demandante],[Total_Incidencias],[Fecha_Captura]) values (?,?,?,?,?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Fecha_Efecto'],
				row['Estado_Incidencia'],
				row['Municipio'],
				row['Cod_Incidencia'],
				row['Tipo_Incidencia'],
				row['Descripcion_Incidencia'],
				row['Demandante'],
				row['Total_Incidencias'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Importaciones(conn,cursor,tbl_F_Importaciones):
	if tbl_F_Importaciones.empty:
		print("tbl_F_Importaciones esta vacio")
	else:
		for index,row in tbl_F_Importaciones.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Importaciones([Nit_Cliente],[Anno],[Fecha_Efecto],[Producto],[Pais],[Valor],[Divisa],[Fecha_Captura]) values (?,?,?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Anno'],
				row['Fecha_Efecto'],
				row['Producto'],
				row['Pais'],
				row['Valor'],
				row['Divisa'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Exportaciones(conn,cursor,tbl_F_Exportaciones):
	if tbl_F_Exportaciones.empty:
		print("tbl_F_Exportaciones esta vacio")
	else:
		for index,row in tbl_F_Exportaciones.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Exportaciones([Nit_Cliente],[Anno],[Fecha_Efecto],[Producto],[Pais],[Valor],[Divisa],[Fecha_Captura]) values (?,?,?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Anno'],
				row['Fecha_Efecto'],
				row['Producto'],
				row['Pais'],
				row['Valor'],
				row['Divisa'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Actividades(conn,cursor,tbl_F_Actividades):
	if tbl_F_Actividades.empty:
		print("tbl_F_Actividades esta vacio")
	else:
		for index,row in tbl_F_Actividades.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Actividades([Nit_Cliente],[Tipo_Actividad],[Cod_Actividad],[Descripcion_Actividad],[Fecha_Captura]) values (?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Tipo_Actividad'],
				row['Cod_Actividad'],
				row['Descripcion_Actividad'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Obligaciones(conn,cursor,tbl_F_Obligaciones):
	if tbl_F_Obligaciones.empty:
		print("tbl_F_Obligaciones esta vacio")
	else:
		for index,row in tbl_F_Obligaciones.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Obligaciones([Nit_Cliente],[Tipo_Obligacion],[Periodo_Obligacion],[Situacion_Obligacion],[Fecha_Ejecucion_Obligacion],[Fuente_Obligacion],[Fecha_Captura]) values (?,?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Tipo_Obligacion'],
				row['Periodo_Obligacion'],
				row['Situacion_Obligacion'],
				row['Fecha_Ejecucion_Obligacion'],
				row['Fuente_Obligacion'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Politica_Comercial(conn,cursor,tbl_F_Politica_Comercial):
	if tbl_F_Politica_Comercial.empty:
		print("tbl_F_Politica_Comercial esta vacio")
	else:
		for index,row in tbl_F_Politica_Comercial.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Politica_Comercial([Nit_Cliente],[Tipo_Pol_Ccial],[Producto_Pol_Ccial],[Politica_Pol_CCial],[Fecha_Efecto_Pol_Ccial],[Porc_Nacional_Pol_Ccial],[Porc_Internacional_Pol_Ccial],[Fecha_Captura]) values (?,?,?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Tipo_Pol_Ccial'],
				row['Producto_Pol_Ccial'],
				row['Politica_Pol_CCial'],
				row['Fecha_Efecto_Pol_Ccial'],
				row['Porc_Nacional_Pol_Ccial'],
				row["Porc_Internacional_Pol_Ccial"],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Publicaciones_Prensa(conn,cursor,tbl_F_Publicaciones_Prensa):
	if tbl_F_Publicaciones_Prensa.empty:
		print("tbl_F_Publicaciones_Prensa esta vacio")
	else:
		for index,row in tbl_F_Publicaciones_Prensa.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Publicaciones_Prensa([Nit_Cliente],[Fecha_Publicacion],[Fuente],[Tipo_Articulo],[Resumen_Publicacion],[Fecha_Captura]) values (?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Fecha_Publicacion'],
				row['Fuente'],
				row['Tipo_Articulo'],
				row['Resumen_Publicacion'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Publicaciones_Legales(conn,cursor,tbl_F_Publicaciones_Legales):
	if tbl_F_Publicaciones_Legales.empty:
		print("tbl_F_Publicaciones_Legales esta vacio")
	else:
		for index,row in tbl_F_Publicaciones_Legales.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Publicaciones_Legales([Nit_Cliente],[Tipo_Acto],[Fecha_Acto],[Referencia],[Fuente],[Lugar_Publicacion],[Fecha_Captura]) values (?,?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Tipo_Acto'],
				row['Fecha_Acto'],
				row['Referencia'],
				row['Fuente'],
				row["Lugar_Publicacion"],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Relaciones_Terceros(conn,cursor,tbl_F_Relaciones_Terceros):
	if tbl_F_Relaciones_Terceros.empty:
		print("tbl_F_Relaciones_Terceros esta vacio")
	else:
		for index,row in tbl_F_Relaciones_Terceros.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Relaciones_Terceros([Nit_Cliente],[Tipo_Relacion],[Razon_Social],[Nit_Razon_Social],[Fecha_Captura]) values (?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Tipo_Relacion'],
				row['Razon_Social'],
				row['Nit_Razon_Social'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def ing_tbl_F_Indicadores_Financieros(conn,cursor,tbl_F_Indicadores_Financieros):
	if tbl_F_Indicadores_Financieros.empty:
		print("tbl_F_Indicadores_Financieros esta vacio")
	else:
		for index,row in tbl_F_Indicadores_Financieros.iterrows():
			cursor.execute("INSERT INTO dbo.tbl_F_Indicadores_Financieros([Nit_Cliente],[Fecha_Efecto_Indicador_Fro],[Evolucion_Ventas],[Evolucion_Utilidad_Neta],[Rentabilidad],[Rentabilidad_Operacional],[Rentabilidad_Patrimonio],[Rentabilidad_Activo_Total],[Cobertura_Gastos_Fro],[EBIT],[EBITDA],[Endeudamiento],[Concentracion_Corto_Plazo],[Endeudamiento_Sin_Valorizacion],[Apalancamiento_Fro],[Carga_Fra],[Capital_Trabajo],[Razon_Cte],[Prueba_Acida],[Dias_Rotacion_Inventario],[Dias_Ciclo_Operacional],[Rotacion_Activos],[Fecha_Captura]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
				row['Nit_Cliente'],
				row['Fecha_Efecto_Indicador_Fro'],
				row['Evolucion_Ventas'],
				row['Evolucion_Utilidad_Neta'],
				row['Rentabilidad'],
				row['Rentabilidad_Operacional'],
				row['Rentabilidad_Patrimonio'],
				row['Rentabilidad_Activo_Total'],
				row['Cobertura_Gastos_Fro'],
				row['EBIT'],
				row['EBITDA'],
				row['Endeudamiento'],
				row['Concentracion_Corto_Plazo'],
				row['Endeudamiento_Sin_Valorizacion'],
				row['Apalancamiento_Fro'],
				row['Carga_Fra'],
				row['Capital_Trabajo'],
				row['Razon_Cte'],
				row['Prueba_Acida'],
				row['Dias_Rotacion_Inventario'],
				row['Dias_Ciclo_Operacional'],
				row['Rotacion_Activos'],
				row['Fecha_Captura']
				)
		conn.commit()
	return True

def Uptate_Tbls_Financieras(conn,cursor):
	#Update a Activos
	cursor.execute(f"""update [dbo].[tbl_F_Activos]
		set [dbo].[tbl_F_Activos].Id_Info_Financiera=ifi.Id_Info_Financiera
		from [dbo].[tbl_F_Activos] a
		inner join [dbo].[tbl_F_Info_Financiera] ifi
		on (a.Fecha_Captura=ifi.Fecha_Captura
		and a.Nit_Cliente=ifi.Nit_Cliente
		and a.Fecha_Efecto=ifi.Fecha_Efecto)""")
	conn.commit()

	#Update a Pasivo_Patrimonio
	cursor.execute(f"""update [dbo].[tbl_F_Pasivos_Patrimonio]
		set [dbo].[tbl_F_Pasivos_Patrimonio].Id_Info_Financiera=ifi.Id_Info_Financiera
		from [dbo].[tbl_F_Pasivos_Patrimonio] pp
		inner join [dbo].[tbl_F_Info_Financiera] ifi
		on (pp.Fecha_Captura=ifi.Fecha_Captura
		and pp.Nit_Cliente=ifi.Nit_Cliente
		and pp.Fecha_Efecto=ifi.Fecha_Efecto)""")
	conn.commit()

	#Update a Resultados Ejercicio
	cursor.execute(f"""update [dbo].[tbl_F_Resultados_Ejercicio]
		set [dbo].[tbl_F_Resultados_Ejercicio].Id_Info_Financiera=ifi.Id_Info_Financiera
		from [dbo].[tbl_F_Resultados_Ejercicio] re
		inner join [dbo].[tbl_F_Info_Financiera] ifi
		on (re.Fecha_Captura=ifi.Fecha_Captura
		and re.Nit_Cliente=ifi.Nit_Cliente
		and re.Fecha_Efecto=ifi.Fecha_Efecto)""")
	conn.commit()

"""FUNCIÓN PRINCIPAL DE EJECUCIÓN"""

def save_dataframe(nit):
	""" Guarda los csv de los df """
	#Directorio="C:\\Python_Flask\\envDivisa\\"
	Directorio = os.getcwd() + "\\"
	#PathCarpetaConsultas = "C:\\Python_Flask\\envDivisa\\Consultas\\"
	# PathCarpetaConsultas = Directorio +"Consultas\\"
	PathCarpetaConsultas = "/var/www/html/flask/Consultas/"
	#PathCarpetaResultados = "C:\\Python_Flask\\envDivisa\\Resultados\\"
	#PathCarpetaResultados = Directorio +"Resultados\\"

	NIT = nit
	Id_Cliente = NIT
	PathXml = PathCarpetaConsultas + NIT +".xml"
	conn,cursor=connectionDB()


	_NIT = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ID_ANEXA/IDFISCAL/VALOR"
	NombreEmpresa = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/DENOMINACION/RAZONSOCIAL/VALOR"

	tree = ET.parse(PathXml)
	root = tree.getroot()

	_NIT = ConsultaElemento(root,_NIT)
	_NombreEmpresa = ConsultaElemento(root,NombreEmpresa).replace(" ", "")

	NombreArchivo = _NIT+"_"+_NombreEmpresa+".csv"
	NombreArchivofill = "_"+_NIT+"_"+_NombreEmpresa+".csv"
	Fecha_Captura_UTC = datetime.now(timezone('UTC'))
	Fecha_Captura = Fecha_Captura_UTC.astimezone(timezone('America/Bogota')).strftime('%Y-%m-%d %H:%M:%S')
	#print("FECHA:", Fecha_Captura)

	"""###tbl_F_Info_Consulta"""

	dicInfoConsulta = {'Nit_Cliente': [NIT], 'Nombre_Consulta': ["INFORME FINANCIERO"], 'Usuario':['C16134'], 'Fecha_Captura':[Fecha_Captura]}
	tbl_F_Info_Consulta = pd.DataFrame.from_dict(dicInfoConsulta)
	#Guardar_csv(tbl_F_Info_Consulta, PathCarpetaResultados, f"{NIT}_tbl_F_Info_Consulta.csv")
	ing_tbl_F_Info_Consulta(conn,cursor,tbl_F_Info_Consulta)


	""" Referencias Comerciales """

	Id_Ref_Comercial = np.nan
	PathReferenciasComerciales = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/REFCOMERCIAL/COMERCIAL/PROVEEDOR"
	DicReferenciasComerciales = ["IDENT_EMPRESA","RAZONSOCIAL","IMPORTE","FORMA_PAGO_LOCAL","PLAZO_PAGO_LOCAL","FEC_ULT_PAGO","FEC_EFECTO","PRODUCTO","COMPOR_PAGO_LOCAL"]
	dfReferenciasComerciales = Extraer_Dataframe_Dic(Directorio,tree,root,PathReferenciasComerciales, DicReferenciasComerciales)

	DicReferenciasComerciales = ["IDENT_EMPRESA/PROVEEDOR","RAZONSOCIAL/PROVEEDOR","IMPORTE/PROVEEDOR","FORMA_PAGO_LOCAL/PROVEEDOR","PLAZO_PAGO_LOCAL/PROVEEDOR","FEC_ULT_PAGO/PROVEEDOR","FEC_EFECTO/PROVEEDOR","PRODUCTO/PROVEEDOR","COMPOR_PAGO_LOCAL/PROVEEDOR"]
	dfReferenciasComerciales = Validar_Formato_Tabla(dfReferenciasComerciales,DicReferenciasComerciales)

	dfReferenciasComerciales.columns = ['Nit_Proveedor', 'Nombre_Proveedor', 'Importe_Proveedor', 'Forma_Pago', 'Plazo_Pago', 'Fecha_Ultimo_Pago', 'Fecha_Efecto', 'Producto', 'Opinion_Proveedor']
	dfReferenciasComerciales['Fecha_Captura'] = Fecha_Captura
	dfReferenciasComerciales['Nit_Cliente'] = NIT

	Dictbl_F_Referencias_Cciales = ['Fecha_Efecto', 'Nombre_Proveedor', 'Importe_Proveedor', 'Forma_Pago', 'Plazo_Pago', 'Fecha_Ultimo_Pago', 'Producto', 'Opinion_Proveedor', 'Nit_Proveedor', 'Nit_Cliente', 'Fecha_Captura']
	tbl_F_Referencias_Cciales = Validar_Formato_Tabla(dfReferenciasComerciales,Dictbl_F_Referencias_Cciales)
	#Guardar_csv(tbl_F_Referencias_Cciales, PathCarpetaResultados, f"{NIT}_tbl_F_Referencias_Cciales.csv")
	ing_tbl_F_Referencias_Cciales(conn,cursor,tbl_F_Referencias_Cciales)

	"""###tbl_F_Evolucion_Empleados"""

	PathEvolucionEmpleados_Actual = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/EMPLEADOS/ACTUAL"
	DicEvolucionEmpleados_Actual = ["FIJOS","FEC_EFECTO"]
	try:
		dfEvolucionEmpleados_Actual = Extraer_Dataframe_Dic(Directorio,tree,root,PathEvolucionEmpleados_Actual, DicEvolucionEmpleados_Actual)
		dfEvolucionEmpleados_Actual.columns = ['Cantidad_Empleados', 'Fecha_Efecto']
	except:
		dfEvolucionEmpleados_Actual = pd.DataFrame(columns=['Cantidad_Empleados', 'Fecha_Efecto'])

	try:
		PathEvolucionEmpleados_Hist = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/EMPLEADOS/ANTERIOR"
		DicEvolucionEmpleados_Hist = ["FIJOS","FEC_EFECTO"]
		dfEvolucionEmpleados_Hist = Extraer_Dataframe_Dic(Directorio,tree,root,PathEvolucionEmpleados_Hist, DicEvolucionEmpleados_Hist)
		dfEvolucionEmpleados_Hist.columns = ['Cantidad_Empleados', 'Fecha_Efecto']
		dfEvolucionEmpleados = Append(df1= dfEvolucionEmpleados_Actual, df2= dfEvolucionEmpleados_Hist)
	except:
		dfEvolucionEmpleados = dfEvolucionEmpleados_Actual

	Id_Evolucion_Empleado = np.nan

	dfEvolucionEmpleados["Nit_Cliente"] = NIT
	dfEvolucionEmpleados["Fecha_Captura"] = Fecha_Captura
	Dictbl_F_Evolucion_Empleados = ["Fecha_Efecto", "Nit_Cliente", "Cantidad_Empleados", 'Fecha_Captura']
	tbl_F_Evolucion_Empleados = Validar_Formato_Tabla(dfEvolucionEmpleados,Dictbl_F_Evolucion_Empleados)

	#Guardar_csv(tbl_F_Evolucion_Empleados, PathCarpetaResultados, f"{NIT}_tbl_F_Evolucion_Empleados.csv")
	ing_tbl_F_Evolucion_Empleados(conn,cursor,tbl_F_Evolucion_Empleados)

	"""###tbl_F_Riesgo_Comercial"""

	Id_Riesgo_Comercial = np.nan

	PathRiesgoComercial = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/EVALUACION"
	AtribRiesgoComercial = "DES"
	dfRiesgoComercial = Extraer_Dataframe_1Atributo(tree,PathRiesgoComercial, AtribRiesgoComercial)
	dicRiesgoComercial = ["SINTESIS_SITUACION_FINANCIERA/EVALUACION", "SINTESIS_TIPOLOGIA/EVALUACION", "SINTESIS_TRAYECTORIA/EVALUACION", "SINTESIS_INCIDENTES/EVALUACION"]
	dfRiesgoComercial = Validar_Formato_Tabla(dfRiesgoComercial,dicRiesgoComercial)
	##display(HTML(dfRiesgoComercial.to_html()))

	dfRiesgoComercialCalificacion = Extraer_Dataframe(Directorio,tree,PathRiesgoComercial)
	try:
		dfRiesgoComercialCalificacion = dfRiesgoComercialCalificacion[["FEC_CALCULO", "NOTA", "TEXTO_NOTA"]]
	except:
		dfRiesgoComercialCalificacion = dfRiesgoComercialCalificacion[["FEC_CALCULO", "TEXTO_NOTA"]]
		dfRiesgoComercialCalificacion['NOTA'] = np.nan
		dfRiesgoComercialCalificacion = dfRiesgoComercialCalificacion[["FEC_CALCULO", "NOTA", "TEXTO_NOTA"]]

	dfRiesgoComercialCalificacion.columns = ["Fecha_Efecto", "Calificacion_Informa", "Riesgo_Informa"]

	tbl_F_Riesgo_Comercial = dfRiesgoComercial.drop(["SINTESIS_TIPOLOGIA/EVALUACION"], axis=1)
	tbl_F_Riesgo_Comercial.columns = ["Situacion_Financiera", "Evolucion_Empresa", "Incidentes"]

	tbl_F_Riesgo_Comercial["Nit_Cliente"] = NIT

	tbl_F_Riesgo_Comercial = Concatenar(df1= tbl_F_Riesgo_Comercial, df2= dfRiesgoComercialCalificacion)
	tbl_F_Riesgo_Comercial["Fecha_Captura"] = Fecha_Captura

	PathRiesgoComercialCLinton = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/EVALUACION/OPINION_CLIENTE"
	RiesgoComercialCLinton = ConsultaElemento(root,PathRiesgoComercialCLinton)
	tbl_F_Riesgo_Comercial["Info_Complementaria"] = RiesgoComercialCLinton

    PathTamanoEmpresa = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/EVALUACION/SINTESIS_TIPOLOGIA"
    Tamano_Empresa = ConsultaElemento(root, PathTamanoEmpresa)
    tbl_D_Clientes["Tamano_Empresa"] = Tamano_Empresa

	Dictbl_F_Riesgo_Comercial = ['Fecha_Efecto', 'Nit_Cliente', 'Situacion_Financiera', 'Evolucion_Empresa', 'Calificacion_Informa', 'Riesgo_Informa', 'Incidentes', 'Info_Complementaria', 'Fecha_Captura']
	tbl_F_Riesgo_Comercial = Validar_Formato_Tabla(tbl_F_Riesgo_Comercial,Dictbl_F_Riesgo_Comercial)

	#Guardar_csv(tbl_F_Riesgo_Comercial, PathCarpetaResultados, f"{NIT}_tbl_F_Riesgo_Comercial.csv")
	ing_tbl_F_Riesgo_Comercial(conn,cursor,tbl_F_Riesgo_Comercial)

	"""###tbl_D_Clientes"""

	Cod_ICI_Cliente = np.nan

	PathIdentificacionCaracteristicasNIT = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ID_ANEXA/IDFISCAL"
	DicIdentificacionCaracteristicasNIT = ["VALOR"]
	dfIdentificacionCaracteristicasNIT = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasNIT, DicIdentificacionCaracteristicasNIT)

	tbl_D_Clientes = dfIdentificacionCaracteristicasNIT
	tbl_D_Clientes.columns = ['Nit_Cliente']
	tbl_D_Clientes['Id_Cliente'] = Id_Cliente

	PathIdentificacionCaracteristicasRAZONZ = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/DENOMINACION/RAZONSOCIAL"
	DicIdentificacionCaracteristicasRAZONZ = ["VALOR", "FEC_EFECTO"]
	dfIdentificacionCaracteristicasRAZONZ = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasRAZONZ, DicIdentificacionCaracteristicasRAZONZ)
	tbl_D_Clientes['Nombre_Cliente'] = dfIdentificacionCaracteristicasRAZONZ["VALOR/RAZONSOCIAL"].iloc[0]

	PathIdentificacionCaracteristicasDIR = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/DIRECCION/SEDESOCIAL"
	dfIdentificacionCaracteristicasDIR = Extraer_Dataframe(Directorio,tree,PathIdentificacionCaracteristicasDIR)
	dfIdentificacionCaracteristicasDIR["Numero"] = " # "
	try:
		Municipio = dfIdentificacionCaracteristicasDIR["LOCALIDAD"].iloc[0]
		Departamento = dfIdentificacionCaracteristicasDIR["DESC_PROVINCIA"].iloc[0]
		Pais = dfIdentificacionCaracteristicasDIR["DESC_PAIS"].iloc[0]
	except:
		Municipio = np.nan
		Departamento = np.nan
		Pais = np.nan

	dicDireccion = ["DESC_TIPOVIA","VIA", "Numero", "NUMEROVIA"]
	dfIdentificacionCaracteristicasDIR = Validar_Formato_Tabla(dfIdentificacionCaracteristicasDIR,dicDireccion)

	dfIdentificacionCaracteristicasDIR = Combinar_Registros(dfIdentificacionCaracteristicasDIR, PathIdentificacionCaracteristicasDIR)

	dfIdentificacionCaracteristicasDIR = dfIdentificacionCaracteristicasDIR[0:1][:]
	tbl_D_Clientes['Direccion_Cliente'] = dfIdentificacionCaracteristicasDIR["SEDESOCIAL"].iloc[0]

	PathIdentificacionCaracteristicasWEB = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/DIRECCION/WEB"
	DicIdentificacionCaracteristicasWEB = ["VALOR"]
	dfIdentificacionCaracteristicasWEB = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasWEB, DicIdentificacionCaracteristicasWEB)
	try:
		tbl_D_Clientes["Direccion_Web_Cliente"] = dfIdentificacionCaracteristicasWEB['VALOR/WEB'].iloc[0].lower()
	except:
		tbl_D_Clientes["Direccion_Web_Cliente"] = np.nan

	PathIdentificacionCaracteristicasTEL = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/CONTACTO/TELEFONO"
	DicIdentificacionCaracteristicasTEL = ["VALOR"]
	dfIdentificacionCaracteristicasTEL = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasTEL, DicIdentificacionCaracteristicasTEL)
	tbl_D_Clientes['Telefono_Cliente'] = dfIdentificacionCaracteristicasTEL['VALOR/TELEFONO'].iloc[0]

	PathIdentificacionCaracteristicasEMAIL = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/CONTACTO/EMAIL"
	DicIdentificacionCaracteristicasEMAIL = ["VALOR"]
	dfIdentificacionCaracteristicasEMAIL = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasEMAIL, DicIdentificacionCaracteristicasEMAIL)
	tbl_D_Clientes['Email_Cliente'] = dfIdentificacionCaracteristicasEMAIL['VALOR/EMAIL'].iloc[0]

	PathIdentificacionCaracteristicasJURID = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/FORJUR/"
	DicIdentificacionCaracteristicasJURID = ["DES_TIPO", "FEC_EFECTO"]
	dfIdentificacionCaracteristicasJURID = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasJURID, DicIdentificacionCaracteristicasJURID)
	tbl_D_Clientes['Forma_Juridica_Cliente'] = dfIdentificacionCaracteristicasJURID["DES_TIPO/"].iloc[0]
	# tbl_D_Clientes['Fecha_Constitucion'] = dfIdentificacionCaracteristicasJURID["FEC_EFECTO/"].iloc[0]

	PathIdentificacionCaracteristicasCONST = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/INFO_LEGAL"
	DicIdentificacionCaracteristicasCONST = ["FEC_CONSTITUCION", "FEC_INI_ACT"]
	dfIdentificacionCaracteristicasCONST = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasCONST, DicIdentificacionCaracteristicasCONST)
	tbl_D_Clientes['Fecha_Constitucion'] = dfIdentificacionCaracteristicasCONST["FEC_CONSTITUCION/INFO_LEGAL"].iloc[0]

	PathIdentificacionCaracteristicasACTIV = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ACTIVIDADES/CODIGO/ACTIVIDAD"
	DicIdentificacionCaracteristicasACTIV = ["CODIGO","DESC_FORMATO_LOCAL"]
	dfIdentificacionCaracteristicasACTIV = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasACTIV, DicIdentificacionCaracteristicasACTIV)
	tbl_D_Clientes['Cod_Actividad_Ccial'] = dfIdentificacionCaracteristicasACTIV["CODIGO/ACTIVIDAD"].iloc[0]
	tbl_D_Clientes['Actividad_Ccial'] = dfIdentificacionCaracteristicasACTIV["DESC_FORMATO_LOCAL/ACTIVIDAD"].iloc[0]

	PathIdentificacionCaracteristicasESTADO = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ESTADOEMPRESA"
	DicIdentificacionCaracteristicasESTADO = ["DESESTADO"]
	dfIdentificacionCaracteristicasESTADO = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasESTADO, DicIdentificacionCaracteristicasESTADO)
	tbl_D_Clientes['Estado_Empresa'] = dfIdentificacionCaracteristicasESTADO["DESESTADO/ESTADOEMPRESA"].iloc[0]

	#Actividades Objeto Social

	PathActividadesObjetoSocial = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ACTIVIDADES/TEXTO/ACTIVIDAD"
	DicActividadesObjetoSocial = ["TEXTO"]
	dfActividadesObjetoSocial = Extraer_Dataframe_Dic(Directorio,tree,root,PathActividadesObjetoSocial, DicActividadesObjetoSocial)

	tbl_D_Clientes["Tipo_Empresa"] = dfRiesgoComercial["SINTESIS_TIPOLOGIA/EVALUACION"].iloc[0]
	try:
		tbl_D_Clientes["Objeto_Social"] = dfActividadesObjetoSocial["TEXTO/ACTIVIDAD"].iloc[0]
	except:
		print("Sin información en la columna TEXTO")
		DicActividadesObjetoSocial = ["TEXTO/ACTIVIDAD"]
		dfActividadesObjetoSocial = Validar_Formato_Tabla(dfActividadesObjetoSocial, DicActividadesObjetoSocial)
		tbl_D_Clientes["Objeto_Social"] = np.nan

	tbl_D_Clientes["Cod_ICI_Cliente"] = Cod_ICI_Cliente
	tbl_D_Clientes["Fecha_Captura"] = Fecha_Captura

	PathIdentificacionCaracteristicasDUNS = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ID_ANEXA/DUNS"
	DicIdentificacionCaracteristicasDUNS = ["VALOR"]
	dfIdentificacionCaracteristicasDUNS = Extraer_Dataframe_Dic(Directorio,tree,root,PathIdentificacionCaracteristicasDUNS, DicIdentificacionCaracteristicasDUNS)
	tbl_D_Clientes["Duns_Cliente"] = dfIdentificacionCaracteristicasDUNS["VALOR/DUNS"].iloc[0]
	tbl_D_Clientes["Municipio_Cliente"] = Municipio
	tbl_D_Clientes["Departamento_Cliente"] = Departamento
	tbl_D_Clientes["Pais_Cliente"] = Pais
	Dictbl_D_Clientes = ['Nit_Cliente','Duns_Cliente','Nombre_Cliente', 'Direccion_Cliente', 'Municipio_Cliente','Departamento_Cliente','Pais_Cliente','Telefono_Cliente','Email_Cliente','Direccion_Web_Cliente','Fecha_Constitucion','Forma_Juridica_Cliente','Cod_ICI_Cliente','Estado_Empresa','Cod_Actividad_Ccial','Actividad_Ccial','Objeto_Social','Tipo_Empresa']
	tbl_D_Clientes = Validar_Formato_Tabla(tbl_D_Clientes,Dictbl_D_Clientes)

	#Guardar_csv(tbl_D_Clientes, PathCarpetaResultados, f"{NIT}_tbl_D_Clientes.csv")

	ing_tbl_D_Clientes(conn,cursor,tbl_D_Clientes)

	"""###tbl_F_Info_Financiera"""

	Id_Info_Financiera = np.nan
	PathBalances =     "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/BALANCES/PRIORITARIO"
	dicFinancieroEncabezadoOrdenado = ['Nit_Cliente', 'Unidades', 'NormaContable', 'Fecha_Efecto', 'Duracion', 'Fuente', 'Fecha_Captura']
	dfFinancieroEncabezado = FinancieroEncabezados(Fecha_Captura,tree,PathBalances, NIT)
	tbl_F_Info_Financiera = Validar_Formato_Tabla(dfFinancieroEncabezado, dicFinancieroEncabezadoOrdenado)
	try:
		PathBalancesActual =     "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/BALANCES/ACTUAL"
		dfFinancieroEncabezadoActual = FinancieroEncabezados(Fecha_Captura,tree,PathBalancesActual, NIT)
		tbl_F_Info_FinancieraActual = Validar_Formato_Tabla(dfFinancieroEncabezadoActual, dicFinancieroEncabezadoOrdenado)
		tbl_F_Info_Financiera=Append(df1=tbl_F_Info_FinancieraActual,df2=tbl_F_Info_Financiera)
	except:
		print("Sin informacion Actual")
		tbl_F_Info_Financiera=tbl_F_Info_Financiera.sort_values('Fecha_Efecto',ascending=False)

	#Guardar_csv(tbl_F_Info_Financiera, PathCarpetaResultados, f"{NIT}_tbl_F_Info_Financiera.csv")
	ing_tbl_F_Info_Financiera(conn,cursor,tbl_F_Info_Financiera)

	"""###tbl_F_Activos"""

	BalanceActivos = "AC"
	Id_Activo = None

	DicActivosOrdenado = ['Id_Info_Financiera', 'Fecha_Efecto', 'TOTAL ACTIVO', 'TOTAL ACTIVO CORRIENTE/ACC', 'CUENTAS POR COBRAR - DEUDORES/ACC', 'INVENTARIOS/ACC', 'DIFERIDOS/ACC', 'GASTOS PAGADOS POR ANTICIPADO/ACC', 'OTROS ACTIVOS/ACC', 'OTROS ACTIVOS FINANCIEROS/ACC', 'OTROS ACTIVOS NO FINANCIEROS/ACC', 'ACTIVOS POR IMPUESTOS CORRIENTES/ACC', 'ACTIVOS CLASIFICADOS COMO MANTENIDOS PAR/ACC', 'EFECTIVO Y EQUIVALENTES AL EFECTIVO/ACC', 'CUENTAS COMERCIALES POR COBRAR Y OTRAS C/ACC', 'CUENTAS POR COBRAR PARTES RELACIONADAS Y/ACC', 'TOTAL ACTIVO NO CORRIENTE/ACL', 'INVERSIONES/ACL', 'INVERSIONES EN SUBSIDIARIAS, NEGOCIOS CO/ACL', 'INVERSIONES CONTABILIZADAS UTILIZANDO EL/ACL', 'PROPIEDADES PLANTA Y EQUIPO/ACL', 'DIFERIDOS/ACL', 'GASTOS PAGADOS POR ANTICIPADO/ACL', 'OTROS ACTIVOS/ACL', 'PROPIEDAD DE INVERSIÓN/ACL', 'PLUSVALÍA/ACL', 'ACTIVOS INTANGIBLES DISTINTOS DE LA PLUS/ACL', 'ACTIVOS POR IMPUESTOS DIFERIDOS/ACL', 'INVERSIONES NO CORRIENTES/ACL', 'CUENTAS POR COBRAR NO CORRIENTES/ACL', 'CUENTAS COMERCIALES POR COBRAR Y OTRAS C/ACL', 'CUENTAS POR COBRAR PARTES RELACIONADAS Y/ACL', 'OTROS ACTIVOS NO FINANCIEROS/ACL', 'OTROS ACTIVOS FINANCIEROS/ACL', 'Fecha_Captura']
	dfActivos = Financiero_Activos(Fecha_Captura,Id_Cliente,tree,PathBalances, BalanceActivos, Id_Activo, Id_Info_Financiera)
	tbl_F_Activos = Validar_Formato_Tabla(dfActivos, DicActivosOrdenado)
	tbl_F_Activos.columns = ['Id_Info_Financiera','Fecha_Efecto','Total_Activos','Total_Activos_Cte','Cuentas_x_Cobrar_Cte','Inventarios_Cte','Diferidos_Cte','Gastos_Pagados_Ant_Cte','Otros_Activos_Cte','Otros_Activos_Financ_Cte','Otros_Activos_No_Financ_Cte','Activos_Imptos_Cte','Activos_Calsif_Mantenido_Venta_Cte','Efectivo_Equivalente_Cte','Cuentas_x_Cobrar_Otras_Cte','Cuentas_x_Cobrar_Partes_Rel_Cte','Total_Activos_No_Cte','Inversiones_No_Cte','Inversiones_Asociadas_No_Cte','Inversiones_Contabilizadas_No_Cte','Propiedad_Planta_Equipo_No_Cte','Diferidos_No_Cte','Gastos_Pagados_Anticipado_No_Cte','Otros_Activos_No_Cte','Propiedad_Inversion_No_Cte','Plusvalia_No_Cte','Activos_Intangibles_No_Plusv_No_Cte','Activos_Imptos_Diferido_No_Cte','Inv_No_Cte','Cuentas_x_Cobrar_No_Cte','Cunetas_x_Cobrar_Otras_No_Cte','Cuentas_x_Cobrar_Partes_Rel_No_Cte','Otros_Activos_No_Fro','Otros_Activos_Fro','Fecha_Captura']

	try:
		dfActivosActual = Financiero_Activos(Fecha_Captura,Id_Cliente,tree,PathBalancesActual, BalanceActivos, Id_Activo, Id_Info_Financiera)
		tbl_F_ActivosActual = Validar_Formato_Tabla(dfActivosActual, DicActivosOrdenado)
		DicActivosActualOrdenado = ['Id_Info_Financiera','Fecha_Efecto','Total_Activos','Total_Activos_Cte','Cuentas_x_Cobrar_Cte','Inventarios_Cte','Diferidos_Cte','Gastos_Pagados_Ant_Cte','Otros_Activos_Cte','Otros_Activos_Financ_Cte','Otros_Activos_No_Financ_Cte','Activos_Imptos_Cte','Activos_Calsif_Mantenido_Venta_Cte','Efectivo_Equivalente_Cte','Cuentas_x_Cobrar_Otras_Cte','Cuentas_x_Cobrar_Partes_Rel_Cte','Total_Activos_No_Cte','Inversiones_No_Cte','Inversiones_Asociadas_No_Cte','Inversiones_Contabilizadas_No_Cte','Propiedad_Planta_Equipo_No_Cte','Diferidos_No_Cte','Gastos_Pagados_Anticipado_No_Cte','Otros_Activos_No_Cte','Propiedad_Inversion_No_Cte','Plusvalia_No_Cte','Activos_Intangibles_No_Plusv_No_Cte','Activos_Imptos_Diferido_No_Cte','Inv_No_Cte','Cuentas_x_Cobrar_No_Cte','Cunetas_x_Cobrar_Otras_No_Cte','Cuentas_x_Cobrar_Partes_Rel_No_Cte','Otros_Activos_No_Fro','Otros_Activos_Fro','Fecha_Captura']
		tbl_F_ActivosActual.columns =  DicActivosActualOrdenado
		tbl_F_Activos = Append(df1=tbl_F_Activos,df2=tbl_F_ActivosActual)
		tbl_F_Activos = Validar_Formato_Tabla(tbl_F_Activos,DicActivosActualOrdenado)
	except:
		print("Sin Informacion Actual")

	DicActivosOrdenado = ['Id_Info_Financiera','Nit_Cliente','Fecha_Efecto','Total_Activos','Total_Activos_Cte','Cuentas_x_Cobrar_Cte','Inventarios_Cte','Diferidos_Cte','Gastos_Pagados_Ant_Cte','Otros_Activos_Cte','Otros_Activos_Financ_Cte','Otros_Activos_No_Financ_Cte','Activos_Imptos_Cte','Activos_Calsif_Mantenido_Venta_Cte','Efectivo_Equivalente_Cte','Cuentas_x_Cobrar_Otras_Cte','Cuentas_x_Cobrar_Partes_Rel_Cte','Total_Activos_No_Cte','Inversiones_No_Cte','Inversiones_Asociadas_No_Cte','Inversiones_Contabilizadas_No_Cte','Propiedad_Planta_Equipo_No_Cte','Diferidos_No_Cte','Gastos_Pagados_Anticipado_No_Cte','Otros_Activos_No_Cte','Propiedad_Inversion_No_Cte','Plusvalia_No_Cte','Activos_Intangibles_No_Plusv_No_Cte','Activos_Imptos_Diferido_No_Cte','Inv_No_Cte','Cuentas_x_Cobrar_No_Cte','Cunetas_x_Cobrar_Otras_No_Cte','Cuentas_x_Cobrar_Partes_Rel_No_Cte','Otros_Activos_No_Fro','Otros_Activos_Fro','Fecha_Captura']
	tbl_F_Activos["Nit_Cliente"] = NIT
	tbl_F_Activos = Validar_Formato_Tabla(tbl_F_Activos, DicActivosOrdenado)

	tbl_F_Activos = tbl_F_Activos.sort_values('Fecha_Efecto',ascending=False)
	tbl_F_Activos.replace({pd.np.NaN:None},inplace=True)
	tbl_F_Activos = tbl_F_Activos.astype(object).where(pd.notnull(tbl_F_Activos),None)
	#Guardar_csv(tbl_F_Activos, PathCarpetaResultados, f"{NIT}_tbl_F_Activos.csv")
	ing_tbl_F_Activos(conn,cursor,tbl_F_Activos)

	"""###tbl_F_Pasivos_Patrimonio"""

	Id_PasivoPatrimonio = np.nan
	BalancePasivosPatrimonio = "P"
	DicPasivosPatrimonioOrdenado = ['Id_PasivoPatrimonio', 'Id_Info_Financiera', 'Fecha_Efecto', 'PASIVO + PATRIMONIO', 'PASIVO/PS', 'PASIVO A CORTO PLAZO/PS/PSC', 'OBLIGACIONES FINANCIERAS/PS/PSC', 'PASIVOS ESTIMADOS Y PROVISIONES/PS/PSC', 'PROVISIONES DIVERSAS/PS/PSC', 'OTROS PASIVOS FINANCIEROS/PS/PSC', 'OTROS PASIVOS NO FINANCIEROS/PS/PSC', 'CUENTAS POR PAGAR CORRIENTE/PS/PSC', 'CUENTAS COMERCIALES POR PAGAR Y OTRAS CU/PS/PSC', 'CUENTAS POR PAGAR A ENTIDADES RELACIONAD/PS/PSC', 'PASIVOS POR IMPUESTOS CORRIENTES/PS/PSC', 'PROVISIONES CORRIENTES POR BENEFICIOS A/PS/PSC', 'OTROS PASIVOS CORRIENTES/PS/PSC', 'PASIVO A LARGO PLAZO/PS/PSL', 'PASIVOS ESTIMADOS Y PROVISIONES/PS/PSL', 'OTROS PASIVOS FINANCIEROS/PS/PSL', 'OTROS PASIVOS NO FINANCIEROS/PS/PSL', 'PASIVO POR IMPUESTOS DIFERIDOS/PS/PSL', 'OBLIGACIONES FINANCIEROS NO CORRIENTES/PS/PSL', 'PROVISIONES NO CORRIENTES POR BENEFICIOS/PS/PSL', 'OTRAS PROVISIONES/PS/PSL', 'OTROS PASIVOS NO CORRIENTES/PS/PSL', 'PATRIMONIO/PT', 'CAPITAL SOCIAL/PT', 'SUPERµVIT DE CAPITAL/PT', 'RESERVAS/PT', 'RESULTADO EJERCICIO/PT', 'COTIZACIONES-AUXIL./APORTES NO VINC./PT','OTROS RUBROS DEL PATRIMONIO/PT', 'ACCIONES PROPIAS EN CARTERA/PT', 'OTRO RESULTADO INTEGRAL ACUMULADO/PT', 'OTRAS PARTICIPACIONES EN EL PATRIMONIO/PT', 'PRIMA DE EMISIÓN/PT', 'GANANCIAS ACUMULADAS/PT', 'CAPITAL EMITIDO/PT', 'Fecha_Captura']

	dfPasivosPatrimonio = Financiero_Pasivos_Patrimonio(tree,Id_Cliente,Fecha_Captura,PathBalances, BalancePasivosPatrimonio, Id_PasivoPatrimonio, Id_Info_Financiera)
	tbl_F_Pasivos_Patrimonio = Validar_Formato_Tabla(dfPasivosPatrimonio, DicPasivosPatrimonioOrdenado)
	DicPasivosPatrimonioOrdenado = ['Id_Pasivo_Patrimonio','Id_Info_Financiera','Fecha_Efecto','Total_Pasivo_Patrimonio','Total_Pasivo','Total_Pasivo_Cte','Obligaciones_Fra','Pasivo_Est_Provi','Provi_Diversa','Otro_Pasivo_Fro','Otro_Pasivo_No_Fro','Cuentas_x_Pagar_Cte','Otras_Cuentas_x_Pagar_Cte','Cuentas_x_Pagar_Ent_Rel','Pasivo_Impto_Cte','Provi_Cte_Empleado','Otro_Pasivo_Cte','Total_Pasivo_No_Cte','Pasivo_Estimado_Provisiones_No_Cte','Otro_Pasivo_Fro_No_Cte','Otro_Pasivo_No_Fro_No_Cte','Pasivo_Impto_Diferido_No_Cte','Obligaciones_Fro_No_Cte','Provisiones_Beneficios_No_Cte','Otras_Provisiones_No_Cte','Otro_Pasivo_No_Cte','Patrimonio','Capital_Social_Pt','Superavit_Capital_Pt','Reserva_Pt','Resultado_Ejercicio_Pt','Cotiza_Aux_Aporte_No_Vinc_Pt','Otros_Rubros_Pt','Acciones_Propias_Cartera_Pt','Otro_Resultado_Integral_Acum_Pt','Otras_Participaciones_Pt','Primas_Emision_Pt','Ganancias_Acum_Pt','Capital_Emitido_Pt','Fecha_Captura']
	tbl_F_Pasivos_Patrimonio.columns = DicPasivosPatrimonioOrdenado
	DicPasivosPatrimonioOrdenado = ['Id_Info_Financiera','Fecha_Efecto','Total_Pasivo_Patrimonio','Total_Pasivo','Total_Pasivo_Cte','Obligaciones_Fra','Pasivo_Est_Provi','Provi_Diversa','Otro_Pasivo_Fro','Otro_Pasivo_No_Fro','Cuentas_x_Pagar_Cte','Otras_Cuentas_x_Pagar_Cte','Cuentas_x_Pagar_Ent_Rel','Pasivo_Impto_Cte','Provi_Cte_Empleado','Otro_Pasivo_Cte','Total_Pasivo_No_Cte','Pasivo_Estimado_Provisiones_No_Cte','Otro_Pasivo_Fro_No_Cte','Otro_Pasivo_No_Fro_No_Cte','Pasivo_Impto_Diferido_No_Cte','Obligaciones_Fro_No_Cte','Provisiones_Beneficios_No_Cte','Otras_Provisiones_No_Cte','Otro_Pasivo_No_Cte','Patrimonio','Capital_Social_Pt','Superavit_Capital_Pt','Reserva_Pt','Resultado_Ejercicio_Pt','Cotiza_Aux_Aporte_No_Vinc_Pt','Otros_Rubros_Pt','Acciones_Propias_Cartera_Pt','Otro_Resultado_Integral_Acum_Pt','Otras_Participaciones_Pt','Primas_Emision_Pt','Ganancias_Acum_Pt','Capital_Emitido_Pt','Fecha_Captura']
	tbl_F_Pasivos_Patrimonio = Validar_Formato_Tabla(tbl_F_Pasivos_Patrimonio, DicPasivosPatrimonioOrdenado)

	try:
		DicPasivosPatrimonioOrdenadoActual = ['Id_PasivoPatrimonio', 'Id_Info_Financiera', 'Fecha_Efecto', 'PASIVO + PATRIMONIO', 'PASIVO/PS', 'PASIVO A CORTO PLAZO/PS/PSC', 'OBLIGACIONES FINANCIERAS/PS/PSC', 'PASIVOS ESTIMADOS Y PROVISIONES/PS/PSC', 'PROVISIONES DIVERSAS/PS/PSC', 'OTROS PASIVOS FINANCIEROS/PS/PSC', 'OTROS PASIVOS NO FINANCIEROS/PS/PSC', 'CUENTAS POR PAGAR CORRIENTE/PS/PSC', 'CUENTAS COMERCIALES POR PAGAR Y OTRAS CU/PS/PSC', 'CUENTAS POR PAGAR A ENTIDADES RELACIONAD/PS/PSC', 'PASIVOS POR IMPUESTOS CORRIENTES/PS/PSC', 'PROVISIONES CORRIENTES POR BENEFICIOS A/PS/PSC', 'OTROS PASIVOS CORRIENTES/PS/PSC', 'PASIVO A LARGO PLAZO/PS/PSL', 'PASIVOS ESTIMADOS Y PROVISIONES/PS/PSL', 'OTROS PASIVOS FINANCIEROS/PS/PSL', 'OTROS PASIVOS NO FINANCIEROS/PS/PSL', 'PASIVO POR IMPUESTOS DIFERIDOS/PS/PSL', 'OBLIGACIONES FINANCIEROS NO CORRIENTES/PS/PSL', 'PROVISIONES NO CORRIENTES POR BENEFICIOS/PS/PSL', 'OTRAS PROVISIONES/PS/PSL', 'OTROS PASIVOS NO CORRIENTES/PS/PSL', 'PATRIMONIO/PT', 'CAPITAL SOCIAL/PT', 'SUPERµVIT DE CAPITAL/PT', 'RESERVAS/PT', 'RESULTADO EJERCICIO/PT', 'COTIZACIONES-AUXIL./APORTES NO VINC./PT','OTROS RUBROS DEL PATRIMONIO/PT', 'ACCIONES PROPIAS EN CARTERA/PT', 'OTRO RESULTADO INTEGRAL ACUMULADO/PT', 'OTRAS PARTICIPACIONES EN EL PATRIMONIO/PT', 'PRIMA DE EMISIÓN/PT', 'GANANCIAS ACUMULADAS/PT', 'CAPITAL EMITIDO/PT', 'Fecha_Captura']
		dfPasivosPatrimonioActual = Financiero_Pasivos_Patrimonio(tree,Id_Cliente,Fecha_Captura,PathBalancesActual, BalancePasivosPatrimonio, Id_PasivoPatrimonio, Id_Info_Financiera)
		tbl_F_Pasivos_PatrimonioActual = Validar_Formato_Tabla(dfPasivosPatrimonioActual, DicPasivosPatrimonioOrdenadoActual)
		DicPasivosPatrimonioOrdenadoActual = ['Id_Pasivo_Patrimonio','Id_Info_Financiera','Fecha_Efecto','Total_Pasivo_Patrimonio','Total_Pasivo','Total_Pasivo_Cte','Obligaciones_Fra','Pasivo_Est_Provi','Provi_Diversa','Otro_Pasivo_Fro','Otro_Pasivo_No_Fro','Cuentas_x_Pagar_Cte','Otras_Cuentas_x_Pagar_Cte','Cuentas_x_Pagar_Ent_Rel','Pasivo_Impto_Cte','Provi_Cte_Empleado','Otro_Pasivo_Cte','Total_Pasivo_No_Cte','Pasivo_Estimado_Provisiones_No_Cte','Otro_Pasivo_Fro_No_Cte','Otro_Pasivo_No_Fro_No_Cte','Pasivo_Impto_Diferido_No_Cte','Obligaciones_Fro_No_Cte','Provisiones_Beneficios_No_Cte','Otras_Provisiones_No_Cte','Otro_Pasivo_No_Cte','Patrimonio','Capital_Social_Pt','Superavit_Capital_Pt','Reserva_Pt','Resultado_Ejercicio_Pt','Cotiza_Aux_Aporte_No_Vinc_Pt','Otros_Rubros_Pt','Acciones_Propias_Cartera_Pt','Otro_Resultado_Integral_Acum_Pt','Otras_Participaciones_Pt','Primas_Emision_Pt','Ganancias_Acum_Pt','Capital_Emitido_Pt','Fecha_Captura']
		tbl_F_Pasivos_PatrimonioActual.columns = DicPasivosPatrimonioOrdenadoActual
		DicPasivosPatrimonioOrdenadoActual = ['Id_Info_Financiera','Fecha_Efecto','Total_Pasivo_Patrimonio','Total_Pasivo','Total_Pasivo_Cte','Obligaciones_Fra','Pasivo_Est_Provi','Provi_Diversa','Otro_Pasivo_Fro','Otro_Pasivo_No_Fro','Cuentas_x_Pagar_Cte','Otras_Cuentas_x_Pagar_Cte','Cuentas_x_Pagar_Ent_Rel','Pasivo_Impto_Cte','Provi_Cte_Empleado','Otro_Pasivo_Cte','Total_Pasivo_No_Cte','Pasivo_Estimado_Provisiones_No_Cte','Otro_Pasivo_Fro_No_Cte','Otro_Pasivo_No_Fro_No_Cte','Pasivo_Impto_Diferido_No_Cte','Obligaciones_Fro_No_Cte','Provisiones_Beneficios_No_Cte','Otras_Provisiones_No_Cte','Otro_Pasivo_No_Cte','Patrimonio','Capital_Social_Pt','Superavit_Capital_Pt','Reserva_Pt','Resultado_Ejercicio_Pt','Cotiza_Aux_Aporte_No_Vinc_Pt','Otros_Rubros_Pt','Acciones_Propias_Cartera_Pt','Otro_Resultado_Integral_Acum_Pt','Otras_Participaciones_Pt','Primas_Emision_Pt','Ganancias_Acum_Pt','Capital_Emitido_Pt','Fecha_Captura']
		tbl_F_Pasivos_PatrimonioActual = Validar_Formato_Tabla(tbl_F_Pasivos_PatrimonioActual, DicPasivosPatrimonioOrdenadoActual)
		tbl_F_Pasivos_Patrimonio = Append(df1=tbl_F_Pasivos_PatrimonioActual,df2=tbl_F_Pasivos_Patrimonio)
	except:
		print("Sin informacion Actual")

	DicPasivosPatrimonioOrdenado = ['Id_Info_Financiera','Nit_Cliente','Fecha_Efecto','Total_Pasivo_Patrimonio','Total_Pasivo','Total_Pasivo_Cte','Obligaciones_Fra','Pasivo_Est_Provi','Provi_Diversa','Otro_Pasivo_Fro','Otro_Pasivo_No_Fro','Cuentas_x_Pagar_Cte','Otras_Cuentas_x_Pagar_Cte','Cuentas_x_Pagar_Ent_Rel','Pasivo_Impto_Cte','Provi_Cte_Empleado','Otro_Pasivo_Cte','Total_Pasivo_No_Cte','Pasivo_Estimado_Provisiones_No_Cte','Otro_Pasivo_Fro_No_Cte','Otro_Pasivo_No_Fro_No_Cte','Pasivo_Impto_Diferido_No_Cte','Obligaciones_Fro_No_Cte','Provisiones_Beneficios_No_Cte','Otras_Provisiones_No_Cte','Otro_Pasivo_No_Cte','Patrimonio','Capital_Social_Pt','Superavit_Capital_Pt','Reserva_Pt','Resultado_Ejercicio_Pt','Cotiza_Aux_Aporte_No_Vinc_Pt','Otros_Rubros_Pt','Acciones_Propias_Cartera_Pt','Otro_Resultado_Integral_Acum_Pt','Otras_Participaciones_Pt','Primas_Emision_Pt','Ganancias_Acum_Pt','Capital_Emitido_Pt','Fecha_Captura']
	tbl_F_Pasivos_Patrimonio["Nit_Cliente"]=NIT
	tbl_F_Pasivos_Patrimonio = Validar_Formato_Tabla(tbl_F_Pasivos_Patrimonio, DicPasivosPatrimonioOrdenado)
	tbl_F_Pasivos_Patrimonio=tbl_F_Pasivos_Patrimonio.sort_values('Fecha_Efecto',ascending=False)

	#Guardar_csv(tbl_F_Pasivos_Patrimonio, PathCarpetaResultados, f"{NIT}_tbl_F_Pasivos_Patrimonio.csv")
	ing_tbl_F_Pasivos_Patrimonio(conn,cursor,tbl_F_Pasivos_Patrimonio)

	"""###tbl_F_Resultados_Ejercicio"""

	Id_Result_Ejercicio = np.nan
	BalanceResultados = "R"
	DicResultadosOrdenado = ['Id_Info_Financiera', 'Fecha_Efecto', 'RESULTADO DEL EJERCICIO', 'RESULTADO ANTES DE IMPUESTOS/R', 'RESULTADOS OPERACIONALES/R', 'TOTAL GASTOS/R', 'COSTOS Y GASTOS OPERACIONALES/R', 'GASTOS DE ADMINISTRACION/R', 'GASTOS DE VENTAS/R','GASTOS DE DISTRIBUCIÓN/R', 'GASTOS POR BENEFICIOS A LOS EMPLEADOS/R', 'OTROS GASTOS OPERATIVOS/R', 'COSTO DE VENTAS/R', 'NO OPERACIONALES/R', 'GASTOS FINANCIEROS/R', 'TOTAL INGRESOS/R', 'INGRESOS OPERACIONALES/R', 'VENTAS/R', 'OTROS INGRESOS OPERACIONALES/R', 'INGRESOS NO OPERACIONALES/R', 'INGRESOS EXTRAORDINARIOS/R', 'INGRESOS FINANCIEROS/R', 'RESULTADO NO OPERACIONAL/R', 'RESULTADO FINANCIERO/R', 'RESULTADO DE IMPUESTOS/R', 'AJUSTES POR INFLACIàN/R', 'IMPUESTO DE RENTA Y COMPLEMENTARIOS/R', 'Fecha_Captura']
	dfResultados = Financiero_Resultados(tree,Id_Cliente,Fecha_Captura,PathBalances, BalanceResultados, Id_Result_Ejercicio, Id_Info_Financiera)
	tbl_F_Resultados_Ejercicio = Validar_Formato_Tabla(dfResultados, DicResultadosOrdenado)
	DicResultadosOrdenado = ['Id_Info_Financiera','Fecha_Efecto','Resultado_Ejercicio','Resultado_Antes_Impto','Resultado_Op','Total_Gastos','Costos_Gastos_Op','Gastos_Op_Admin','Gastos_Op_Venta','Gastos_Dist','Gastos_Beneficio_Empl','Otros_Gastos_Op','Costos_Venta','Gastos_No_Op','Gastos_Fro','Total_Ingresos','Ingresos_Operacional','Ventas','Otros_Ingresos_Op','Ingresos_No_Op','Ingresos_Extraordinarios','Ingresos_Fro','Resultados_No_Op','Resultados_Fro','Resultados_Impuesto','Ajuste_Inflacion','Impto_Renta','Fecha_Captura']
	tbl_F_Resultados_Ejercicio.columns = DicResultadosOrdenado

	try:
		DicResultadosOrdenadoActual = ['Id_Info_Financiera', 'Fecha_Efecto', 'RESULTADO DEL EJERCICIO', 'RESULTADO ANTES DE IMPUESTOS/R', 'RESULTADOS OPERACIONALES/R', 'TOTAL GASTOS/R', 'COSTOS Y GASTOS OPERACIONALES/R', 'GASTOS DE ADMINISTRACION/R', 'GASTOS DE VENTAS/R','GASTOS DE DISTRIBUCIÓN/R', 'GASTOS POR BENEFICIOS A LOS EMPLEADOS/R', 'OTROS GASTOS OPERATIVOS/R', 'COSTO DE VENTAS/R', 'NO OPERACIONALES/R', 'GASTOS FINANCIEROS/R', 'TOTAL INGRESOS/R', 'INGRESOS OPERACIONALES/R', 'VENTAS/R', 'OTROS INGRESOS OPERACIONALES/R', 'INGRESOS NO OPERACIONALES/R', 'INGRESOS EXTRAORDINARIOS/R', 'INGRESOS FINANCIEROS/R', 'RESULTADO NO OPERACIONAL/R', 'RESULTADO FINANCIERO/R', 'RESULTADO DE IMPUESTOS/R', 'AJUSTES POR INFLACIàN/R', 'IMPUESTO DE RENTA Y COMPLEMENTARIOS/R', 'Fecha_Captura']
		dfResultadosActual = Financiero_Resultados(tree,Id_Cliente,Fecha_Captura,PathBalancesActual, BalanceResultados, Id_Result_Ejercicio, Id_Info_Financiera)
		tbl_F_Resultados_EjercicioActual = Validar_Formato_Tabla(dfResultadosActual, DicResultadosOrdenadoActual)
		DicResultadosOrdenadoActual = ['Id_Info_Financiera','Fecha_Efecto','Resultado_Ejercicio','Resultado_Antes_Impto','Resultado_Op','Total_Gastos','Costos_Gastos_Op','Gastos_Op_Admin','Gastos_Op_Venta','Gastos_Dist','Gastos_Beneficio_Empl','Otros_Gastos_Op','Costos_Venta','Gastos_No_Op','Gastos_Fro','Total_Ingresos','Ingresos_Operacional','Ventas','Otros_Ingresos_Op','Ingresos_No_Op','Ingresos_Extraordinarios','Ingresos_Fro','Resultados_No_Op','Resultados_Fro','Resultados_Impuesto','Ajuste_Inflacion','Impto_Renta','Fecha_Captura']
		tbl_F_Resultados_EjercicioActual.columns = DicResultadosOrdenadoActual
		tbl_F_Resultados_Ejercicio = Append(df1=tbl_F_Resultados_EjercicioActual,df2=tbl_F_Resultados_Ejercicio)
	except:
		print("Sin informacion actual")

	DicResultadosOrdenado = ['Id_Info_Financiera','Nit_Cliente','Fecha_Efecto','Resultado_Ejercicio','Resultado_Antes_Impto','Resultado_Op','Total_Gastos','Costos_Gastos_Op','Gastos_Op_Admin','Gastos_Op_Venta','Gastos_Dist','Gastos_Beneficio_Empl','Otros_Gastos_Op','Costos_Venta','Gastos_No_Op','Gastos_Fro','Total_Ingresos','Ingresos_Operacional','Ventas','Otros_Ingresos_Op','Ingresos_No_Op','Ingresos_Extraordinarios','Ingresos_Fro','Resultados_No_Op','Resultados_Fro','Resultados_Impuesto','Ajuste_Inflacion','Impto_Renta','Fecha_Captura']
	tbl_F_Resultados_Ejercicio['Nit_Cliente'] = int(NIT)
	tbl_F_Resultados_Ejercicio = Validar_Formato_Tabla(tbl_F_Resultados_Ejercicio, DicResultadosOrdenado)

	tbl_F_Resultados_Ejercicio=tbl_F_Resultados_Ejercicio.sort_values('Fecha_Efecto',ascending=False)

	#Guardar_csv(tbl_F_Resultados_Ejercicio, PathCarpetaResultados, f"{NIT}_tbl_F_Resultados_Ejercicio.csv")
	ing_tbl_F_Resultados_Ejercicio(conn,cursor,tbl_F_Resultados_Ejercicio)

	"""###tbl_F_Participantes"""

	Id_Participante = np.nan
	PathInfoCorporativa_Partic =    "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/VINCFINAN/PARTICIPACIONES/PARTICIPACION"
	dfInfoCorporativa_Partic = Extraer_Dataframe(Directorio,tree,PathInfoCorporativa_Partic)
	dicInfoCorporativa_Partic = ['RAZONSOCIAL', 'FEC_EFECTO', 'PORCENTAJE', 'IDENT_EMPRESA']
	dfInfoCorporativa_Partic = Validar_Formato_Tabla(dfInfoCorporativa_Partic, dicInfoCorporativa_Partic)
	dicInfoCorporativa_Partic = ['Nombre_Participante', 'Fecha_Efecto', 'Porcentaje', 'Doc_Participante']
	dfInfoCorporativa_Partic.columns = dicInfoCorporativa_Partic
	dfInfoCorporativa_Partic["Nit_Cliente"] = NIT
	dfInfoCorporativa_Partic["Fecha_Captura"] = Fecha_Captura
	dicInfoCorporativa_Partic = ['Nit_Cliente', 'Nombre_Participante', 'Doc_Participante', 'Porcentaje', 'Fecha_Efecto', 'Fecha_Captura']
	tbl_F_Participantes = Validar_Formato_Tabla(dfInfoCorporativa_Partic, dicInfoCorporativa_Partic)

	#Guardar_csv(tbl_F_Participantes, PathCarpetaResultados, f"{NIT}_tbl_F_Participantes.csv")
	ing_tbl_F_Participantes(conn,cursor,tbl_F_Participantes)

	"""###tbl_F_Accionistas"""

	Id_Accionista = np.nan
	PathInfoCorporativa_Accion = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/VINCFINAN/ACCIONISTAS/ACCIONISTA"
	dfInfoCorporativa_Accion = Extraer_Dataframe(Directorio,tree,PathInfoCorporativa_Accion)
	dicInfoCorporativa_Accion = ['NOMBRE', 'APELLIDO1', 'APELLIDO2','RAZONSOCIAL', 'FEC_EFECTO', 'ID_FISCAL']
	dfInfoCorporativa_Accion = Validar_Formato_Tabla(dfInfoCorporativa_Accion, dicInfoCorporativa_Accion)
	dicInfoCorporativa_Accion = ['NOMBRE_COMPLETO','RAZONSOCIAL', 'FEC_EFECTO', 'ID_FISCAL']
	dfInfoCorporativa_Accion = Combinar_Celdas(dfInfoCorporativa_Accion, dicInfoCorporativa_Accion)
	dicInfoCorporativa_Accion = ['Nombre_Accionista', 'Razon_Social', 'Fecha_Efecto', 'Doc_Accionista']
	dfInfoCorporativa_Accion.columns = dicInfoCorporativa_Accion
	dfInfoCorporativa_Accion['Nit_Cliente'] = int(NIT)
	dfInfoCorporativa_Accion['Fecha_Captura'] = Fecha_Captura
	dicInfoCorporativa_Accion = ['Nit_Cliente', 'Doc_Accionista', 'Nombre_Accionista', 'Razon_Social', 'Fecha_Efecto', 'Fecha_Captura']
	tbl_F_Accionistas = Validar_Formato_Tabla(dfInfoCorporativa_Accion, dicInfoCorporativa_Accion)
	#Guardar_csv(tbl_F_Accionistas, PathCarpetaResultados, f"{NIT}_tbl_F_Accionistas.csv")
	ing_tbl_F_Accionistas(conn,cursor,tbl_F_Accionistas)

	"""###tbl_F_Capital"""
	Id_Capital = np.nan

	PathInformacionComercial_Capital_Actual = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/CAPITAL/ACTUAL/SOCIAL"

	PathInformacionComercial_Capital_Anterior = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/CAPITAL/ANTERIOR/SOCIAL"

	dfInfoCorporativa_Capital_Actual = Extraer_Dataframe(Directorio,tree,PathInformacionComercial_Capital_Actual)

	dfInfoCorporativa_Capital_Anterior = Extraer_Dataframe(Directorio,tree,PathInformacionComercial_Capital_Anterior)

	dicInfoCorporativa_Capital = ['IMPORTE', 'FEC_EFECTO']

	dfInfoCorporativa_Capital_Actual = Validar_Formato_Tabla(dfInfoCorporativa_Capital_Actual, dicInfoCorporativa_Capital)

	dfInfoCorporativa_Capital_Anterior = Validar_Formato_Tabla(dfInfoCorporativa_Capital_Anterior, dicInfoCorporativa_Capital)

	frames = [dfInfoCorporativa_Capital_Actual, dfInfoCorporativa_Capital_Anterior]

	dfInfoCorporativa_Capital = pd.concat(frames)

	dicInfoCorporativa_Capital = ['Importe', 'Fecha_Efecto']
	dfInfoCorporativa_Capital.columns = dicInfoCorporativa_Capital
	dfInfoCorporativa_Capital['Nit_Cliente'] = int(NIT)
	dfInfoCorporativa_Capital['Fecha_Captura'] = Fecha_Captura
	dicInfoCorporativa_Capital = ['Nit_Cliente', 'Importe', 'Fecha_Efecto', 'Fecha_Captura']
	tbl_F_Capital = Validar_Formato_Tabla(dfInfoCorporativa_Capital, dicInfoCorporativa_Capital)
	ing_tbl_F_Capital(conn,cursor,tbl_F_Capital)

	"""###tbl_F_Administradores"""

	PathInformacionComercial_Fec_Actualiz = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ADMINISTRADORES/FEC_ACTUALIZACION"
	try:
		Fecha_Actualizacion = ConsultaElemento(root,PathInformacionComercial_Fec_Actualiz)
	except:
		Fecha_Actualizacion = np.nan

	Id_Administrador = np.nan

	PathInformacionComercial_Admin = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ADMINISTRADORES/ADMINISTRADOR"
	dicInformacionComercial_Admin = ["DESC_CARGO","NOMBRE","APELLIDO1", "APELLIDO2", "ID_VALOR", "FEC_EFECTO"]
	dfInformacionComercial_Admin = Extraer_Dataframe_Dic(Directorio,tree,root,PathInformacionComercial_Admin, dicInformacionComercial_Admin)
	dicInformacionComercial_Admin = ['DESC_CARGO/ADMINISTRADOR', 'NOMBRE/ADMINISTRADOR', 'APELLIDO1/ADMINISTRADOR', 'APELLIDO2/ADMINISTRADOR', 'ID_VALOR/ADMINISTRADOR', 'FEC_EFECTO/ADMINISTRADOR']
	dfInformacionComercial_Admin = Validar_Formato_Tabla(dfInformacionComercial_Admin, dicInformacionComercial_Admin)

	dicNombre = ["NOMBRE/ADMINISTRADOR", "APELLIDO1/ADMINISTRADOR", "APELLIDO2/ADMINISTRADOR"]
	dfNombre = dfInformacionComercial_Admin[dicNombre]
	dfNombre = dfNombre.replace({pd.np.nan: ""})
	dfNombre = Combinar_Registros(dfNombre, PathInformacionComercial_Admin)
	dicNombre = Validar_Formato_Tabla(dfNombre, dicNombre)
	dfInformacionComercial_Admin = dfInformacionComercial_Admin.drop(dicNombre, axis=1)
	dfInformacionComercial_Admin = Concatenar(df1=dfInformacionComercial_Admin, df2=dfNombre)

	dicInformacionComercial_Admin = ['Cargo_Administrador', 'Doc_Administrador', 'Fecha_Efecto', 'Nombre_Administrador']
	dfInformacionComercial_Admin.columns = dicInformacionComercial_Admin
	dfInformacionComercial_Admin["Nit_Cliente"] = int(NIT)
	dfInformacionComercial_Admin["Fecha_Captura"] = Fecha_Captura
	dfInformacionComercial_Admin["Fecha_Actualizacion"] = Fecha_Actualizacion
	dicInformacionComercial_Admin=['Nit_Cliente', 'Fecha_Actualizacion', 'Doc_Administrador', 'Nombre_Administrador', 'Cargo_Administrador', 'Fecha_Efecto', 'Fecha_Captura']
	tbl_F_Administradores = Validar_Formato_Tabla(dfInformacionComercial_Admin,dicInformacionComercial_Admin)
	#Guardar_csv(tbl_F_Administradores, PathCarpetaResultados, f"{NIT}_tbl_F_Administradores.csv")
	ing_tbl_F_Administradores(conn,cursor,tbl_F_Administradores)


	"""###tbl_F_Establecimientos"""

	PathEstalecimientos = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/SUCURSALES/SUCURSAL"
	try:
		dicEstablecimientos = ["ROTULO","DIR_DES_PROVINCIA","DES_TIPO_EXPLOTACION","FECHA_SITUACION"]
		dfEstablecimientos = Extraer_Dataframe_Dic(Directorio,tree,root,PathEstalecimientos, dicEstablecimientos)
		dicEstablecimientos = ["Nombre_Establecimiento", "Departamento","Tipo_Explotacion","Fecha_Efecto"]
		dfEstablecimientos.columns = dicEstablecimientos
		dfEstablecimientos["Nit_Cliente"] = NIT
		dfEstablecimientos["Fecha_Captura"] = Fecha_Captura
	except:
		dfEstablecimientos = pd.DataFrame()

	dicEstablecimientos = ['Nit_Cliente', 'Nombre_Establecimiento', 'Tipo_Explotacion', 'Departamento', 'Fecha_Efecto', 'Fecha_Captura']
	tbl_F_Establecimientos = Validar_Formato_Tabla(dfEstablecimientos, dicEstablecimientos)
	#Guardar_csv(tbl_F_Establecimientos, PathCarpetaResultados, f"{NIT}_tbl_F_Establecimientos.csv")
	ing_tbl_F_Establecimientos(conn,cursor,tbl_F_Establecimientos)

	"""###tbl_F_Incidencias"""

	dicIncidencias = ["FEC_EFECTO","DESC_MUNICIPIO","COD_INCIDENCIA","DES_INCIDENCIA","TEXTO","DEMANDANTE_RAZONSOCIAL"]
	try:
		PathIncidenciasVigentes = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/INCIDENCIAS/JUDICIALES/VIGENTES/INCIDENCIA"
		dfIncidenciasVigentes = Extraer_Dataframe_Dic(Directorio,tree,root,PathIncidenciasVigentes, dicIncidencias)
		dicIncidenciasVigentes = ["FEC_EFECTO/INCIDENCIA","DESC_MUNICIPIO/INCIDENCIA","COD_INCIDENCIA/INCIDENCIA","DES_INCIDENCIA/INCIDENCIA","TEXTO/INCIDENCIA","DEMANDANTE_RAZONSOCIAL/INCIDENCIA"]
		dfIncidenciasVigentes = Validar_Formato_Tabla(dfIncidenciasVigentes, dicIncidenciasVigentes)
		dfIncidenciasVigentes["Estado_Incidencia"] = "VIGENTE"
	except:
		#print("Sin incidencias vigentes")
		dfIncidenciasVigentes = pd.DataFrame()

	try:
		PathIncidenciasFin = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/INCIDENCIAS/JUDICIALES/FINALIZADAS/INCIDENCIA"
		dfIncidenciasFin = Extraer_Dataframe_Dic(Directorio,tree,root,PathIncidenciasFin, dicIncidencias)
		dicIncidenciasFin = ["FEC_EFECTO/INCIDENCIA","DESC_MUNICIPIO/INCIDENCIA","COD_INCIDENCIA/INCIDENCIA","DES_INCIDENCIA/INCIDENCIA","TEXTO/INCIDENCIA","DEMANDANTE_RAZONSOCIAL/INCIDENCIA"]
		dfIncidenciasFin = Validar_Formato_Tabla(dfIncidenciasFin, dicIncidenciasFin)
		dfIncidenciasFin['Estado_Incidencia'] = 'FINALIZADO'
	except:
		#print("Sin incidencias finalizadas")
		dfIncidenciasFin = pd.DataFrame()

	tbl_F_Incidencias = Append(df1=dfIncidenciasFin,df2=dfIncidenciasVigentes)
	dicIncidencias = ["Fecha_Efecto","Municipio","Cod_Incidencia","Tipo_Incidencia","Descripcion_Incidencia","Demandante", "Estado_Incidencia"]
	tbl_F_Incidencias.columns = dicIncidencias

	try:
		PathIncidenciasTotal = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/INCIDENCIAS/JUDICIALES/VIGENTES/NUMTOTAL"
		IncidenciasTotal = ConsultaElemento(root,PathIncidenciasTotal)
		tbl_F_Incidencias["Total_Incidencias"] = IncidenciasTotal
	except:
		tbl_F_Incidencias["Total_Incidencias"] = np.nan

	tbl_F_Incidencias["Fecha_Captura"] = Fecha_Captura
	tbl_F_Incidencias["Nit_Cliente"] = NIT

	dicIncidencias = ["Nit_Cliente","Fecha_Efecto","Estado_Incidencia","Municipio","Cod_Incidencia","Tipo_Incidencia","Descripcion_Incidencia","Demandante","Total_Incidencias","Fecha_Captura"]
	tbl_F_Incidencias = Validar_Formato_Tabla(tbl_F_Incidencias,dicIncidencias)
	#Guardar_csv(tbl_F_Incidencias, PathCarpetaResultados, f"{NIT}_tbl_F_Incidencias.csv")
	ing_tbl_F_Incidencias(conn,cursor,tbl_F_Incidencias)

	"""###Actividad Comercial"""

	PathActividades_codigo = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ACTIVIDADES/CODIGO"
	dicActividades = ["CODIGO","DESC_FORMATO_LOCAL"]
	dfActividades = Extraer_Dataframe_Actividades(tree,PathActividades_codigo, dicActividades)
	dicActividades = ["Cod_Actividad","Descripcion_Actividad", "Tipo_Actividad"]
	dfActividades.columns = dicActividades
	dfActividades["Nit_Cliente"]=NIT
	dfActividades["Fecha_Captura"]=Fecha_Captura
	dicActividades = ["Nit_Cliente","Tipo_Actividad","Cod_Actividad","Descripcion_Actividad","Fecha_Captura"]
	tbl_F_Actividades = Validar_Formato_Tabla(dfActividades,dicActividades)
	ing_tbl_F_Actividades(conn,cursor,tbl_F_Actividades)

	"""###Actividad Comercial Exterior"""

	ActividadImportacion = "IMPORTA"
	ActividadExportacion = "EXPORTA"
	dicActividadExterior = ["Anno","Fecha_Efecto","Producto","Valor","Divisa", "Pais"]

	PathActividadActual = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ACTEXTERNA/ACTUAL"
	try:
		dfActividadImportacionActual = Actividad_Exterior(tree,PathActividadActual, ActividadImportacion)
		dfActividadExportacionActual = Actividad_Exterior(tree,PathActividadActual, ActividadExportacion)
	except:
		dfActividadImportacionActual = pd.DataFrame()
		dfActividadExportacionActual = pd.DataFrame()
		#print("Sin Actividad Actual")

	PathActividadAnterior = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/ACTEXTERNA/ANTERIOR"
	try:
		dfActividadImportacionAnterior = Actividad_Exterior(tree,PathActividadAnterior, ActividadImportacion)
		dfActividadExportacionAnterior = Actividad_Exterior(tree,PathActividadAnterior, ActividadExportacion)
	except:
		dfActividadImportacionAnterior = pd.DataFrame()
		dfActividadExportacionAnterior = pd.DataFrame()
		#print("Sin Actividad Actual")

	tbl_F_Importaciones = Append(df1=dfActividadImportacionActual,df2=dfActividadImportacionAnterior)
	if tbl_F_Importaciones.empty:
		tbl_F_Importaciones = Validar_Formato_Tabla(tbl_F_Importaciones,dicActividadExterior)
	else:
		tbl_F_Importaciones.columns = dicActividadExterior

	tbl_F_Exportaciones = Append(df1=dfActividadExportacionActual,df2=dfActividadExportacionAnterior)
	if tbl_F_Exportaciones.empty:
		tbl_F_Exportaciones = Validar_Formato_Tabla(tbl_F_Exportaciones, dicActividadExterior)
	else:
		tbl_F_Exportaciones.columns = dicActividadExterior

	tbl_F_Importaciones["Fecha_Captura"] = Fecha_Captura
	tbl_F_Exportaciones["Fecha_Captura"] = Fecha_Captura
	tbl_F_Importaciones["Nit_Cliente"] = NIT
	tbl_F_Exportaciones["Nit_Cliente"] = NIT

	dicActividadExterior = ["Nit_Cliente","Anno", "Fecha_Efecto","Producto","Pais","Valor","Divisa", "Fecha_Captura"]
	tbl_F_Importaciones = Validar_Formato_Tabla(tbl_F_Importaciones, dicActividadExterior)
	tbl_F_Exportaciones = Validar_Formato_Tabla(tbl_F_Exportaciones, dicActividadExterior)
	#Guardar_csv(tbl_F_Importaciones, PathCarpetaResultados, f"{NIT}_tbl_F_Importaciones.csv")
	#Guardar_csv(tbl_F_Exportaciones, PathCarpetaResultados, f"{NIT}_tbl_F_Exportaciones.csv")
	ing_tbl_F_Importaciones(conn,cursor,tbl_F_Importaciones)
	ing_tbl_F_Exportaciones(conn,cursor,tbl_F_Exportaciones)

	"""###Obligaciones"""

	PathObligaciones = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/OBLIGACIONES"
	dicObligaciones = ["Periodo_Obligacion","Fecha_Ejecucion_Obligacion","Fuente_Obligacion","Situacion_Obligacion","Tipo_Obligacion"]
	dfObligaciones = Extraer_Dataframe_Obligaciones(tree,PathObligaciones,"DES_SITU","DES_TIPO")
	dfObligaciones.columns = dicObligaciones
	dfObligaciones["Nit_Cliente"] = NIT
	dfObligaciones["Fecha_Captura"] = Fecha_Captura
	dicObligaciones = ["Nit_Cliente","Tipo_Obligacion","Periodo_Obligacion","Situacion_Obligacion","Fecha_Ejecucion_Obligacion","Fuente_Obligacion","Fecha_Captura"]
	tbl_F_Obligaciones = Validar_Formato_Tabla(dfObligaciones,dicObligaciones)
	ing_tbl_F_Obligaciones(conn,cursor,tbl_F_Obligaciones)

	"""###Politica Comercial"""

	dicPolitica = ["Nit_Cliente","Tipo_Pol_Ccial","Producto_Pol_Ccial","Politica_Pol_CCial","Fecha_Efecto_Pol_Ccial","Porc_Nacional_Pol_Ccial","Porc_Internacional_Pol_Ccial","Fecha_Captura"]

	try:
		PathPoliticaVentas = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/POLVENTAS/VENTAS"
		dfPoliticaVentas = Extraer_Dataframe_Politica_Ccial(NIT,Fecha_Captura,Directorio,tree,PathPoliticaVentas)
		dfPoliticaVentas = Validar_Formato_Tabla(dfPoliticaVentas,dicPolitica)
		#print(dfPoliticaVentas)
	except:
		PathPoliticaVentas = pd.DataFrame(columns=dicPolitica)

	try:
		PathPoliticaCompra = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/POLVENTAS/COMPRAS"
		dfPoliticaCompra = Extraer_Dataframe_Politica_Ccial(NIT,Fecha_Captura,Directorio,tree,PathPoliticaCompra)
		dfPoliticaCompra = Validar_Formato_Tabla(dfPoliticaCompra,dicPolitica)
		#Print(dfPoliticaCompra)
	except:
		PathPoliticaCompra = pd.DataFrame(columns=dicPolitica)

	try:
		tbl_F_Politica_Comercial = Append(df1=dfPoliticaCompra,df2=dfPoliticaVentas)
	except:
		tbl_F_Politica_Comercial = pd.DataFrame(columns=dicPolitica)


	ing_tbl_F_Politica_Comercial(conn,cursor,tbl_F_Politica_Comercial)

	"""###Publicaciones de Prensa"""

	try:
		PathPublicaciones_Prensa = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/PRENSA/ARTICULO"
		dicPublicaciones_Prensa = ["FEC_ARTICULO","FUENTE_ARTICULO_LOCAL","TEXTO_LOCAL"]
		dfPublicaciones_Prensa = Extraer_Dataframe_Dic(Directorio,tree,root,PathPublicaciones_Prensa, dicPublicaciones_Prensa)
		dicPublicaciones_Prensa = ["Fecha_Publicacion","Fuente","Resumen_Publicacion"]
		dfPublicaciones_Prensa.columns = dicPublicaciones_Prensa
		dfPublicaciones_Prensa["Nit_Cliente"]=NIT
		dfPublicaciones_Prensa["Tipo_Articulo"]="Prensa"
		dfPublicaciones_Prensa["Fecha_Captura"]=Fecha_Captura
		dicPublicaciones_Prensa = ["Nit_Cliente","Fecha_Publicacion","Fuente","Tipo_Articulo","Resumen_Publicacion","Fecha_Captura"]
		tbl_F_Publicaciones_Prensa = Validar_Formato_Tabla(dfPublicaciones_Prensa,dicPublicaciones_Prensa)
	except:
		tbl_F_Publicaciones_Prensa = pd.DataFrame()

	#Guardar_csv(tbl_F_Publicaciones_Prensa, PathCarpetaResultados, f"{NIT}_tbl_F_Publicaciones_Prensa.csv")
	ing_tbl_F_Publicaciones_Prensa(conn,cursor,tbl_F_Publicaciones_Prensa)

	"""###Publicaciones Legales"""

	try:
		PathPublicaciones_Legales = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/PBLC_LEGAL/PUBLICACION"
		dicPublicaciones_Legales = ["FEC_EFECTO","TIPO_ACTO","DES_TIPO_ACTO","FUENTE","DES_FUENTE"]
		dfPublicaciones_Legales = Extraer_Dataframe_Dic(Directorio,tree,root,PathPublicaciones_Legales, dicPublicaciones_Legales)
		dicPublicaciones_Legales = ["Fecha_Acto","Tipo_Acto","Referencia","Fuente","Lugar_Publicacion"]
		dfPublicaciones_Legales.columns = dicPublicaciones_Legales
		dfPublicaciones_Legales["Nit_Cliente"]=NIT
		dfPublicaciones_Legales["Fecha_Captura"]=Fecha_Captura
		dicPublicaciones_Legales = ["Nit_Cliente","Tipo_Acto","Fecha_Acto","Referencia","Fuente","Lugar_Publicacion","Fecha_Captura"]
		tbl_F_Publicaciones_Legales = Validar_Formato_Tabla(dfPublicaciones_Legales,dicPublicaciones_Legales)
	except:
		tbl_F_Publicaciones_Legales = pd.DataFrame()

	#Guardar_csv(tbl_F_Publicaciones_Legales, PathCarpetaResultados, f"{NIT}_tbl_F_Publicaciones_Legales.csv")
	ing_tbl_F_Publicaciones_Legales(conn,cursor,tbl_F_Publicaciones_Legales)

	"""###Relaciones Terceros"""

	dicRelaciones_Terceros = ["RAZONSOCIAL", "IDENT_EMPRESA"]
	dicRelaciones_Terceros2 = ["Razon_Social","Nit_Razon_Social"]

	try:
		PathRelaciones_Terceros_Bancos = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/RELTERCEROS/BANCOS/BANCO"
		dfRelaciones_Terceros_Bancos = Extraer_Dataframe_Dic(Directorio,tree,root,PathRelaciones_Terceros_Bancos,dicRelaciones_Terceros)
		dfRelaciones_Terceros_Bancos.columns = dicRelaciones_Terceros2
		dfRelaciones_Terceros_Bancos["Tipo_Relacion"]="BANCO"
	except:
		dfRelaciones_Terceros_Bancos = pd.DataFrame()

	try:
		PathRelaciones_Terceros_Clientes = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/RELTERCEROS/CLIENTES/CLIENTE"
		dfRelaciones_Terceros_Clientes = Extraer_Dataframe_Dic(Directorio,tree,root,PathRelaciones_Terceros_Clientes,dicRelaciones_Terceros)
		dfRelaciones_Terceros_Clientes.columns = dicRelaciones_Terceros2
		dfRelaciones_Terceros_Clientes["Tipo_Relacion"]="CLIENTE"
	except:
		dfRelaciones_Terceros_Clientes = pd.DataFrame()

	try:
		PathRelaciones_Terceros_Proveedores = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/RELTERCEROS/PROVEEDORES/PROVEEDOR"
		dfRelaciones_Terceros_Proveedores = Extraer_Dataframe_Dic(Directorio,tree,root,PathRelaciones_Terceros_Proveedores,dicRelaciones_Terceros)
		dfRelaciones_Terceros_Proveedores.columns = dicRelaciones_Terceros2
		dfRelaciones_Terceros_Proveedores["Tipo_Relacion"]="PROVEEDOR"
	except:
		dfRelaciones_Terceros_Proveedores = pd.DataFrame()

	try:
		PathRelaciones_Terceros_Aseguradoras = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/RELTERCEROS/ASEGURADORAS/ASEGURADORA"
		dfRelaciones_Terceros_Aseguradoras = Extraer_Dataframe_Dic(Directorio,tree,root,PathRelaciones_Terceros_Aseguradoras,dicRelaciones_Terceros)
		dfRelaciones_Terceros_Aseguradoras.columns = dicRelaciones_Terceros2
		dfRelaciones_Terceros_Aseguradoras["Tipo_Relacion"]="ASEGURADORA"
	except:
		dfRelaciones_Terceros_Aseguradoras = pd.DataFrame()

	tbl_F_Relaciones_Terceros = Append(df1=dfRelaciones_Terceros_Bancos,df2=dfRelaciones_Terceros_Clientes,df3=dfRelaciones_Terceros_Proveedores,df4=dfRelaciones_Terceros_Aseguradoras)
	tbl_F_Relaciones_Terceros["Nit_Cliente"]=NIT
	tbl_F_Relaciones_Terceros["Fecha_Captura"]=Fecha_Captura
	dicRelaciones_Terceros = ["Nit_Cliente","Tipo_Relacion","Razon_Social","Nit_Razon_Social","Fecha_Captura"]
	tbl_F_Relaciones_Terceros = Validar_Formato_Tabla(tbl_F_Relaciones_Terceros,dicRelaciones_Terceros)
	ing_tbl_F_Relaciones_Terceros(conn,cursor,tbl_F_Relaciones_Terceros)

	try:
		dicIndicadores = ['Fecha_Efecto_Indicador_Fro','VENTAS/EVOLUCION','RESULTADOS/EVOLUCION','RENTABILIDAD/RENTABILIDAD','OPERACIONAL/RENTABILIDAD','PATRIMONIO/RENTABILIDAD','ACTIVO/RENTABILIDAD','COBERTURA/RENTABILIDAD','EBIT/RENTABILIDAD','EBITDA/RENTABILIDAD','ENDEUDAMIENTO/ENDEUDAMIENTO','CORTO_PLAZO/ENDEUDAMIENTO','SIN_VALORIZACION/ENDEUDAMIENTO','APALANCAMIENTO/ENDEUDAMIENTO','CARGA_FINANCIERA/ENDEUDAMIENTO','CAPITAL_TRABAJO/LIQUIDEZ','RAZON_CORRIENTE/LIQUIDEZ','PRUEBA_ACIDA/LIQUIDEZ','ROTACION_INVENTARIO/EFICIENCIA','CICLO_OPERACIONAL/EFICIENCIA','ROTACION_ACTIVOS/EFICIENCIA']
		dicIndicadoresFinancieros = ['Fecha_Efecto_Indicador_Fro','Evolucion_Ventas','Evolucion_Utilidad_Neta','Rentabilidad','Rentabilidad_Operacional','Rentabilidad_Patrimonio','Rentabilidad_Activo_Total','Cobertura_Gastos_Fro','EBIT','EBITDA','Endeudamiento','Concentracion_Corto_Plazo','Endeudamiento_Sin_Valorizacion','Apalancamiento_Fro','Carga_Fra','Capital_Trabajo','Razon_Cte','Prueba_Acida','Dias_Rotacion_Inventario','Dias_Ciclo_Operacional','Rotacion_Activos']
		PathIndicadoresFinancieros = "./PRODUCTO_DEVUELTO/DATOS_PROD_DEVUELTO/INFORME_FINANCIERO_INTERNACIONAL/BALANCES/RATIOS/EJERCICIO"
		#print("EL PATH FRO ES....")
		#print(PathIndicadoresFinancieros)
		dfIndicadoresFinancieros = Financiero_Indicadores(tree,PathIndicadoresFinancieros, dicIndicadores)
		dfIndicadoresFinancieros.columns = dicIndicadoresFinancieros
		dfIndicadoresFinancieros["Nit_Cliente"]=NIT
		dfIndicadoresFinancieros["Fecha_Captura"]=Fecha_Captura
		dicIndicadoresFinancieros = ['Nit_Cliente','Fecha_Efecto_Indicador_Fro','Evolucion_Ventas','Evolucion_Utilidad_Neta','Rentabilidad','Rentabilidad_Operacional','Rentabilidad_Patrimonio','Rentabilidad_Activo_Total','Cobertura_Gastos_Fro','EBIT','EBITDA','Endeudamiento','Concentracion_Corto_Plazo','Endeudamiento_Sin_Valorizacion','Apalancamiento_Fro','Carga_Fra','Capital_Trabajo','Razon_Cte','Prueba_Acida','Dias_Rotacion_Inventario','Dias_Ciclo_Operacional','Rotacion_Activos','Fecha_Captura']
		tbl_F_Indicadores_Financieros = Validar_Formato_Tabla(dfIndicadoresFinancieros,dicIndicadoresFinancieros)
	except:
		tbl_F_Indicadores_Financieros = pd.DataFrame(columns=dicIndicadoresFinancieros)
		print("Error en el Indicador Financiero, no tiene")

	#print("LOS VALORES SON")
	#print(tbl_F_Indicadores_Financieros)


	ing_tbl_F_Indicadores_Financieros(conn,cursor,tbl_F_Indicadores_Financieros)

	Uptate_Tbls_Financieras(conn,cursor)

	Break_conn(conn,cursor)

	cursor.close()
	conn.close()





