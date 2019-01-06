import pandas as pd
import sqlite3
import datetime
import sys
import time
import numpy as np
import tkinter as tk
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

##import xslxwriter (es necesario hacer "pip3 install xlsxwriter")


## IMPORTANDO .CSV A NUESTRO ESPACIO DE TRABAJO EN PANDAS ##

#df = pd.read_csv("Gastos.csv")
#df2 = pd.read_csv("M.O Temporal.csv")
actualiza = int(input("quieres actualizar los datos? 1 = Si; 0 = No "))
if actualiza == 1:
    #ahora = datetime.datetime.now()
    f    = open("M.O Temporal.csv","rb")
    text = f.read().decode(errors='ignore')
    f2    = open("bodegas.csv","rb")
    text2 = f2.read().decode(errors='ignore')
    f3   = open("Arriendo Maq.csv","rb")
    text3 = f3.read().decode(errors='ignore')
    f4   = open("Gastos.csv","rb")
    text4 = f4.read().decode(errors='ignore')
    f5   = open("M.O  Permanente.csv","rb")
    text5 = f5.read().decode(errors='ignore')


    TESTDATA = StringIO(text)
    TESTDATA2 = StringIO(text2)
    TESTDATA3 = StringIO(text3)
    TESTDATA4 = StringIO(text4)
    TESTDATA5 = StringIO(text5)

    df = pd.read_csv(TESTDATA, sep=";")
    df2 = pd.read_csv(TESTDATA2, sep=";")
    df3 = pd.read_csv(TESTDATA3, sep=";")
    df4 = pd.read_csv(TESTDATA4, sep=";")
    df5 = pd.read_csv(TESTDATA5, sep=";")

    #REVISAR LAS KEYS QUE TIENEN ESPACIO ALGUNAS
    df.dropna(subset=[' Fecha'], inplace=True)
    df.dropna(subset=[" Valor "],inplace=True)
    df2.dropna(subset=["Fechas Salida"],inplace=True)
    df2.dropna(subset=[" $ salida "],inplace=True)
    df3.dropna(subset=["Fecha"],inplace=True)
    df3.dropna(subset=["  Total  "],inplace=True)
    df4.dropna(subset=["Fecha "],inplace=True)
    df4.dropna(subset=[" Total "],inplace=True)
    df5.dropna(subset=[" Fecha "],inplace=True)
    df5.dropna(subset=[" Costo Empresa "],inplace=True)

    #despues = datetime.datetime.now()
    #despues = datetime.datetime.now()
    #print("se demora {} ".format(despues-ahora))
    def crear_tabla_gastos(df4):
        connection = sqlite3.connect('gastos.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS gastos")
        cursor.execute("CREATE TABLE IF NOT EXISTS gastos(id INTEGER,fecha DATE,valor INTEGER,Clasificacion VARCHAR(30),lugar VARCHAR(30),empresa VARCHAR(30),cultivo VARCHAR(30),CONSTRAINT Keytemporal PRIMARY KEY(id))")
        datos = []
        ID = 0
        forma = 0
        errores = 0
        for index, row in df4.iterrows():
            try:
                a = datetime.datetime.strptime(row[0][3:], '%m-%y')
                b = a.strftime('%y-%m')
                j = str(row[12]).replace(".","").strip(" ")
                if j != "":
                    if j[0]=="-" or j[0]== "$":
                        if len(j[1:])==0:
                            j = 0
                        else:
                            j = int(j[1:])*-1
                    elif "," in j:
                        j = int(j[:j.find(",")])
                    else:
                        if j == "nan":
                            j = 0
                        else:
                            try:
                                j = int(j)
                            except ValueError:
                                j = 0
            except AttributeError:
                errores+=1
            try:
                row[5]=row[5].upper()
                row[5]=row[5].strip(" ")
            except AttributeError:
                errores+=1
            try:
                row[6]=row[6].upper()
            except AttributeError:
                errores+=1
            try:
                row[18]=row[18].upper()
            except AttributeError:
                errores+=1
            try:
                row[8]=row[8].upper()
            except AttributeError:
                errores+=1

            datos.append((index,b,j,row[5],row[6],row[18],row[8]))

        cursor.executemany("INSERT INTO gastos VALUES(?,?,?,?,?,?,?)", datos)
        connection.commit()
        connection.close()


    def crear_tabla_maquinas(df3):
        connection = sqlite3.connect('maquinas.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS maquinas")
        cursor.execute("CREATE TABLE IF NOT EXISTS maquinas(id INTEGER,fecha DATE,valor INTEGER,Clasificacion VARCHAR(30),lugar VARCHAR(30),empresa VARCHAR(30),cultivo VARCHAR(30),CONSTRAINT Keytemporal PRIMARY KEY(id))")
        datos = []
        ID = 0
        forma = 0
        errores = 0
        for index, row in df3.iterrows():

            a = datetime.datetime.strptime(row[2][3:], '%m-%y')
            b = a.strftime('%y-%m')
            j = str(row[20]).replace(".","").strip(" ")
            if j[0]=="-" or j[0]== "$":
                if len(j[1:])==0:
                    j = 0
                else:
                    j = int(j[1:])*-1
            elif "," in j:
                j = int(j[:j.find(",")])
            else:
                if j == "nan":
                    j = 0
                else:
                    try:
                        j = int(j)
                    except ValueError:
                        j = 0
            try:
                row[5]=row[5].upper()
                row[5]=row[5].strip(" ")
            except AttributeError:
                errores+=1
            try:
                row[7]=row[7].upper()
            except AttributeError:
                errores+=1
            try:
                row[21]=row[21].upper()
            except AttributeError:
                errores+=1
            try:
                row[8]=row[8].upper()
            except AttributeError:
                errores+=1
            datos.append((index,b,j,row[5],row[7],row[21],row[8]))

        cursor.executemany("INSERT INTO maquinas VALUES(?,?,?,?,?,?,?)", datos)
        connection.commit()
        connection.close()


    def crear_tabla_bodega(df2):
        connection = sqlite3.connect('bodega.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS bodega")
        cursor.execute("CREATE TABLE IF NOT EXISTS bodega(id INTEGER,fecha DATE,valor INTEGER,Clasificacion VARCHAR(30),lugar VARCHAR(30),empresa VARCHAR(30),cultivo VARCHAR(30),CONSTRAINT Keytemporal PRIMARY KEY(id))")
        datos = []
        ID = 0
        forma = 0
        errores = 0
        for index, row in df2.iterrows():
            a = datetime.datetime.strptime(row[9][3:], '%m-%y')
            b = a.strftime('%y-%m')
            j = str(row[18]).replace(".","").strip(" ")
            if j[0]=="-" or j[0]== "$":
                if len(j[1:])==0:
                    j = 0
                else:
                    j = int(j[1:])*-1
            elif "," in j:
                j = int(j[:j.find(",")])
            else:
                if j == "nan":
                    j = 0
                else:
                    try:
                        j = int(j)
                    except ValueError:
                        j = 0
            try:
                row[2]=row[2].upper()
                row[2]=row[2].strip(" ")
            except AttributeError:
                errores+=1
            try:
                row[13]=row[13].upper()
            except AttributeError:
                errores+=1
            try:
                row[12]=row[12].upper()
            except AttributeError:
                errores+=1
            try:
                row[14]=row[14].upper()
            except AttributeError:
                errores+=1

            datos.append((index,b,j,row[2],row[13],row[12],row[14]))

        cursor.executemany("INSERT INTO bodega VALUES(?,?,?,?,?,?,?)", datos)
        connection.commit()
        connection.close()

    def crear_tabla_temporal(df):
        connection = sqlite3.connect('temporal.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS temporal")
        cursor.execute("CREATE TABLE IF NOT EXISTS temporal(id INTEGER,fecha DATE,valor INTEGER,Clasificacion_labor VARCHAR(30),lugar VARCHAR(30),clasif_cultivo VARCHAR(30),cod VARCHAR(30),empresa VARCHAR(30),CONSTRAINT Keytemporal PRIMARY KEY(id))")
        datos = []
        ID = 0
        forma = 0
        errores = 0
        for index, row in df.iterrows():
            a = datetime.datetime.strptime(row[0][3:], '%m-%y')
            b = a.strftime('%y-%m')
            j = str(row[5]).replace(".","").strip(" ")
            if j[0]=="-" or j[0]== "$":
                if len(j[1:])==0:
                    j = 0
                else:
                    j = int(j[1:])*-1
            elif "," in j:
                j = int(j[:j.find(",")])
            else:
                if j == "nan":
                    j = 0
                else:
                    try:
                        j = int(j)
                    except ValueError:
                        j = 0
            try:
                row[7]=row[7].strip(" ")
                row[7]=row[7].upper()
            except AttributeError:
                errores+=1
            try:
                row[8]=row[8].upper()
            except AttributeError:
                errores+=1
            try:
                row[11]=row[11].upper()
            except AttributeError:
                errores+=1
            try:
                row[13]=row[13].upper()
            except AttributeError:
                errores+=1
            datos.append((index,b,j,row[7].strip(),row[8],row[11],row[12],row[13].strip(" ")))

        cursor.executemany("INSERT INTO temporal VALUES(?,?,?,?,?,?,?,?)", datos)
        connection.commit()
        connection.close()
    def crear_tabla_permanente(df5):
        connection = sqlite3.connect('permanente.db',detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS permanente")
        cursor.execute("CREATE TABLE IF NOT EXISTS permanente(id INTEGER,fecha DATE,valor INTEGER,empresa VARCHAR(30),CONSTRAINT Keytemporal PRIMARY KEY(id))")
        datos = []
        ID = 0
        forma = 0
        errores = 0
        for index, row in df5.iterrows():
            a = datetime.datetime.strptime(row[0][3:], '%m-%y')
            b = a.strftime('%y-%m')
            j = str(row[22]).replace(".","").strip(" ")
            if j[0]=="-" or j[0]== "$":
                if len(j[1:])==0:

                    j = 0
                else:
                    j = int(j[1:])*-1
            elif "," in j:
                j = int(j[:j.find(",")])
            else:
                if j == "nan":

                    j = 0
                else:
                    try:
                        j = int(j)

                    except ValueError:

                        j = 0
            try:
                row[2]=row[2].strip(" ")
                row[2]=row[2].upper()
            except AttributeError:
                errores+=1
            datos.append((index,b,j,row[2]))

        cursor.executemany("INSERT INTO permanente VALUES(?,?,?,?)", datos)
        connection.commit()
        connection.close()
    crear_tabla_permanente(df5)
    crear_tabla_temporal(df)
    crear_tabla_bodega(df2)
    crear_tabla_maquinas(df3)
    crear_tabla_gastos(df4)

##rellena datos faltantes
def actualizar_tabla_temporal(df):
    connection = sqlite3.connect('temporal.db')
    cursor = connection.cursor()
    cursor.execute("select count(*) from {}".format(nombre))
    values = cursor.fetchone()

    if len(df.loc[values[0]:])!=0:
        datos = []
        for index, row in df.loc[values[0]:, :].iterrows():
            a = datetime.datetime.strptime(row[0], '%d-%m-%y')
            b = a.strftime('%y/%m/%d')
            j = str(row[5]).replace(".","").strip(" ")
            if j[0]=="-" or j[0]== "$":
                if len(j[1:])==0:
                    j = 0
                else:
                    j = int(j[1:])*-1
            elif "," in j:
                j = int(j[:j.find(",")])
            else:
                try:
                    j = int(j)
                except ValueError:
                    j = 0

            datos.append((index,b,j,row[7].strip(),row[8],row[11],row[12],row[13].strip(" ")))
        cursor.executemany("INSERT INTO temporal VALUES(?,?,?,?,?,?,?,?)", datos)

    connection.commit()
    connection.close()
    return None

def get_info(Empresa,Cultivo,Fecha,Lugar):
    connection = sqlite3.connect("temporal.db")
    cursor = connection.cursor()
    cursor.execute("""SELECT fecha, valor, Clasificacion_labor
    FROM temporal T
    WHERE (T.fecha >= ? AND T.empresa = ? AND T.lugar = ? AND T.clasif_cultivo = ?) AND T.Clasificacion_labor IN ('RIEGO','CONTROL MALEZAS','PLANTACIN/SIEMBRA','TRASLADOS/FLETES','COSECHA','ABONADO','ADMINISTRACIN','CULTIVAR','MANTENCIN/REPARACIN','MAQUINARIA','APLICACIN QUMICA','LABORES CULTURALES','POST COSECHA','SOMBREADERO','ANTICIPO AGRICULTOR','LIQUIDACIN AGRICULTOR','VARIOS')
    """,(Fecha,Empresa,Lugar,Cultivo))
    return cursor.fetchall()
def get_info2(Empresa,Cultivo,Fecha,Lugar):
    connection = sqlite3.connect("bodega.db")
    cursor = connection.cursor()
    cursor.execute("""SELECT fecha, valor, Clasificacion
    FROM bodega B
    WHERE (B.fecha >= ? AND B.empresa = ? AND B.lugar = ? AND B.cultivo = ?)  AND B.Clasificacion IN ('FUNGICIDAS','ACARICIDAS','HERBICIDAS','INSECTICIDAS','ESTIMULANTES','ADHERENTE, COAY. Y HUMEC.','ENRAIZANTE','FERTILIZANTE FOLIAR','REPELENTE','ABONOS FOLIARES','FERTILIZANTES','POLIETILENOS','MATERIALES DE RIEGO') """,(Fecha,Empresa,Lugar,Cultivo))
    return cursor.fetchall()
def get_info3(Empresa,Cultivo,Fecha,Lugar):
    connection = sqlite3.connect("maquinas.db")
    cursor = connection.cursor()
    cursor.execute("""SELECT fecha, valor, Clasificacion
    FROM maquinas M
    WHERE (M.fecha >= ? AND M.empresa = ? AND M.lugar = ? AND M.cultivo = ?)  AND M.Clasificacion IN ('RASTRAJE','APLIC. PESTICIDAS','APLIC. HERBICIDA','SIEMBRA/PLANTACION','CULTIVAR','MELGADO','ACEQUIAR','OTROS','APLIC. GUANO','ARADURA','ROTOVATOR SIN MELGA','MELGADO CON ABONO','ENCAMADO',"ABONADO",'ROTOVATOR CON MELGA','ARRIENDO MAQUINARIAS','ROTOVATOR CON MEZCLA','OTROS FLETES','APLICACIONES')
     """ ,(Fecha,Empresa,Lugar,Cultivo))
    return cursor.fetchall()
def get_info4(Empresa,Cultivo,Fecha,Lugar):
    connection = sqlite3.connect("gastos.db")
    cursor = connection.cursor()
    cursor.execute("""SELECT fecha, valor, Clasificacion
    FROM gastos G
    WHERE ((G.fecha >= ? AND G.empresa = ? AND G.lugar = ? AND G.cultivo = ?) ) AND G.Clasificacion IN ("COMBUSTIBLE Y LUBRICANTES",'COMERCIALIZADORA',"ENVASES","HERRAMIENTAS","MALLAS/LONAS","MATERIALES","MATERIALES DE RIEGO","MULCH","OTROS","VARIOS","MATERIALES DE OFICINA","REPUESTOS") """,(Fecha,Empresa,Lugar,Cultivo))
    return cursor.fetchall()
def get_info5(Empresa,Fecha):
    connection = sqlite3.connect("permanente.db")
    cursor = connection.cursor()
    cursor.execute("""SELECT fecha, valor,  "SUELDO PERMANENTE"
    FROM permanente P
    WHERE P.fecha>=? AND P.empresa = ?""",(Fecha,Empresa))
    return cursor.fetchall()
def get_info6(Empresa,Fecha):
    connection = sqlite3.connect("gastos.db")
    cursor = connection.cursor()
    cursor.execute("""SELECT fecha, valor,Clasificacion
    FROM gastos G
    WHERE (G.fecha>=? AND G.empresa = ?) AND G.Clasificacion IN ('TELEFONA/INTERNET','ELECTRICIDAD','AGUA','ARTCULOS DE OFICINA','GASTOS LEGALES','GASTOS DE REPRESENTACIN','CORREO Y ENCOMIENDA','PUBLICIDAD','VARIOS','DESPENSA','ASOCIACIN DE AGRICULTORES','GAS','CAPACITACIONES','BA_O QUMICO','SERVICIO DE ALARMAS','PATENTES VEHCULOS','REPUESTOS','ARRIENDO GENERADOR','OTROS','FINIQUITOS','DEV. M.O.PROPORCIONAL','DEPRECIACIN','PAGO INTERESES','MANTENCIN/REPARACIN'
    )""",(Fecha,Empresa))
    return cursor.fetchall()

def aaa():
        productos = ['Apio 16', 'Coliflor 16', 'Maz 17', 'Coliflor 17', 'Brcoli 17', 'Flores 16', ' Aj 17 ', 'Tomate 17', 'Kale 16', 'Kale 17', 'Apio 17', ' Flores 17 ', 'Administracin', 'Casa','General', 'Tomate 16/17', 'Maquinaria', 'Maravilla 17','Zapallo', 'Soya 17', 'Jardn', 'Colchina 17', 'Briceo', 'Brcoli 18', 'Acelga 17', 'EN 2 SEM', 'Flores 17', 'Bodega', 'Kale 18', 'Petitvert', 'Inversin', 'Otro', 'Flores 18', 'Tomate 18', 'Orillas', 'Cebolla 18', 'Energa Elctrica', 'Soya 18', ' Aj 18 ', 'Apio 18', 'Maz 18', 'Albahaca 18', 'Colchina 18', 'Betarraga 17','Tierra', 'Maravilla 18', ' Flores 18 ']
        ventana= tk.Tk()
        ventana.title("ELECCION DE PRODUCTO")
        ventana.geometry("380x300+600+50")
        ventana.config(background = "black")
        var = tk.StringVar(ventana)
        var.set("Apio 16")
        opcion= tk.OptionMenu(ventana,var,*productos)
        opcion.config(width=20)
        opcion.pack(side= "left",padx=30,pady=30)
        el = tk.Label(ventana,text="Para seleccionar cierre la ventana",bg="red",fg="white")
        el.pack(padx=5,pady=5,ipadx=5,ipady=5,fill=tk.X)
        color = tk.Label(ventana,bg="plum",textvariable=var,padx=5,pady=5,width=50)
        color.pack
        ventana.mainloop()
        return var.get()
def aaa2():
        lugares = ['San Jorge', 'Castao 1', ' Castao 2 ', 'Nogales', 'Allende','Pocochay', 'Catemu', ' La Fortuna ','Llay llay', 'La Ligua', 'General', 'San Pedro','Collao', 'Casa', 'DESCUENTO', 'Olivo', 'bucalemu', 'general', 'Caja Chica', 'casa', 'Consiglieri', 'Administracin', 'Bucalemu', 'Sta Olga', 'Hijuelas', 'Romeral', 'Poncho Celeste', 'Los Caballos', 'La Pea', 'Olmu', 'Briceo', 'Administracion', 'San Felipe', 'Adm', 'adm', 'Catemu 1','Catemu 3', 'Catemu 5','Colina', 'Parrones','La Calera','casa edison','Chagre','El Roble']
        ventana= tk.Tk()
        ventana.title("ELECCION DE LUGAR")
        ventana.geometry("380x300+600+50")
        ventana.config(background = "black")
        var2 = tk.StringVar(ventana)
        var2.set("San Jorge")
        opcion= tk.OptionMenu(ventana,var2,*lugares)
        opcion.config(width=20)
        opcion.pack(side= "left",padx=30,pady=30)
        el = tk.Label(ventana,text="Para seleccionar cierre la ventana",bg="red",fg="white",width=50)
        el.pack(padx=5,pady=5,ipadx=5,ipady=5,fill=tk.X)
        color = tk.Label(ventana,bg="plum",textvariable=var2,padx=5,pady=5,width=50)
        color.pack
        ventana.mainloop()
        return var2.get()
def aaa3():
        empresas = ['MASSAI','VALCAM','SANDRO','BRICE_O','P. HENRIQUEZ','COLLAO','SEGUNDO CALIZARIO','JUAN GONZALEZ','ARANCIBIA','ROSARIO FERNANDEZ','VASQUEZ','MARIA CORTEZ','DAVID VILLARROEL','VIZA','ALEJANDRO CONTRERAS','CSILVA','NELSON ZAMORA','CARLOS NAVARRO','NAHUEN','GONZALEZ','RICARDO VASQUEZ','LUIS RODRIGUEZ','VENTURA SEPULVEDA','RODRIGO MARTINEZ','ARANGUE','ORLANDO ARAYA','FELIPE BUSTAMANTE','AGRO LIDER','VENTURA','ALMONTE','JOSE HURTADO','HURTADO','ELIAS JAMET','JAMETT','MARCO ARANGUE','CALISARIO','VIAL','CALISARIO 400 VISL 341','MARTINEZ','MARZAN' ]
        ventana= tk.Tk()
        ventana.title("ELECCION DE EMPRESA")
        ventana.geometry("380x300+600+50")
        ventana.config(background = "black")
        var2 = tk.StringVar(ventana)
        var2.set("VALCAM")
        opcion= tk.OptionMenu(ventana,var2,*empresas)
        opcion.config(width=20)
        opcion.pack(side= "left",padx=30,pady=30)
        el = tk.Label(ventana,text="Para seleccionar cierre la ventana",bg="red",fg="white",width=50)
        el.pack(padx=5,pady=5,ipadx=5,ipady=5,fill=tk.X)
        color = tk.Label(ventana,bg="plum",textvariable=var2,padx=5,pady=5,width=50)
        color.pack
        ventana.mainloop()
        return var2.get()

def GV():

    date = input(str("ELIGE UNA FECHA formato mes-año XX-XX "))
    producto = aaa()
    empresa = aaa3()
    lugar = aaa2()
    return [date,producto.upper(),lugar.upper(),empresa.upper()]
eleccion = GV()

a = datetime.datetime.strptime(eleccion[0], '%m-%y')
b = a.strftime('%y-%m')
sales = get_info(eleccion[3],eleccion[1],b,eleccion[2])
sales2= get_info2(eleccion[3],eleccion[1],b,eleccion[2])
sales3= get_info3(eleccion[3],eleccion[1],b,eleccion[2])
sales4= get_info4(eleccion[3],eleccion[1],b,eleccion[2])
sales5 = get_info5(eleccion[3],b)
sales6 = get_info6(eleccion[3],b)

models = ['RIEGO','CONTROL MALEZAS','PLANTACIN/SIEMBRA','TRASLADOS/FLETES','COSECHA','ABONADO','ADMINISTRACIN','CULTIVAR','MANTENCIN/REPARACIN','MAQUINARIA','APLICACIN QUMICA','LABORES CULTURALES','POST COSECHA','SOMBREADERO','ANTICIPO AGRICULTOR','LIQUIDACIN AGRICULTOR','VARIOS']
models2 = ['FUNGICIDAS','ACARICIDAS','HERBICIDAS','INSECTICIDAS','ESTIMULANTES','ADHERENTE, COAY. Y HUMEC.','ENRAIZANTE','FERTILIZANTE FOLIAR','REPELENTE','ABONOS FOLIARES','FERTILIZANTES','POLIETILENOS','MATERIALES DE RIEGO']
models3 = ['RASTRAJE','APLIC. PESTICIDAS','APLIC. HERBICIDA','SIEMBRA/PLANTACION','CULTIVAR','MELGADO','ACEQUIAR','OTROS','APLIC. GUANO','ARADURA','ROTOVATOR SIN MELGA','MELGADO CON ABONO','ENCAMADO',"ABONADO",'ROTOVATOR CON MELGA','ARRIENDO MAQUINARIAS','ROTOVATOR CON MEZCLA','OTROS FLETES','APLICACIONES']
models4 = ["COMBUSTIBLE Y LUBRICANTES",'COMERCIALIZADORA',"ENVASES","HERRAMIENTAS","MALLAS/LONAS","MATERIALES","MATERIALES DE RIEGO","MULCH","OTROS","VARIOS","MATERIALES DE OFICINA","REPUESTOS"]
models5=["SUELDO PERMANENTE"]
models6 = ['TELEFONA/INTERNET','ELECTRICIDAD','AGUA','ARTCULOS DE OFICINA','GASTOS LEGALES','GASTOS DE REPRESENTACIN','CORREO Y ENCOMIENDA','PUBLICIDAD','VARIOS','DESPENSA','ASOCIACIN DE AGRICULTORES','GAS','CAPACITACIONES','BA_O QUMICO','SERVICIO DE ALARMAS','PATENTES VEHCULOS','REPUESTOS','ARRIENDO GENERADOR','OTROS','FINIQUITOS','DEV. M.O.PROPORCIONAL','DEPRECIACIN','PAGO INTERESES','MANTENCIN/REPARACIN']
for c in models:
    actual = a
    while actual < datetime.datetime.now():
        sales.append((actual.strftime('%y-%m'),0,c))
        actual+=datetime.timedelta(hours=720)
for m in models2:
    actual = a
    while actual < datetime.datetime.now():
        sales2.append((actual.strftime('%y-%m'),0,m))
        actual+=datetime.timedelta(hours=720)
for l in models3:
    actual=a
    while actual < datetime.datetime.now():
        sales3.append((actual.strftime('%y-%m'),0,l))
        actual+=datetime.timedelta(hours=720)
for w in models4:
    actual = a
    while actual < datetime.datetime.now():
        sales4.append((actual.strftime('%y-%m'),0,w))
        actual+=datetime.timedelta(hours=720)
for x in models5:
    actual = a
    while actual < datetime.datetime.now():
        sales5.append((actual.strftime('%y-%m'),0,x))
        actual+=datetime.timedelta(hours=720)
for t in models6:
    actual = a
    while actual < datetime.datetime.now():
        sales6.append((actual.strftime('%y-%m'),0,t))
        actual+=datetime.timedelta(hours=720)

labels = ['fecha', 'valor', 'actividad']
resumen = pd.DataFrame.from_records(sales, columns=labels)

resumen2 = pd.DataFrame.from_records(sales2, columns=labels)

resumen3 = pd.DataFrame.from_records(sales3, columns=labels)

resumen4 = pd.DataFrame.from_records(sales4, columns=labels)

resumen5 = pd.DataFrame.from_records(sales5, columns=labels)

resumen6 = pd.DataFrame.from_records(sales6, columns=labels)

tabla = pd.crosstab([resumen.actividad],[resumen.fecha],dropna=True,values = resumen.valor,aggfunc = np.sum, margins = False)
tabla2 = pd.crosstab([resumen2.actividad],[resumen2.fecha],dropna=True,values = resumen2.valor,aggfunc = np.sum, margins = False)
tabla3 = pd.crosstab([resumen3.actividad],[resumen3.fecha],dropna=True,values = resumen3.valor,aggfunc = np.sum, margins = False)
tabla4 = pd.crosstab([resumen4.actividad],[resumen4.fecha],dropna=True,values = resumen4.valor,aggfunc = np.sum, margins = False)
tabla5 =  pd.crosstab([resumen5.actividad],[resumen5.fecha],dropna=True,values = resumen5.valor,aggfunc = np.sum, margins = False)
tabla6 =  pd.crosstab([resumen6.actividad],[resumen6.fecha],dropna=True,values = resumen6.valor,aggfunc = np.sum, margins = False)
final = pd.concat([tabla2,tabla4,tabla3,tabla])
final2 = pd.concat([tabla5,tabla6],sort = True)
diccionario = {"Bet.":final,"G.Adm":final2}
writer = pd.ExcelWriter("Resumen.xlsx",engine='xlsxwriter')
for sheet_name in diccionario.keys():
    diccionario[sheet_name].to_excel(writer,sheet_name=sheet_name,index = True,header = True,startrow= 1)
writer.save()

#final2.to_csv("resumen2.csv",sep=";")
#final.to_csv("resumen.csv",sep=";",)
