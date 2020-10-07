#!/usr/bin/python3
from __future__ import with_statement
from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
from ftplib import FTP
import shutil,time,os,pwd
import sys
import argparse
from datetime import datetime, date, time, timedelta

#python3 check_respaldo_ftp.py -s 192.168.0.7 -u cristian -pass cristian -r /etc/apache2/ -t /home/cristian/backup/temporal -c /home/cristian/backup/

"""
Hola a Todos,
Por favor si me pueden orientar como hacer lo siguiente.
Resulta que necesito que se ejecute un script cada cierto tiempo que lo defina el usuario, sea cada 7 dias, 1 mes, etc. Algunos me diran que use un cronjob, pero no me sirve en este caso. Necesito que el usuario lo ingrese por ejemplo por argparse, el ciclo de los dias que se ejecutara el script
El punto que si elije 7 dias, puedo sumar a la variable por ejemplo dia_ciclo = fecha+timedelta(days=7) o semana_siguiente = fecha_inicio+timedelta(weeks=1), pero como lo hago para la semana siguiente, ya que el script sera llamado de forma constante (cada 30min por ejemplo).
Pense algo asi, pero solo me servira para un ciclo. Nose como hacerlo para que sea constante.
fecha_inicio = date(2020, 10, 2 )
semana_siguiente = fecha_inicio+timedelta(weeks=1)
while fecha_inicio < semana_siguiente:
    fecha_inicio = fecha_inicio+timedelta(days=1)

"""


parser = argparse.ArgumentParser(description="Check Respaldo FTP Nagios")

parser.add_argument(
    "-s",
    "--server",
    required=True,
    help="ip del servidor FTP a conectar"

)

parser.add_argument(
    "-u",
    "--usuario",
    required=True,
    help="nombre de usuario"

)

parser.add_argument(
    "-pass",
    "--password",
    required=True,
    help="password de usuario"

)

parser.add_argument(
    "-r",
    "--ruta",
    required=True,
    help="ruta de descarga del FTP, debe terminar con /"

)

parser.add_argument(
    "-t",
    "--temporal",
    required=True,
    help="ruta temporal de descarga"

)

parser.add_argument(
    "-c",
    "--copia",
    required=True,
    help="ruta copia de descarga en archivo zip"

)

parser.add_argument(
    "-d",
    "--dia",
    type=int,
    help="ciclo de dias que se realizara el respaldo"

)

parser.add_argument(
    "-w",
    "--semana",
    type=int,
    help="ciclos por semana que se realizara el respaldo"

)

parser.add_argument(
    "-port",
    "--puerto",
    nargs="?",
    default=21,
    type=int,
    help="ruta copia de descarga en archivo zip"

)



args = parser.parse_args()

""" print(f"la ip del server es: {args.server}")
print(f"El nombre de usuario es: {args.usuario}")
print(f"la password es: {args.password}")
print(f"la ruta de descarga del FTP es: {args.ruta}")
print(f"la ruta temporal de recibo es: {args.temporal}")
print(f"el puerto de conexion  es: {args.puerto}")
print(f"la cantidad de dia es: {args.dia}")
print(f"la cantidad de semana es: {args.semana}")
exit() """



#variables Nagios
UNKNOWN = -1
OK = 0
WARNING = 1
CRITICAL = 2

#Variables Globales de configuracion
HOST = f'{args.server}'
PUERTO = args.puerto
USUARIO = f'{args.usuario}'
CLAVE = f'{args.password}'
#ruta = "/"
ruta = f"{args.ruta}"
destino = f"{args.temporal}" 
ftp = FTP()

""" print(f"la ip del server es: {HOST}")
print(f"El nombre de usuario es: {USUARIO}")
print(f"la password es: {CLAVE}")
print(f"la ruta de descarga del FTP es: {type(ruta)}")
print(f"la ruta temporal de recibo es: {destino}")
print(f"el puerto de conexion  es: {PUERTO}")
exit() """

#Variables globales para logs
listaErrores = []
numErrores = 0

#Ruta donde se guardará un .zip de todo lo descargado
COPIAZIP='/home/cristian/backup/'

if destino == COPIAZIP:
    print("LA ruta de Destino:"+destino+" No puede ser igual a: "+COPIAZIP)
    sys.exit(CRITICAL)

#Metodos
##################################################################
#Metodo de conexion al servidor FTP
def conectar():
    
    try:
        if ftp.connect(HOST, PUERTO) and ftp.login(USUARIO, CLAVE):
           # print('Conectado al servidor')
           # print(str(ftp.welcome))
            return
    except:
        print("error en datos Host, Puerto o Usuario y Contraseña")
        sys.exit(CRITICAL)
    
#Método recursivo de descarga
def descargaRecursiva(ruta):
    global numErrores
    #Nos movemos a la ruta en el servidor
    #ftp.cwd(ruta)
    try:
        ftp.cwd(ruta)
    except:
        print("Ruta Ingresada para el respaldo no es valida, recuerde terminar con '/'")
        sys.exit(CRITICAL)
    #print(ftp.cwd(ruta))
    #exit()
    #print('En ruta: '+ruta)
    #Obtenemos una lista de string, cada string define un archivo
    listaInicial =[]
    ftp.dir(listaInicial.append)
    #print(ftp.dir(listaInicial.append))
    #exit()
    '''
    con dir() obtenemos un string que lista todos los elementos contenido en la ruta
    la estructura obtenida en mi caso fue
    
        drwxr-xr-x    2 362309906  usuario1       4096 Nov  2 15:09 .
        drwxr-x--x    4 362309906  usuario1        4096 Jun  9 08:26 ..
        -rw-r--r--    1 362309906  usuario1         563 Nov  1 17:10 index.html
        
    donde las carpetas comienzan con d y el nombre del archivo/carpeta se ubica al fin
    el string anterior se trata de la siguiente manera:
        -se divide el string en una lista de strings donde cada posición representa una linea
        -cada posición de la lista se subdivide tomando como referencia los espacios formando un 
        array bidimensional donde tenemos la información separada y podemos consultarla con más facilidad
    En este caso el array bidimensional tiene en las posiciones:
        -nx0: la información relativa a si es un directorio (d) o un archivo (-) y sus pemisos
        -nx8: el nombre de fichero
    '''
    listaIntermedia = []
    for elemento in listaInicial:
        listaIntermedia.append(str(elemento).split())
        #print(listaIntermedia.append(str(elemento).split()))
    #exit()
    '''
    Tras obtener en listaIntermedia el array bidimensional, generamos dos listas:
        -Una lista de pos[8] (nombres de ficheros) que cumplen que pos[0] no comienza con d
        -Una lista de pos[8] (carpetas) que cumplen que pos[0] comienza con d
    '''
    listaArchivos = []
    listaCarpetas=[]
    for elemento in listaIntermedia:
        if elemento[0].startswith('d'):
            listaCarpetas.append(str(elemento[8]))
            #print (elemento)
            #print(listaCarpetas.append(str(elemento[8])))
        else:
            listaArchivos.append(str(elemento[8]))
            #lisArchivos = listaArchivos.append(str(elemento[8]))
            #print(listaArchivos.append(str(elemento[8])))
            #print (elemento)
            #print (lisArchivos)
    #exit()
    '''
    Eliminamos de la lista de carpetas . y .. para evitar bucles por el servidor
    '''
    try:
        listaCarpetas.remove('.')
        listaCarpetas.remove('..')
        #exit()
    except:
        pass
    
    '''
    Listamos los elementos a trabajar de la ruta actual
    '''
    #print('\tLista de Archivos: '+str(listaArchivos))
    #exit()
    #print('\tLista de Carpetas: '+str(listaCarpetas))
    #exit()
    '''
    Si la ruta actual no tiene su equivalente local, creamos la carpeta a nivel local
    '''
    
    try:
        if not os.path.exists(destino+ruta):
            os.makedirs(destino+ruta)
    except PermissionError:
        usuario = pwd.getpwuid(os.getuid()).pw_dir
        print("El usuario "+usuario+", no tiene permiso para escribir o crear carpetas en la ruta destino: "+ destino)
        sys.exit(CRITICAL)
        
    '''
    Los elementos de la lista de archivo se proceden a descargar de forma secuencial en la ruta
    '''
    for elemento in listaArchivos:
       # print('\t\tDescargando '+elemento+' en '+destino+ruta)
        try:
            ftp.retrbinary("RETR "+elemento, open(os.path.join(destino+ruta,elemento),"wb").write)
        except:
            print('Error al descargar '+elemento+' ubicado en '+destino+ruta)
            listaErrores.append('Archivo '+elemento+' ubicado en '+destino+ruta)
            numErrores = numErrores+1
            sys.exit(CRITICAL)
    '''
    Una vez se termina de descargar los archivos invocamos el método actual provocando una solución
    recursiva, para ello concatenamos la ruta actual con el nombre de la carpeta, realizando tantas
    llamadas al método actual como elementos tengamos listados en listaCarpetas
    '''
    #exit()
    for elemento in listaCarpetas:
        descargaRecursiva(ruta+elemento+'/')
        #print(descargaRecursiva(ruta+elemento+'/'))
        #exit()
    return
    #exit()

#Método para imprimir resultado de errores detectados y crear un log con los ficheros que dieron fallo
def mostrarLog():
    global numErrores
    #print('##################################################################')
    log = open(destino+"/logFTP.txt",'w')
    #print('Errores detectados = '+str(numErrores))
    log.write('Errores detectados = '+str(numErrores))
    for elemento in listaErrores:
        log.write('\t'+str(elemento))
        print('\t'+str(elemento))
    #print('##################################################################')
    if numErrores != 0:
        print("Se ecnontraron errores de algunos archivos de descarga, verificar el log ubicado"+destino+"/logFTP.txt")
        sys.exit(CRITICAL)
#Método de copia
def comprimirYBorrar(ruta):
    #print('Comprimiendo '+ruta)
    assert os.path.isdir(ruta)
    with closing(ZipFile(COPIAZIP+'Copia'+str(time.strftime("%Y%m%d-%H%M%S"))+'.zip', "w", ZIP_DEFLATED)) as z:
        for root, dirs, files in os.walk(ruta):
            for fn in files:
                absfn = os.path.join(root, fn)
                zfn = absfn[len(ruta)+len(os.sep):]
                z.write(absfn, zfn)
    #print("Ruta comprimida en "+COPIAZIP)
    shutil.rmtree(ruta, ignore_errors=True)
    #print("Eliminados los archivos descargados que no estaban comprimidos de la ruta:\n\t"+ruta)

def init(path_file, fecha, formato_fecha):
    """ Guarda la fecha determinada por el usuario """
    with open(path_file, 'w') as ruta_fecha:
        ruta_fecha.write(fecha.strftime(formato_fecha))

def check_if_datefile_exists(path_file):
    return os.path.exists(path_file)

def get_content_date_from_file(path_file):
    with open(path_file, "r") as ruta_fecha:
        return ruta_fecha.read()


       
#Main

# Ruta del archivo
tmp_path = os.getcwd()+"/tmp" # Preferir full_path para evitar problemas de referencia de directorio
path_file = f"{tmp_path}/fecha.txt" # nombre del archivo con su direcotrio base

# Fecha actual
today = date.today()
formato_fecha = "%d/%m/%Y %H:%M:%S"

try:
    if not check_if_datefile_exists(path_file):
        # Se inicia el archivo con la fecha
        os.makedirs(tmp_path) #crea la carpeta
        init(path_file, today, formato_fecha)
except PermissionError:
    usuario = pwd.getpwuid(os.getuid()).pw_dir
    print("El usuario "+usuario+", no tiene permiso para escribir o crear carpetas en la ruta destino: "+ tmp_path)
    sys.exit(CRITICAL)


date_string = get_content_date_from_file(path_file)
fecha_almacenada = datetime.strptime(date_string, formato_fecha).date()

week = args.semana
# sumaFechaWeek = fecha_almacenada+timedelta(weeks=week)
# totalWeek = today-sumaFechaWeek
# print(totalWeek.days)
# print((today - (fecha_almacenada+timedelta(weeks=week))).days)
# exit()

day = args.dia

# sumaFechaDay = fecha_almacenada+timedelta(days=day)
# totalDay = today-sumaFechaDay



#if week and (str(today - fecha_almacenada+timedelta(weeks=week))) >= "1 days, 0:00:00":
if week and ((today - (fecha_almacenada+timedelta(weeks=week))).days) > 0:
    # Ejecutar respaldo
    # print("estoy en week")
    # exit()
    conectar()
    descargaRecursiva(ruta)
    mostrarLog()
    comprimirYBorrar(destino)
    #Desconectamos con el servidor de forma protocolaria
    ftp.quit()
    init(path_file, today, formato_fecha)
    print('OK - Conexion cerrada y archivos respaldados correctamente, ruta: '+ COPIAZIP+'con fecha: '+str(time.strftime("%Y%m%d-%H%M%S")))
    # Actualizar fecha en el archivo con la fecha actual


if day and ((today - (fecha_almacenada+timedelta(days=day))).days) > 0:
    # Ejecutar respaldo
    # print("estoy en day")
    # exit()
    conectar()
    descargaRecursiva(ruta)
    mostrarLog()
    comprimirYBorrar(destino)
    #Desconectamos con el servidor de forma protocolaria
    ftp.quit()
    init(path_file, today, formato_fecha)
    print('OK - Conexion cerrada y archivos respaldados correctamente, ruta: '+ COPIAZIP+'con fecha: '+str(time.strftime("%Y%m%d-%H%M%S")))
    # Actualizar fecha en el archivo con la fecha actual

print(f"OK - Aun no corresponde realizar respaldo, el ultimo respaldo fue realizado con fecha: {fecha_almacenada}")





