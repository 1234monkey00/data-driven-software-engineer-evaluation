import PyPDF2
import os
import sqlite3

from PyPDF2 import PdfReader

#IL PROGRAMMA PRENDE TUTTI I FILE PDF DALLA CARTELLA INDICATA E LI INSERISCE CON UN UNICA COMMIT NEL DATABASE SQLITE PDFdb.db CREATO ALL'AVVIO

#funzione per mostrare il contenuto dell'unica tabella del database e controllare d'aver inserito correttamente i campi
def ReadDB(DBconnection):


        command = DBconnection.cursor()
        command.execute("Select * from pdfsTable;")

        for row in command.fetchall():
            print(row)


#funzione per inserire pdf nel database, ha come parametri la connessione al DB ed il nome del file pdf
def InsertPDFinDB(DBconnection, PDFname):

    try:
        path = os.path.abspath(os.getcwd() + os.sep + "data-processing" + os.sep + "pdfs" + os.sep + PDFname)

        readPDF = PdfReader(path)

        pagenumber = len(readPDF.pages)

        meta = readPDF.metadata

        author = meta.author
        date = meta.creation_date

        producer = meta.producer





        pages = readPDF.pages
        pagetxt = ""

        for page in pages:
            pagetxt += page.extract_text()





        command = DBconnection.cursor()

        command.execute("INSERT INTO pdfsTable (PDFname,PDFtext,PDFauthor,PDFcreationDate,PDFpagesNumber,PDFeditorUsed) VALUES ('" + str(PDFname) + "','" + str(pagetxt) + "','" + str(author) + "','" + str(date) + "','" + str(pagenumber) + "','" + str(producer) + "');")
        command.fetchall()

    except : #try catch per la gestione di file corrotti o errati
        print("can't put the pdf "+ str(PDFname) + " in the DB, it could be that the pdf is corrupted check it out and try again...")



if os.path.isfile("PDFdb.db"):
    print("the DB already exist, i will now start working")
else:
    print("the DB doesnt exist, i'll make it now")
    try:
        with sqlite3.connect("PDFdb.db") as DBconnection:
            command = DBconnection.cursor()
            print("creating table")
            command.execute("CREATE TABLE pdfsTable (ID integer primary key, PDFname varchar(20), PDFtext text, PDFauthor varchar(20), PDFcreationDate text, PDFpagesNumber int, PDFeditorUsed varchar(20));")

    except sqlite3.OperationalError as err:
        print("Failed to open the database:", err)



#x = input("pdf name:")

try:
    with sqlite3.connect("PDFdb.db") as DBconnection:
        #DBconnection.isolation_level = None #usato per test sulla velocità con e senza transaction implicite

        PDFdir = os.path.abspath(os.getcwd() + os.sep + "data-processing" + os.sep + "pdfs" + os.sep)
        for pdfFile in os.listdir(PDFdir):
            if pdfFile.endswith(".pdf"):
                InsertPDFinDB(DBconnection, pdfFile)


        #USATO PER TESTS SULLA VELOCITA'
        #for n in range(101):
        #    InsertPDFinDB(DBconnection, x)

        DBconnection.commit()

        #funzione che può essere usata alla fine semplicemente per controllare se il programma ha eseguito correttamente, per ora è lasciata commentata per non rallentare il programma con tante print inutili
        #ReadDB(DBconnection)

except FileNotFoundError:
  print("An exception occurred during file reading")
except sqlite3.OperationalError as err:
    print("Failed to reopen database:", err)

print("CLOSING...")
DBconnection.close()
