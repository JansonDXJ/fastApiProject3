import os
import pyodbc
from PIL import Image
from datetime import datetime

from starlette.responses import JSONResponse

from main import app

"""global constant"""
server = 'xiaojie.database.windows.net'
database = 'ml'
username = 'xiaojieding'
password = '*****'
driver = 'SQL Server Native Client 11.0'

"""
Store all Training Images and labels into DB:PhotosDB
"""

class StoredToDB:
    def storePhotoDB(self, conn=None):
        pathNG = 'D:/images/NG/'
        pathOK = 'D:/images/OK/'
        res = []
        timestamp = datetime.now()
        for i in os.listdir(pathNG):
            image = Image.open(pathNG + i)
            image_name = i
            f = open(pathNG + i, 'rb')
            img_bin = f.read()
            res.append([image_name, img_bin, 'NG', image.size, timestamp])
        for i in os.listdir(pathOK):
            image = Image.open(pathOK + i)
            image_name = i
            f = open(pathOK + i, 'rb')
            img_bin = f.read()
            res.append([image_name, img_bin, 'OK', image.size, timestamp])
        sql = "INSERT INTO dbo.PhotosDB ([PhotoName], [PhotoBin], [PhotoType], [PhotoSize], [UploadTime]) VALUES (?, ?, ?, ?, ?)"
        with conn.cursor() as cursor:
            try:
                for r in res:
                    cursor.execute(sql, r[0], r[1], r[2], r[3], r[4], r[5])
                return JSONResponse(content=None, status_code=200, headers=None)
            except pyodbc.OperationalError as e:
                app.logger.err(f"{e.args[1]}")
                raise
            finally:
                cursor.close()


