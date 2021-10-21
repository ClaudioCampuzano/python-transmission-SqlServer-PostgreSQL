import pyodbc
import json
import psycopg2
from psycopg2 import extras
from psycopg2 import sql

def connectPostgres(params):
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(end=" ")
    #print("Connection successful PostgreSQL")
    return conn


def connectSqlServer(params):
    conn = None
    try:
        conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s'
                              % (params['driver'], params['server'], params['database'], params['user'], params['password']))
    except pyodbc.Error as e:
        print(e, "error")
    return conn


def main(jsonFile):
    recordsToPostgreSQL = []
    camerasId = jsonFile["cameraId"].split(',')

    try:
        connPostgres, connSqlServer = connectPostgres(
            jsonFile["postgres"]), connectSqlServer(jsonFile["sqlserver"])
        if connPostgres is None or connSqlServer is None:
            raise ValueError('Error when trying to connect to the DB ...')
        else:
            print("Connected to both DB")
        curPostgres, curSqlServer = connPostgres.cursor(), connSqlServer.cursor()

        sqlServer_Query = f"SELECT * FROM {jsonFile['table']}"
        curSqlServer.execute(sqlServer_Query)
        for record in curSqlServer.fetchall():
            if record[7].split(',')[0] in camerasId and not record[-1]:
                listAux = list(record)
                listAux.pop(23)
                listAux.pop(30)
                recordsToPostgreSQL.append(tuple(listAux))

        postgreSQL_Query_delete = f"TRUNCATE {jsonFile['table']}"
        curPostgres.execute(postgreSQL_Query_delete)

        postgreSQL_Query_insert = "INSERT INTO {table} (id_conf_alerta,nombre_conf_alerta,descripcion_conf_ale,flag_todo,country_id,region_id,location_id,camaras_id,camaras_nombres,evento_id_conf_analitica,evento_id_operador_logico,evento_umbral,alarma_porcent_valor,alarma_id_ventana_tiempo,alarma_valor_ventana_tiempo,notificacion_habilitada,notificacion_fonos,notificacion_correos,notificacion_id_conf_canal_fono,notificacion_id_conf_canal_correo,notificacion_id_template_fono,notificacion_templat,notificacion_id_template_correo,notificacions_alarmas_max,notificacions_alarma_off,estado,fecha_creacion,rol,last_id,last_id_alarma) VALUES %s;"
        sqlQuery = sql.SQL(postgreSQL_Query_insert).format(table=sql.Identifier(jsonFile['table']))
        extras.execute_values(curPostgres, sqlQuery.as_string(curPostgres), recordsToPostgreSQL)
        print(str(curPostgres.rowcount)+' insert in postgreSQL')
        connPostgres.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connPostgres is not None:
            connPostgres.close()
        if connSqlServer is not None:
            connSqlServer.close()


if __name__ == '__main__':
    with open("DB_credentials.json") as jsonFile:
        jsonCamInfo = json.load(jsonFile)
        jsonFile.close()

    main(jsonCamInfo)
