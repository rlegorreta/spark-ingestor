#!/usr/bin/python
# Copyright (c) 2022, LMASS Desarrolladores, S.C.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are not permitted.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
#  configuration.py
#
#  Developed 2022 by LMASS Desarrolladores, S.C. www.legosoft.com.mx
#
# Class thta ask for a configuration and return its values stored in a
# json.
#
# project: ingestor
# author: rlh
# date: December 2022
#
import os
import logging
from python_graphql_client import GraphqlClient
from enum import Enum
import json


# This class gets the configuration from the paramDB datasource name
#
# Response ex = {'data':
#                  {'datasources': [
#                    {'nombre': 'ordenes',
#                     'config': '{
#                       "delimitadores": {
#                                         "entrada": ",",
#                                         "salida": ",",
#                                         "encabezado": false
#                                         },
#                       "formato_entrada": "csv",
#                       "tipo_salida": "archivo",
#                       "salida": "file:///Users/rlh/LocalDeveloper/LMASSPocs/ingestor/err-files",
#                       "validador_campo_invalido": "xxxxxx",
#                       "archivo_errores_salida": "file:///Users/rlh/LocalDeveloper/LMASSPocs/ingestor/err-files",
#                       "debug": true,
#       (opcional)     "estadisticas" : {
#                         "salida": "file:///Users/rlh/LocalDeveloper/LMASSPocs/ingestor/stat-files"
#                       }
#                     }'
#                    'json': '{
#                       "nombre": "retailOrders",
#                       "atributos": [
#                           { "nombre": "date",
#                             "posicion": 0,
#                             "tipo": "Fecha",
#                             "patronFecha": "yyyy-MM-dd",
#                             "fechaInicio": "2018-06-30",
#                             "validadores": ["rangoFecha","min"]
#                            },
#                            { "nombre": "storeID",
#                              "posicion": 1,
#                              "id": true,
#                              "tipo": "Texto",
#                              "longitud": 8,
#                              "validadores": ["longitudExacta"]
#                             },
#                             { "nombre": "zip",
#                               "posicion": 2,
#                               "tipo": "conjunto",
#                               "elementos": ["95126","95137","94936","95024","94925","94501","94502","94509",
#                                             "94513","94531","94561","94565","94541","94546","94552","94577", "94578"],
#                               "validadores": ["existe","pertenece"]
#                              },
#                              { "nombre": "orderID",
#                                "posicion": 3,
#                                "id": true,
#                                "tipo": "Texto"
#                                },
#                                { "nombre": "productID",
#                                  "posicion": 4,
#                                  "id": true,
#                                  "tipo": "Texto",
#                                  "validadores": ["existe","perteneceExt"]
#                                },
#                                { "nombre": "category",
#                                  "posicion": 5,
#                                  "tipo": "conjunto",
#                                  "elementos": ["general","produce","poultry","dairy","beverages","bakery",
#                                                "canned food","frozen food","meat","cleaner","paper goods"],
#                                  "validadores": ["existe","pertenece"]
#                                 },
#                                 { "nombre": "quantity",
#                                    "posicion": 6,
#                                    "tipo": "Entero",
#                                    "min": 1,
#                                    "validadores": ["esEntero","min"]
#                                  },
#                                  { "nombre": "amount",
#                                    "posicion": 7,
#                                    "tipo": "Real",
#                                    "max": 1000,
#                                    "validadores": ["max"]
#                                  }
#                                ],
#                                "validadoresReg": ["pipedMaxMonetaryAmount"]
#                              },
#                              'mapping':
#                 '(date, fecha), (storeID, tiendaID), (productID, productoID), (quantity, cantidad), (amount, monto)'
#                            }
#                          ]
#                         }
#                      }
# project: ingestor
# author: rlh
# date: December 2023
class Configuration:

    def __init__(self, datasource_name):
        self.datasource_name = datasource_name
        self.query = """
            query dss($input: String) {
                  datasources(nombre: $input) {
                    nombre
                    config
                    json
                    mapping
                  }
                }
        """
        self.url = os.getenv("URL_PARAM")
        self.client = None
        self.config = {}
        self.schema = None
        self.mapping = Mapping()
        self.statistics = None

    def initConfiguration(self, datasource_name):
        if datasource_name != self.datasource_name:
            self.client = GraphqlClient(endpoint=self.url)
            logging.debug(f'Will read configuration:{self.url}')
            variables = {"input": datasource_name}
            try:
                response = self.client.execute(query=self.query, variables=variables)
                if 'data' not in response:
                    logging.error(f'Bad Json response[data]:{response}')
                else:
                    data = response['data']
                    if 'datasources' not in data:
                        logging.error(f'Bad Json response[datasources]:{data}')
                    else:
                        datasources = data['datasources']
                        if len(datasources) == 0:
                            logging.warning(f'Datasource does not exists:{response}')
                        else:
                            logging.debug(f'Configuration found:{datasources[0]}')
                            if 'config' not in datasources[0]:
                                logging.error(f'Bad Json response[datasources[0][config]:{response}')
                            else:
                                self.config = json.loads(datasources[0]['config'])
                                # validate is need statistics
                                if 'estadisticas' in self.config:
                                    if 'estadisticas' in self.config['estadisticas']:
                                        self.statistics = Statistics(self.config['estadisticas']['estadisticas'])
                                if 'json' in datasources[0]:
                                    self.schema = datasources[0]['json']
                                if 'mapping' in datasources[0]:
                                    self.mapping.initMapping(datasources[0]['mapping'])
            except Exception as e:
                logging.error(f"Error en lectura de la configuración de {datasource_name}")
                logging.error(f"Error:{e}")
                self.config = {}

    def fieldDelIn(self):
        if 'delimitadores' in self.config:
            if 'entrada' in self.config['delimitadores']:
                return self.config['delimitadores']['entrada']
        return ','

    def fieldDelOut(self):
        if 'delimitadores' in self.config:
            if 'salida' in self.config['delimitadores']:
                return self.config['delimitadores']['salida']
        return ','

    def withHeader(self):
        if 'delimitadores' in self.config:
            if 'encabezado' in self.config['delimitadores']:
                return self.config['delimitadores']['encabezado']

        return False

    def fileInputType(self):
        if 'formato_entrada' in self.config:
            return self.config['formato_entrada']
        return 'csv'

    def output(self):
        if 'tipo_salida' in self.config:
            if self.config['tipo_salida'] == 'archivo':
                return OutputType.FILE
            elif self.config['tipo_salida'] == 'mail':
                return OutputType.MAIL
            elif self.config['tipo_salida'] == 'evento':
                return OutputType.EVENT
        return OutputType.FILE

    def outputPath(self):
        if 'salida' in self.config:
            return self.config['salida']
        return './out-files'

    def outputPathError(self):
        if 'archivo_errores_salida' in self.config:
            return self.config['archivo_errores_salida']
        return './err-files'

    def debug(self):
        if 'debug' in self.config:
            return self.config['debug']
        return False

    # ==== End Configuration


class OutputType(Enum):
    FILE = 1
    MAIL = 2
    EVENT = 3


#
# This class gets the statistics (optional) to be output after processing the file
#
class Statistics:

    def __init__(self, stats):
        if 'salida' in stats:
            self.output_file = stats['salida']
        else:
            self.output_file = './stat-files'


# This class makes the mapping between in attributes to out attribute
#
# project: ingestor
# author: rlh
# date: November 2022
class Mapping:

    def __init__(self):
        self._mapping = {}

    def initMapping(self, mp):
        pairs = list(mp.split('('))
        for pair in pairs:
            if pair != "":
                first = pair[0:pair.index(',')]
                second = pair[pair.index(',') + 2:pair.index(')')]
                self._mapping[first] = second

    def getMapping(self, attr_name):
        if attr_name in self._mapping:
            return self._mapping[attr_name]
        return None

    def sqlMapping(self, dy_types):
        sql_expr = 'select '
        for col in dy_types:
            map_col = self.getMapping(col[0])
            if map_col is not None:
                sql_expr = sql_expr + col[0] + ' as ' + map_col + ', '
        return sql_expr[:-2]


