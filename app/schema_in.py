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
#  schema_in.py
#
#  Developed 2022 by LMASS Desarrolladores, S.C. www.legosoft.com.mx
#
import json
import logging
import uuid
import re
from enum import Enum
from pyspark.sql.types import *
from datetime import datetime

from numbers_validation import *
from session import session


# =============================================================================
# Schema class: this class gets the schema validation for input attribute
#
class Schema:

    def __init__(self):
        self._schema = {}
        self.attributes = {}
        self.validators = []
        self.columns = []
        self.system_dates = None
        self.DEFINED_PATTERNS = {'telefono': "^(\+\d{1,3}\s)?\(?\d{1,4}\)?[\s.-]\d{3}[\s.-]\d{4}$",
                                 'RFC': "/^([A-ZÑ&]{3,4}) ?(?:- ?)?(\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])) ?(?:- ?)?([A-Z\d]{2})([A\d])$/",
                                 'codigo_postal': '^\d{5}(?:[-\s]\d{4})?$',
                                 'email': "([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")@([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\[[\t -Z^-~]*])"
                                 }

    def initSchema(self, sc, system_dates):
        self.system_dates = system_dates
        self._schema = json.loads(sc)
        if 'atributos' in self._schema:
            for attribute in self._schema['atributos']:
                if 'nombre' in attribute:
                    name = attribute['nombre']
                else:
                    name = uuid.uuid4
                    logging.error(f'No se definió el nombre del atributo. Se declara uno a la zar {name}')
                self.columns.append(name)
                self.attributes[name] = self.parse(attribute)
            self.attributes['validatorsReg'] = self.attrRowValidators()

    # This is to optimize the parsing as a hash-map and avoid to call each method in the loop for each record
    def parse(self, attr):
        attr_dict = {}
        if 'tipo' in attr:
            tipo = attr['tipo']
        else:
            tipo = 'unknown'
        attr_dict['sparkAttrType'] = self.sparkAttrType(tipo)
        attr_dict['isRequired'] = self.attrIsRequired(attr)
        attr_dict['type'] = self.attrType(tipo)
        attr_dict['datePattern'] = self.attrDatePattern(attr)
        attr_dict['startDate'] = self.attrStartDate(attr)
        attr_dict['validators'] = self.attrValidators(attr)
        attr_dict['isId'] = self.attrIsId(attr)
        attr_dict['length'] = self.attrLength(attr)
        attr_dict['minLength'] = self.attrMinLength(attr)
        attr_dict['maxLength'] = self.attrMaxLength(attr)
        attr_dict['pattern'] = self.attrPattern(attr)
        if attr_dict['pattern'] is None:
            attr_dict['pattern'] = self.attrPatternDefined(attr)
        attr_dict['elements'] = self.attrElements(attr)
        attr_dict['min'] = self.attrMin(attr)
        attr_dict['max'] = self.attrMax(attr)
        attr_dict['batch'] = self.attrLote(attr)

        return attr_dict

    def schema(self):
        sc = StructType()
        for name in self.attributes.keys():
            if name != 'validatorsReg':
                sc.add(StructField(name, self.attributes[name]['sparkAttrType'],
                                   self.attributes[name]['isRequired']))

        return sc

    # Schema where all field type are String in order to validate first if the type is correct
    def preSchema(self):
        sc = StructType()
        for name in self.attributes.keys():
            if name != 'validatorsReg':
                sc.add(StructField(name, StringType(), True))

        return sc

    def sparkAttrType(self, tipo):
        if tipo == 'Texto':
            return StringType()
        elif tipo == 'Fecha':
            return DateType()
        elif tipo == 'FechaHora':
            return DayTimeIntervalType()
        elif tipo == 'Entero':
            return IntegerType()
        elif tipo == 'Real':
            return DoubleType()
        elif tipo == 'Decimal':
            return DecimalType()
        elif tipo == 'Conjunto':
            return StringType()

        return StringType()

    def attrType(self, tipo):
        if tipo == 'Texto':
            return AttrType.STRING
        elif tipo == 'Fecha':
            return AttrType.DATE
        elif tipo == 'FechaHora':
            return AttrType.DATE_TIME
        elif tipo == 'Entero':
            return AttrType.INTEGER
        elif tipo == 'Real':
            return AttrType.DOUBLE
        elif tipo == 'Decimal':
            return AttrType.DECIMAL
        elif tipo == 'Conjunto':
            return AttrType.ENUM
        else:
            return AttrType.UNKNOWN

    def attrIsRequired(self, attr):
        if 'requerido' in attr:
            return attr['requerido'] == 'true'
        return True

    def attrDatePattern(self, attr):
        if 'patron_fecha' in attr:
            return attr['patron_fecha']
        return '%Y-%m-%d'

    def attrStartDate(self, attr):
        if 'fecha_inicio' in attr:
            try:
                return datetime.strftime(attr['fecha_inicio'], '%Y-%m-%d')
            except:
                return None
        return '2000-01-01'

    def attrValidators(self, attr):
        if 'validadores' in attr:
            vals = []
            for validator in attr['validadores']:
                if validator == 'futuro':
                    val = self.system_dates.isFuture
                elif validator == 'futuroEq':
                    val = self.system_dates.isIncFuture
                elif validator == 'pasado':
                    val = self.system_dates.isPast
                elif validator == 'pasadoEq':
                    val = self.system_dates.isIncPast
                elif validator == 'hoy':
                    val = self.system_dates.isToday
                elif validator == 'habil':
                    val = self.system_dates.isHoliday
                elif validator == 'multiplo':
                    val = isMultiple
                elif validator == 'dinero':
                    val = isMoney
                elif validator == 'iva':
                    val = isIva
                elif validator == 'porcentaje':
                    val = isPercentage
                else:
                    val = None

                if val is not None:
                    vals.append(val)
                    self.validators.append(val)
            return vals
        return []

    def attrIsId(self, attr):
        if 'id' in attr:
            return attr['id']
        return False

    def attrLength(self, attr):
        if 'longitud' in attr:
            return attr['longitud']
        return None

    def attrMinLength(self, attr):
        if 'min_longitud' in attr:
            return attr['min_longitud']
        return None

    def attrMaxLength(self, attr):
        if 'max_longitud' in attr:
            return attr['max_longitud']
        return None

    def attrPattern(self, attr):
        if 'patron' in attr:
            return re.compile(r'^.*' + attr['patron'])
        return None

    def attrPatternDefined(self, attr):
        if 'patron_pre_definido' in attr and attr['patron_pre_definido'] in self.DEFINED_PATTERNS:
            return re.compile(r'^.*' + self.DEFINED_PATTERNS[attr['patron_pre_definido']])
        return None

    def attrMin(self, attr):
        if 'min' in attr:
            return attr['min']
        return None

    def attrMax(self, attr):
        if 'max' in attr:
            return attr['max']
        return None

    def attrLote(self, attr):
        if 'lote' in attr:
            return attr['lote']
        return None

    def attrElements(self, attr):
        if 'elementos' in attr:
            elements = []
            for element in attr['elementos']:
                elements.append(element)
            return elements
        return None

    def attrRowValidators(self):
        if 'validadores_reg' in self._schema:
            validators = []
            for validator in self._schema['validadores_reg']:
                validators.append(validator)  # TODO createValidator(validator)
            if session.spark_cntxt:
                session.spark_cntxt.broadcast(validators)
            return validators
        return []

    def finishSchema(self):
        if session.spark_cntxt:
            session.spark_cntxt.broadcast(self.validators)


class AttrType(Enum):
    STRING = 1
    INTEGER = 2
    DOUBLE = 3
    DECIMAL = 4
    DATE = 5
    DATE_TIME = 6
    ENUM = 7
    UNKNOWN = 8
