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
#  numbers.py
#
#  Developed 2022 by LMASS Desarrolladores, S.C. www.legosoft.com.mx
#
from decimal import Decimal


def isMultiple(value, attr):
    if not isinstance(value, int):
        return '300', f'El valor {value} debe de ser un entero'
    if 'batch' not in attr:
        return '301', f'Se debe definir un lote para ver si es múltiplo'
    if value % attr['batch'] != 0:
        return '302', f"El valor {value} no el múltiplo de {attr['batch']}"
    return None


def isMoney(value, attr):
    if not isinstance(value, Decimal):
        return '303', f'El valor {type(value)} debe de ser del tipo decimal'
    if value.as_tuple().exponent != -2:
        return '304', f'El valor {value} debe tener solo dos decimales'
    return None


def isIva(value, attr):
    if not isinstance(value, float, Decimal):
        return '305', f'El valor {type(value)} debe de ser del tipo decimal o float'
    if value != 11.0 and value != 16.0 and value != 0.0:
        return '306', f'El valor {value} debe de tener un valor de IVA válido'
    return None


def isPercentage(value, attr):
    if not isinstance(value, float, Decimal):
        return '307', f'El valor {type(value)} debe de ser del tipo decimal o float'
    if value < 0.0 or value > 1.0:
        return '308', f'El valor {value} debe ser un porcentaje'
    return None
