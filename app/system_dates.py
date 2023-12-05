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
#  session.py
#
#  Developed 2022 by LMASS Desarrolladores, S.C. www.legosoft.com.mx
#
from datetime import datetime, timedelta
import logging


# ============================================================================================================
# System dates validation
class SystemDates:

    def __init__(self):
        self.systemDates = []
        self.today = datetime.today()  # these default values are not good. Use the ones stored yn paramDB
        self.tomorrow = self.today + timedelta(days=1)
        self.yesterday = self.today + timedelta(days=-1)

    def allSystemDates(self, all_system_dates):
        for day in all_system_dates:
            date_type = day['name']     # name & day must exist
            if date_type == 'HOY':
                self.today = datetime.strptime(day['day'], '%Y-%m-%d')
            elif date_type == 'MANANA':
                self.tomorrow = datetime.strptime(day['day'], '%Y-%m-%d')
            elif date_type == 'AYER':
                self.yesterday = datetime.strptime(day['day'], '%Y-%m-%d')
            elif date_type == 'FESTIVO':
                self.systemDates.append(datetime.strptime(day['day'], '%Y-%m-%d'))
            else:
                logging.error(f'Invalid system date found:{day}')

    def isHoliday(self, date, attr):
        weekday = date.weekday()
        if weekday == 5 or weekday == 6:
            return '200', f'La fecha {date} es un día festivo'
        if date in self.systemDates:
            return '201', f'La fecha {date} es un día festivo'
        return None

    def isFuture(self, date, attr):
        if date < self.tomorrow:
            return '202', f'La fecha {date} es anterior al dia de mañana {self.tomorrow}'
        return None

    def isIncFuture(self, date, attr):
        if date <= self.tomorrow:
            return '203', f'La fecha {date} es anterior al dia de mañana {self.tomorrow}'
        return None

    def isPast(self, date, attr):
        if date > self.yesterday:
            return '204', f'La fecha {date} es posterior al dia de ayer {self.yesterday}'
        return None

    def isIncPast(self, date, attr):
        if date >= self.tomorrow:
            return '205', f'La fecha {date} es posterior al dia de ayer {self.yesterday}'
        return None

    def isToday(self, date, attr):
        if date == self.today:
            return None
        return '206', f"La fecha {date} no es el día de 'hoy' ({self.today})"

