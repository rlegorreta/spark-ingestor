# Copyright (c) 2023, LegoSoft Soluciones, S.C.
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
# configSystemDates.py
#
# Developed 2023 by LegoSoftSoluciones, S.C. www.legosoft.com.mx

# This clas is to avoid th message:
# ModuleNotFoundError: No module named 'python_graphql_client'
import logging
import os
from python_graphql_client import GraphqlClient


class ConfigSystemDates:

    def __init__(self):
        self.query = """
        query getAllSystemDates {
                systemDates {
                                name
                                day
                }
        }
        """
        self.client = None
        self.url = os.getenv("URL_PARAM")

    def getAllSystemDates(self):
        logging.debug(f'Will connect to param-service:{self.url}')
        self.client = GraphqlClient(endpoint=os.getenv("URL_PARAM"))
        response = self.client.execute(query=self.query)
        if 'data' not in response:
            logging.error(f'Bad json response[data] for systemDates:{response}')
        else:
            data = response['data']
            if 'systemDates' not in data:
                logging.error(f'Bad json response[systemDates]: {data}')
            else:
                logging.info('Read all system dates')
                return data['systemDates']
        return []
