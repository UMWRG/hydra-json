#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Copyright 2013, 2014, 2015 University of Manchester\
#\
# ImportJSON is free software: you can redistribute it and/or modify\
# it under the terms of the GNU General Public License as published by\
# the Free Software Foundation, either version 3 of the License, or\
# (at your option) any later version.\
#\
# ImportJSON is distributed in the hope that it will be useful,\
# but WITHOUT ANY WARRANTY; without even the implied warranty of\
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\
# GNU General Public License for more details.\
# \
# You should have received a copy of the GNU General Public License\
# along with ImportJSON.  If not, see <http://www.gnu.org/licenses/>\
#

import argparse as ap
import logging

from hydra_client.output import write_progress, write_output, create_xml_response
from hydra_client import RequestError
from hydra_client import HydraPluginError

import json

import os, sys

from datetime import datetime

log = logging.getLogger(__name__)

__location__ = os.path.split(sys.argv[0])[0]

class ImportJSON:
    """
       Importer of JSON files into Hydra. Also accepts XML files.
    """

    Network = None

    def __init__(self, client):

        self.warnings = []
        self.files = []

        self.client = client

        #3 steps: start, read, save
        self.num_steps = 3

    def import_network(self, network, template_id, project_id):
        """
            Read the file containing the network data and send it to
            the server.
        """

        write_output("Reading Network")
        write_progress(2, self.num_steps)

        if network is not None:
            network_data = json.load(network)

            if project_id is None:
                project = self.create_project(network_data)
                network_data['project_id'] = project['id']
            else:
                network_data['project_id'] = project_id

            write_output("Saving Network")
            write_progress(3, self.num_steps)

            #The network ID can be specified to get the network...
            network = self.client.call('add_network', {'net':network_data})
        else:
            raise HydraPluginError("A network ID must be specified!")
        return network

    def import_template(self, template_file):
        """
            Import a template file
        """
        template = self.client.import_template_json(template_file)
        return template.id

    def create_project(self, network):
        """
            If a project ID is not specified within the network, a new one
            must be created to hold the incoming network.
            If an ID is specified, we must retrieve the project to make sure
            it exists. If it does not exist, then a new project is created.

            Returns the project object so that the network can access it's ID.
        """
        project_id = network.get('project_id')
        if project_id is not None:
            try:
                project = self.client.call('get_project', {'project_id':project_id})
                log.info('Loading existing project (ID=%s)' % project_id)
                return project
            except RequestError:
                log.info('Project ID not found. Creating new project')

        #Using 'datetime.now()' in the name guarantees a unique project name.
        new_project = dict(
            name = "Project for network %s created at %s" % (network['name'], datetime.now()),
            description = \
            "Default project created by the %s plug-in." % \
                (self.__class__.__name__),
        )
        saved_project = self.client.call('add_project', {'project':new_project})
        return saved_project
