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
import zipfile
import tempfile

from hydra_client.output import write_progress, write_output, create_xml_response
from hydra_client import RequestError
from hydra_client import HydraPluginError
from hydra_base.lib.objects import JSONObject

import json

import os, sys

from datetime import datetime
import hydra_pywr_common

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

        self.new_network = None
        self.input_network = None
        self.attr_negid_posid_lookup = {}
        self.type_id_map = {} # a mapping from a type ID to a type object
        self.name_maps = {'NODE': {}, 'LINK': {}, 'GROUP': {}}

        #This is a special case to cater for the fact that the NAME of a network type
        #often changes from one template to another, even when then node type names
        #remain the same.
        self.network_template_type = None

        #3 steps: start, read, save
        self.num_steps = 3

    def import_network(self, network, template_id, project_id, network_name=None):
        """
            Read the file containing the network data and send it to
            the server.
        """

        write_output("Reading Network")
        write_progress(2, self.num_steps)

        if network is not None:

            if zipfile.is_zipfile(network):
                log.info("File is zipped...extracting..")
                tmp_folder = tempfile.mkdtemp()
                zip_ref = zipfile.ZipFile(network, 'r')
                zip_ref.extractall(tmp_folder)
                zip_ref.close()

                # Looking inside the extracted folder for the file to import. Navigating eventual subfolders tree to the json file
                network = self.get_network_file_name(tmp_folder, {})

            with open(network, 'r') as netfile:
                json_data = json.load(netfile)

            self.input_network = JSONObject(json_data['network'])

            if project_id is None:
                project = self.create_project()
                json_data['network']['project_id'] = project['id']
            else:
                json_data['network']['project_id'] = project_id

            self.make_attribute_id_mapping(json_data.get('attributes', []))


            #Replace the attr_id for each resource attribute with the DB's correct ID
            for ra_j in self.input_network.attributes:
                ra_j.attr_id = self.attr_negid_posid_lookup[ra_j.attr_id]

            self.input_network.project_id = project_id

            if template_id is None:
                raise HydraError("Please specifiy a template")

            self.template_id = template_id

            if network_name:
                self.input_network.name = network_name


            #make all the negative type and attribute IDs into positive ones from the DB
            self.update_type_and_attribute_ids()

            write_output("Saving Network")
            write_progress(3, self.num_steps)

            #The network ID can be specified to get the network...
            self.new_network = self.client.add_network(self.input_network)

            self.add_rules(json_data.get('rules', []))

            write_output(f"Network {self.new_network.name} imported with ID {self.new_network.id}.\n"+
                         f"Scenario ID:{self.new_network.scenarios[0].id}")
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
        project_id = self.input_network.get('project_id')
        if project_id is not None:
            try:
                project = self.client.call('get_project', {'project_id':project_id})
                log.info('Loading existing project (ID=%s)' % project_id)
                return project
            except RequestError:
                log.info('Project ID not found. Creating new project')

        #Using 'datetime.now()' in the name guarantees a unique project name.
        new_project = dict(
            name="Project for network %s created at %s" % (network['name'], datetime.now()),
            description=\
            "Default project created by the %s plug-in." % \
                (self.__class__.__name__),
        )
        saved_project = self.client.call('add_project', {'project':new_project})
        return saved_project

    def make_attribute_id_mapping(self, json_attributes):
        """
            Create a mapping from the attributes contained in the json_data
            to positive ids in the database. If an attribute does not exist in the
            database, create it.
            args:
                json_attributes: A list of attribute objects containing
        """

        all_attributes = self.client.get_attributes()

        #Map a name/dimension combo to a positive DB id
        attr_name_id_lookup = {}
        for a in all_attributes:
            attr_name_id_lookup[(a.name.lower().strip(), a.dimension_id)] = a.id

        dimensions = self.client.get_dimensions()
        dimension_map = {d.name.lower(): d.id for d in dimensions}

        #Map the file's negative attr_id to the DB's positive ID
        for neg_id in json_attributes:
            attr_j = JSONObject(json_attributes[neg_id])

            if attr_j.dimension is None or attr_j.dimension.strip() == '':
                attr_j.dimension_id = None
            else:
                attr_j.dimension_id = dimension_map[attr_j.dimension.lower()]

            #Attribute not in the DB?
            if attr_name_id_lookup.get((attr_j.name.lower().strip(), attr_j.dimension_id)) is None:
                #Add it
                newattr = self.client.add_attribute(attr_j)
                #Add it to the name/dimension -> lookup
                attr_name_id_lookup[(newattr.name.lower().strip(), newattr.dimension_id)] = newattr.id

            #Add the id to the negative id -> positive id map
            self.attr_negid_posid_lookup[int(neg_id)] = attr_name_id_lookup[(attr_j.name.lower().strip(),
                                                                             attr_j.dimension_id)]

    def update_type_and_attribute(self, resource_j):
        """
            Update the attribute and type IDS for a single resource (node, link, group).
            args:
                resource_j (dict): THe node, link or group
            returns:
                None
        """
        if (len(resource_j.types)>0):
            # If the node has type
            resource_j.types = [self.type_id_map[resource_j.types[0].name]]

        #Replace the attr_id for each resource attribute with the DB's correct ID
        for ra_j in resource_j.attributes:
            ra_j.attr_id = self.attr_negid_posid_lookup[ra_j.attr_id]

    def update_type_and_attribute_ids(self):
        """
            For each node, link and group in the network change its attribute ID to
            the newly created positive ID, and do the same with their type ID.
        """

        self.get_type_name_map()

        if len(self.input_network.types)>0:
            # If the network has type
            self.input_network.types = [self.network_template_type]

        #map the name of the nodes, links and groups to its negative ID
        for n_j in self.input_network.nodes:
            self.name_maps['NODE'][n_j.name] = n_j.id
            self.update_type_and_attribute(n_j)

        for l_j in self.input_network.links:
            self.name_maps['LINK'][l_j.name] = l_j.id
            self.update_type_and_attribute(l_j)

        for g_j in self.input_network.resourcegroups:
            self.name_maps['GROUP'][g_j.name] = g_j.id
            self.update_type_and_attribute(g_j)

    def get_type_name_map(self):
        """
            If an incoming network uses type names instead of IDs for cross-hydra
            compatibility, look up the appropriate template in the local hydra
            and set all the IDs to match the type names
        """

        if not hasattr(self.input_network, 'types') or len(self.input_network.types) == 0:
            log.info("Network %s has no type", self.input_network.name)
            return

        template = self.client.get_template(self.template_id)
        if template is None:
            log.info("Template %s is not found", self.template_id)
            return
        for t in template.templatetypes:
            if t.resource_type == 'NETWORK':
                self.network_template_type = t
            self.type_id_map[t.name] = t

    def create_reverse_id_lookups(self):
        """
            Create mappings for nodes, links and groups from the negative IDS which came in the JSON
            to the new positive IDS from the database. THis relies on name matching. THis is OK because
            we've just created a network in which node, link and group names must be unique and the network
            has not been altered in any way since creation
            returns:
                reverse_id_lookups (dict of dicts). As we're iterating over the whole network, we can create
                                                    a mapping from the negative IDS to positive IDS for each
                                                    resource (node, link, group), so we return it here.
        """
        reverse_id_lookups = {'ATTRIBUTE': self.attr_negid_posid_lookup,
                              'NODE': {},
                              'LINK': {},
                              'GROUP': {}}
        #Map the negative IDS of the nodes to their positive counterparts
        for n in self.new_network.nodes:
            reverse_id_lookups['NODE'][self.name_maps['NODE'][n.name]] = n.id

        #Map the negative IDS of the links to their positive counterparts
        for l in self.new_network.links:
            reverse_id_lookups['LINK'][self.name_maps['LINK'][l.name]] = l.id

        #Map the negative IDS of the groups to their positive counterparts
        for g in self.new_network.resourcegroups:
            reverse_id_lookups['GROUP'][self.name_maps['GROUP'][g.name]] = g.id

        return reverse_id_lookups

    def add_rules(self, json_rules):

        rule_type_definition_codes = [rtd.code for rtd in self.client.get_rule_type_definitions()]

        for r in json_rules:
            r['id'] = None
            r['network_id'] = self.new_network.id

            #make sure rule types are added
            for t in r.get('types', []):
                if t['code'] not in rule_type_definition_codes:
                    if t.get('typedefinition') is not None:
                        self.client.add_rule_type_definition(JSONObject(t['typedefinition']))
                    else:
                        # if the rule hasn't come with a typedefintiion, just make one where the name is the same as the code
                        self.client.add_rule_type_definition(JSONObject({'code':t['code'], 'name': t['name']}))

            self.client.add_rule(JSONObject(r))


    def get_network_file_name(self, base_path, not_valid_filenames):
        # Starting from the passed path, finds the first valid file and return its path
        foldercontents = os.listdir(base_path)

        #Remove any hidden files
        visible_files = []
        for f in foldercontents:
            if f[0] != '.' and f[0] != '_' and f not in not_valid_filenames:
                # We cannot add the just unzipped FILE!
                visible_files.append(f)

        last_file = visible_files[-1] #This ensures you ignore hidden files.

        data_dir = os.path.join(base_path, last_file)

        if os.path.isdir(data_dir):
            # If it is a folder, navigate inside
            data_dir = self.get_network_file_name(data_dir, not_valid_filenames)

        return data_dir


