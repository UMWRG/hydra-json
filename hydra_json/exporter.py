#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Copyright 2015 University of Manchester\
#\
# ExportJSON is free software: you can redistribute it and/or modify\
# it under the terms of the GNU General Public License as published by\
# the Free Software Foundation, either version 3 of the License, or\
# (at your option) any later version.\
#\
# ExportJSON is distributed in the hope that it will be useful,\
# but WITHOUT ANY WARRANTY; without even the implied warranty of\
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\
# GNU General Public License for more details.\
# \
# You should have received a copy of the GNU General Public License\
# along with ExportJSON.  If not, see <http://www.gnu.org/licenses/>\
#

"""A Hydra plug-in for exporting a hydra network to a JSON file.

Basics
~~~~~~

The plug-in for exporting a network to a JSON file.
Basic usage::

       ExportJSON.py [-h] [-n network_id] [-s scenario_id] [-d target_dir] [-x]

Options
~~~~~~~

====================== ====== ============ =======================================
Option                 Short  Parameter    Description
====================== ====== ============ =======================================
``--help``             ``-h``              Show help message and exit.
``--network-id         ``-n`` NETWORK_ID   The ID of the network to be exported.
``--scenario-id        ``-s`` SCENARIO_ID  The ID of the scenario to be exported.
                                           (optional)
``--target-dir``       ``-d`` TARGET_DIR   Target directory
``--as-xml``           ``-x`` AS_XML       Export to XML file instead of JSON.
``--server-url``       ``-u`` SERVER-URL   Url of the server the plugin will
                                           connect to.
                                           Defaults to localhost.
``--session-id``       ``-c`` SESSION-ID   Session ID used by the calling software.
                                           If left empty, the plugin will attempt
                                           to log in itself.
====================== ====== ============ =======================================

"""
import os
import json
import tempfile
import time
import re
import logging
import zipfile
from hydra_client.objects import ExtendedDict
from hydra_client.

from hydra_client.output import write_progress,\
                               write_output


LOG = logging.getLogger(__name__)

class ExportJSON:
    """
       Exporter of Hydra networks to JSON or XML files.
    """

    def __init__(self, client):

        #Record the names of the files created by the plugin so we can
        #display them to the user.
        self.files = []

        self.client = client

        self.num_steps = 3

        #A lookup from attr_id to attribute object
        self.attr_dict = {}

        self.dimension_lookup = {}

    def get_dimension_name(self, dimension_id):
        """
            Get the name of the dimension with the given ID
        """
        if dimension_id is None:
            return None
        dimension = self.dimension_lookup.get(dimension_id)
        if dimension is None:
            dimension = self.client.get_dimension(dimension_id)
            self.dimension_lookup[dimension.id] = dimension

        return dimension.name


    def update_attributes(self, resource):
        """
            For a given resource, extract the attributes from it.
        """
        #why is this not already a JSON Objject??
        resource.attributes = [ExtendedDict(a) for a in resource.attributes]
        for res_attr in resource.attributes:
            res_attr.id = res_attr.id * -1
            res_attr.attr_id = res_attr.attr_id * -1
            self.attr_dict[res_attr.attr_id] = ExtendedDict(
                {'name': res_attr.name,
                 'dimension':self.get_dimension_name(res_attr.dimension_id)})


    def export_network(self, network_id, scenario_id=None, target_dir=None,
                       newlines=False, zipped=False, include_results=True):
        """
            Export the network to a file. Requires a network ID. The
            other two are optional.

            Scenario_id is None: Include all scenarios in the network
            target_dir: Location of the resulting file.

            If this is None, export the file to the Desktop.

        """

        write_output("Retrieving Network")
        write_progress(2, self.num_steps)

        client = self.client

        if scenario_id is not None:
            scenario_id = [scenario_id]

        network_j = client.get_network(network_id=network_id,
                                       scenario_id=scenario_id,
                                       include_maps=False,
                                       include_data=True,
                                       include_results=include_results)

        network_templates = []

        if network_j.types is not None and len(network_j.types) > 0:
            template_id = network_j.types[0].template_id

            tmpl = client.get_template_as_json(template_id=template_id)
            network_templates.append(tmpl)

        self.update_attributes(network_j)

        for node in network_j.nodes:
            node.id = node.id * -1
            self.update_attributes(node)

        for link in network_j.links:
            link.id = link.id * -1
            link.node_1_id = link.node_1_id * -1
            link.node_2_id = link.node_2_id * -1
            self.update_attributes(link)

        for group in network_j.resourcegroups:
            group.id = group.id * -1
            self.update_attributes(group)

        for scenario in network_j.scenarios:
            scenario.resourcescenarios_1 = []

            for r_s in scenario.resourcescenarios:
                new_rs = ExtendedDict({})
                new_rs.resource_attr_id = r_s.resource_attr_id * -1
                dataset = r_s.dataset
                new_rs.dataset = ExtendedDict(dataset)
                scenario.resourcescenarios_1.append(new_rs)

            scenario.resourcescenarios = scenario.resourcescenarios_1

            for rgi in scenario.resourcegroupitems:
                if rgi.node_id is not None:
                    rgi.ref_id = rgi.node_id * -1
                if rgi.subgroup_id is not None:
                    rgi.ref_id = rgi.subgroup_id * -1
                if rgi.link_id is not None:
                    rgi.ref_id = rgi.link_id * -1
                rgi.group_id = rgi.group_id * -1

        # Creating the timestamp to add at the end of the filename

        rules = client.get_resource_rules(ref_key='NETWORK',
                                          ref_id=network_j.id)


        output_data = {'attributes': self.attr_dict,
                       'network': network_j,
                       'templates': network_templates,
                       'rules': rules}

        additional_data = self.get_additional_data()

        output_data.update(additional_data)

        dump_kwargs = {}
        if newlines is True:
            dump_kwargs["indent"] = 0

        final_data = json.dumps(output_data, **dump_kwargs)

        self.write_network(network_j.name, final_data, target_dir, zipped=zipped)

        LOG.info("File export complete.")



    def get_additional_data(self):
        """
            Get any auxiliary information such as metrics that aren't necessarily
            stored in hydra-base.
        """
        #TODO: figure out how to get this information that isn't stored in hydra base????

        #TODO: ex: a way we don't have to reference metrics directly?
        #network_metrics = app.blueprint_fn_registry['get_network'](network_id, user_id)
        #for m in network_metrics:
        #    m.id = None
        #    m.node_ids = [-n for n in m.node_ids]
        #    m.link_ids = [-n for n in m.link_ids]
        #    m.group_ids = [-n for n in m.group_ids]
        #    m.network_id = [-n for n in m.network_id]#m.network_id is a list
        #    m.attribute_id = m.attribute_id * -1

        return {}

    def write_network(self, network_name, network_data, target_dir, zipped=False):
        """
            Write the network to a file
        """
        write_output("Writing network to file")
        write_progress(3, self.num_steps)

        # Returning the file
        if target_dir is None:
            target_dir = os.path.join(os.path.expanduser('~'), 'Desktop')

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        #replacing not ascii chars with "-"
        network_name = re.sub("[^A-Za-z0-9-_]", "-", network_name)

        location = os.path.join(target_dir, '%s.json'%(network_name))

        #Write the file
        with open(location, 'w') as output_file:
            output_file.write(network_data)

        #Now zip it if required
        if zipped is True:
            zip_file_name = '%s.zip'%(network_name)
            zip_location = os.path.join(target_dir, zip_file_name)
            with zipfile.ZipFile(zip_location, 'w') as zip_file:
                # writing each file one by one
                zip_file.write(location,
                               os.path.basename(location),
                               compress_type=zipfile.ZIP_DEFLATED)

        write_output("Network Written to %s "%(location))
