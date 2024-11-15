import asyncio
from uuid import uuid4

import pandas as pd
from PyDataCore import DataPool
from qasync import QEventLoop
from PySide6.QtWidgets import QApplication
import sys

from tabulate import tabulate

from pydataprocessor.core_node_processor import CoreProcessorFactory
from PyDataCore import DataPool

# class NodePool:
#     def __init__(self, datapool: DataPool):
#         self.datapool = datapool
#         self.nodes_register = pd.DataFrame(columns=[
#             'node_id', 'node_name', 'node_type', 'core', 'data_processed', 'input_ports_ready',
#             'output_ports_acknowledged', 'all_parameters_are_set'
#         ])
#         self.input_ports_register = pd.DataFrame(columns=[
#             'node_id', 'port_number', 'port_id', 'data_type', 'source_port_id', 'data_processed', 'received_data'
#         ])
#         self.output_ports_register = pd.DataFrame(columns=[
#             'node_id', 'port_number', 'port_id', 'data_type', 'subscriber_port_id', 'data_available'
#         ])
#         self.parameters_register = pd.DataFrame(columns=[
#             'node_id', 'parameter_id', 'parameter_name', 'parameter_type', 'parameter_value', 'parameter_is_set'
#         ])
#
#     def add_node(self, node_id, node_type, core_type):
#         core_processor = CoreProcessorFactory.create_processor(core_type, node_id, self, self.datapool)
#         self.nodes_register = pd.concat([
#             self.nodes_register,
#             pd.DataFrame([{
#                 'node_id': node_id, 'node_name': f"{core_type} Processor", 'node_type': node_type,
#                 'core': core_processor, 'data_processed': False, 'input_ports_ready': False,
#                 'output_ports_acknowledged': False, 'all_parameters_are_set': False
#             }])
#         ], ignore_index=True)
#
#     def add_input_port(self, node_id, port_number, data_type, source_port_id=None):
#         port_id = str(uuid4())
#         self.input_ports_register = pd.concat([
#             self.input_ports_register,
#             pd.DataFrame([{
#                 'node_id': node_id, 'port_number': port_number, 'port_id': port_id,
#                 'data_type': data_type, 'source_port_id': source_port_id, 'data_processed': False, 'received_data': None
#             }])
#         ], ignore_index=True)
#         return port_id
#
#     def add_output_port(self, node_id, port_number, data_type, subscriber_port_id=None):
#         port_id = str(uuid4())
#         self.output_ports_register = pd.concat([
#             self.output_ports_register,
#             pd.DataFrame([{
#                 'node_id': node_id, 'port_number': port_number, 'port_id': port_id,
#                 'data_type': data_type, 'subscriber_port_id': subscriber_port_id, 'data_available': False
#             }])
#         ], ignore_index=True)
#         return port_id
#
#     def connect_ports(self, output_port_id, input_port_id):
#         self.input_ports_register.loc[
#             self.input_ports_register['port_id'] == input_port_id, 'source_port_id'
#         ] = output_port_id
#         self.output_ports_register.loc[
#             self.output_ports_register['port_id'] == output_port_id, 'subscriber_port_id'
#         ] = input_port_id
#
#     async def start_processing_chain(self):
#         for _, node in self.nodes_register.iterrows():
#             await self._process_node(node['node_id'])
#
#     async def _process_node(self, node_id):
#         node_row = self.nodes_register[self.nodes_register['node_id'] == node_id]
#         core = node_row['core'].values[0]
#
#         input_data = self._gather_input_data(node_id)
#         if input_data is not None:
#             produced_data = await core.process(input_data)
#             self.nodes_register.loc[self.nodes_register['node_id'] == node_id, 'data_processed'] = True
#             self._distribute_data_to_outputs(node_id, produced_data)
#         elif input_data is None:
#             produced_data = await core.process()
#             self.nodes_register.loc[self.nodes_register['node_id'] == node_id, 'data_processed'] = True
#             self._distribute_data_to_outputs(node_id, produced_data)
#
#     def _gather_input_data(self, node_id):
#         inputs_ready = self.input_ports_register[
#             (self.input_ports_register['node_id'] == node_id) &
#             (self.input_ports_register['data_processed'])
#             ]
#         if inputs_ready.empty:
#             return None
#         return [inputs_ready['received_data'].values[0]]
#
#     def _distribute_data_to_outputs(self, node_id, produced_data):
#         """Distribue les données produites aux ports de sortie et informe les ports d’entrée abonnés."""
#         output_ports = self.output_ports_register[self.output_ports_register['node_id'] == node_id]
#
#         for _, output_row in output_ports.iterrows():
#             output_port_id = output_row['port_id']
#             subscribers = self.input_ports_register[
#                 self.input_ports_register['source_port_id'] == output_port_id
#                 ]
#
#             # Distribuer `produced_data` pour chaque port d'entrée souscripteur
#             for subscriber_index in subscribers.index:
#                 # Mise à jour de la colonne `data_processed` pour marquer les données comme prêtes
#                 self.input_ports_register.at[subscriber_index, 'data_processed'] = produced_data
#
#     def print_all_registers(self):
#         print("Input Ports Register:")
#         print(tabulate(self.input_ports_register, headers='keys', tablefmt='pretty'))
#         print("\nOutput Ports Register:")
#         print(tabulate(self.output_ports_register, headers='keys', tablefmt='pretty'))
#         print("\nNode Register:")
#         print(tabulate(self.nodes_register, headers='keys', tablefmt='pretty'))
#         print("\nParameters Register:")
#         print(tabulate(self.parameters_register, headers='keys', tablefmt='pretty'))
#
#     def add_parameter(self, node_id, parameter_id, parameter_name, parameter_type, parameter_value=None):
#         """Ajoute un paramètre au registre de paramètres pour un nœud spécifique."""
#         new_param = {
#             'node_id': node_id,
#             'parameter_id': parameter_id,
#             'parameter_name': parameter_name,
#             'parameter_type': parameter_type,
#             'parameter_value': parameter_value,
#             'parameter_is_set': parameter_value is not None
#         }
#         self.parameters_register = pd.concat(
#             [self.parameters_register, pd.DataFrame([new_param])], ignore_index=True
#         )

import asyncio
import pandas as pd
from PyDataCore import DataPool,Data_Type
from qasync import QEventLoop
from PySide6.QtWidgets import QApplication
import sys
from tabulate import tabulate


class NodePool:
    def __init__(self, datapool: DataPool):
        self.datapool = datapool

        # Register to track each node, its type, processing status, and port states
        self.nodes_register = pd.DataFrame(columns=[
            'node_id', 'node_name', 'node_type', 'core', 'data_processed',
            'input_ports_ready', 'output_ports_acknowledged', 'all_parameters_are_set'
        ])

        # Register to manage input ports with states and data linkage
        self.input_ports_register = pd.DataFrame(columns=[
            'node_id', 'port_number', 'port_id', 'data_type', 'source_port_id',
            'data_processed', 'received_data'
        ])

        # Register to manage output ports with availability and subscribers
        self.output_ports_register = pd.DataFrame(columns=[
            'node_id', 'port_number', 'port_id', 'data_type', 'subscriber_port_id',
            'data_available'
        ])

        # Register to track parameters for each node, their state, and values
        self.parameters_register = pd.DataFrame(columns=[
            'node_id', 'parameter_id', 'parameter_name', 'parameter_type',
            'parameter_value', 'parameter_is_set'
        ])

        # Events to control flow
        self.data_ready_events = {}
        self.data_processed_events = {}

    def declare_node(self, node_name, node_type, core):
        """Adds a node to the register and initializes its ports and parameters."""
        #création d'un identifiant unique pour le port via uusid
        node_id = str(uuid4())
        core = CoreProcessorFactory.create_processor(core, node_id, self, self.datapool)
        self.nodes_register = pd.concat([
            self.nodes_register,
            pd.DataFrame([{
                'node_id': node_id,
                'node_name': node_name,
                'node_type': node_type,
                'core': core,
                'data_processed': False,
                'input_ports_ready': False,
                'output_ports_acknowledged': False,
                'all_parameters_are_set': False
            }])
        ], ignore_index=True)
        return node_id

    def add_input_port(self, node_id, port_number,port_name, data_type, source_port_id=None):
        """Adds an input port to the register and links to a data source if specified."""
        port_id = f"{node_id}_input_{port_number}"
        self.input_ports_register = pd.concat([
            self.input_ports_register,
            pd.DataFrame([{
                'node_id': node_id,
                'port_number': port_number,
                'port_id': port_id,
                'port_name': port_name,
                'data_type': data_type,
                'source_port_id': source_port_id,
                'data_processed': False,
                'received_data': False
            }])
        ], ignore_index=True)

        # Initialize data ready event
        self.data_ready_events[port_id] = asyncio.Event()
        return port_id

    def add_output_port(self, node_id, port_number,port_name=None, data_type = None, subscriber_port_id=None):
        """Adds an output port to the register and optionally assigns a subscriber.
        :param port_name:
        """
        port_id = f"{node_id}_output_{port_number}"
        self.output_ports_register = pd.concat([
            self.output_ports_register,
            pd.DataFrame([{
                'node_id': node_id,
                'port_number': port_number,
                'port_id': port_id,
                'port_name': port_name,
                'data_type': data_type,
                'subscriber_port_id': subscriber_port_id,
                'data_available': False
            }])
        ], ignore_index=True)

        # Initialize data processed event
        self.data_processed_events[port_id] = asyncio.Event()
        return port_id

    def add_parameter(self, node_id, parameter_id, parameter_name, parameter_type, parameter_value=None):
        """Adds a parameter to the parameters register and sets initial state as not set."""
        new_param = pd.DataFrame([{
            'node_id': node_id,
            'parameter_id': parameter_id,
            'parameter_name': parameter_name,
            'parameter_type': parameter_type,
            'parameter_value': parameter_value,
            'parameter_is_set': parameter_value is not None
        }])

        self.parameters_register = pd.concat(
            [self.parameters_register, new_param.dropna(axis=1, how='all')],
            ignore_index=True
        )
        self._check_all_parameters_set(node_id)

    def update_parameter(self, node_id, parameter_name, value):
        """Updates a parameter's value and sets its 'is_set' status to True."""
        param_index = self.parameters_register[
            (self.parameters_register['node_id'] == node_id) &
            (self.parameters_register['parameter_name'] == parameter_name)
            ].index

        if not param_index.empty:
            self.parameters_register.loc[param_index, 'parameter_value'] = value
            self.parameters_register.loc[param_index, 'parameter_is_set'] = True
            self._check_all_parameters_set(node_id)
        else:
            raise ValueError(f"Parameter {parameter_name} for node {node_id} not found")

    def _check_all_parameters_set(self, node_id):
        """Checks if all parameters for a node are set, and updates the node status."""
        params = self.parameters_register[self.parameters_register['node_id'] == node_id]
        all_set = params['parameter_is_set'].all()
        self.nodes_register.loc[self.nodes_register['node_id'] == node_id, 'all_parameters_are_set'] = all_set

    async def start_processing_chain(self):
        """Starts the processing chain by iterating over nodes and processing them in sequence."""
        for _, node in self.nodes_register.iterrows():
            await self._process_node(node['node_id'])

    async def _process_node(self, node_id):
        """Processes a node if all input data is ready and parameters are set."""
        node_row = self.nodes_register[self.nodes_register['node_id'] == node_id]
        if node_row['input_ports_ready'].values[0] and node_row['all_parameters_are_set'].values[0]:
            core = node_row['core'].values[0]
            produced_data = await core.process()
            self._distribute_data_to_outputs(node_id, produced_data)

    def _distribute_data_to_outputs(self, node_id, produced_data):
        """Distributes produced data to output ports and sets events for input ports of subscribers."""
        output_ports = self.output_ports_register[self.output_ports_register['node_id'] == node_id]
        for _, output_row in output_ports.iterrows():
            output_port_id = output_row['port_id']
            subscribers = self.input_ports_register[self.input_ports_register['source_port_id'] == output_port_id]
            for subscriber_index in subscribers.index:
                self.input_ports_register.at[subscriber_index, 'data_processed'] = True
                self.data_ready_events[self.input_ports_register.at[subscriber_index, 'port_id']].set()

    def connect_ports(self, output_port_id, input_port_id):
        """Connects output and input ports, updating the input's source reference and setting up the event."""
        self.input_ports_register.loc[
            self.input_ports_register['port_id'] == input_port_id, 'source_port_id'
        ] = output_port_id

    def print_input_ports_register(self):
        """Affiche le registre des ports d'entrée."""
        print("Input Ports Register:")
        print(tabulate(self.input_ports_register, headers="keys", tablefmt="pretty"))

    def print_output_ports_register(self):
        """Affiche le registre des ports de sortie."""
        print("Output Ports Register:")
        print(tabulate(self.output_ports_register, headers="keys", tablefmt="pretty"))

    def print_node_register(self):
        """Affiche le registre des nœuds."""
        print("Node Register:")
        print(tabulate(self.nodes_register, headers="keys", tablefmt="pretty"))

    def print_parameters_register(self):
        """Affiche le registre des paramètres."""
        print("Parameters Register:")
        print(tabulate(self.parameters_register, headers="keys", tablefmt="pretty"))

    def print_all_registers(self):
        """Affiche tous les registres du NodePool pour un aperçu complet."""
        self.print_node_register()
        print("\n")
        self.print_parameters_register()
        print("\n")
        self.print_input_ports_register()
        print("\n")
        self.print_output_ports_register()
        print("\n")


