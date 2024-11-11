import asyncio
import pandas as pd
from PyDataCore import DataPool
from qasync import QEventLoop
from PySide6.QtWidgets import QApplication
import sys

from tabulate import tabulate


class NodePool:
    def __init__(self, datapool: DataPool):
        self.datapool = datapool

        # Register to track each node, its type, processing status, and port states
        self.nodes_register = pd.DataFrame(columns=[
            'node_id', 'node_type', 'core', 'data_processed',
            'input_ports_ready', 'output_ports_acknowledged',
            'all_parameters_are_set'
        ])

        # Register to manage input ports with states and data linkage
        self.input_ports_register = pd.DataFrame(columns=[
            'node_id', 'port_number', 'port_id', 'data_type',
            'source_port_id', 'data_processed'
        ])

        # Register to manage output ports with availability and subscribers
        self.output_ports_register = pd.DataFrame(columns=[
            'node_id', 'port_number', 'port_id', 'data_type',
            'subscriber_port_id', 'data_available'
        ])

        # Register to track parameters for each node, their state, and values
        self.parameters_register = pd.DataFrame(columns=[
            'node_id', 'parameter_id', 'parameter_name',
            'parameter_type', 'parameter_value', 'parameter_is_set'
        ])

    def add_node(self, node_id, node_type, core):
        """Adds a node to the register and initializes its ports and parameters."""
        self.nodes_register = pd.concat([
            self.nodes_register,
            pd.DataFrame([{
                'node_id': node_id,
                'node_type': node_type,
                'core': core,
                'data_processed': False,
                'input_ports_ready': False,
                'output_ports_acknowledged': False,
                'all_parameters_are_set': False
            }])
        ], ignore_index=True)

    def add_input_port(self, node_id, port_number, data_type, source_port_id=None):
        """Adds an input port to the register and links to a data source if specified."""
        port_id = f"{node_id}_input_{port_number}"
        self.input_ports_register = pd.concat([
            self.input_ports_register,
            pd.DataFrame([{
                'node_id': node_id,
                'port_number': port_number,
                'port_id': port_id,
                'data_type': data_type,
                'source_port_id': source_port_id,
                'data_processed': False
            }])
        ], ignore_index=True)
        return port_id

    def add_output_port(self, node_id, port_number, data_type, subscriber_port_id=None):
        """Adds an output port to the register and optionally assigns a subscriber."""
        port_id = f"{node_id}_output_{port_number}"
        self.output_ports_register = pd.concat([
            self.output_ports_register,
            pd.DataFrame([{
                'node_id': node_id,
                'port_number': port_number,
                'port_id': port_id,
                'data_type': data_type,
                'subscriber_port_id': subscriber_port_id,
                'data_available': False
            }])
        ], ignore_index=True)
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

        # Filter empty columns to avoid FutureWarning
        self.parameters_register = pd.concat(
            [self.parameters_register, new_param.dropna(axis=1, how='all')],
            ignore_index=True
        )

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

    async def process_node(self, node_id):
        """Processes a node if all input data is ready, parameters are set, and acknowledges output data."""
        node_row = self.nodes_register[self.nodes_register['node_id'] == node_id]

        if node_row['input_ports_ready'].values[0] and node_row['all_parameters_are_set'].values[0]:
            core = node_row['core'].values[0]
            await core.process()

            # Update node status in register
            self.nodes_register.loc[self.nodes_register['node_id'] == node_id, 'data_processed'] = True
            self._acknowledge_output_ports(node_id)
            self._update_port_status(node_id)  # Ensure ports are marked as ready/acknowledged

    def _acknowledge_output_ports(self, node_id):
        """Marks all output ports of a node as acknowledged."""
        output_ports = self.output_ports_register[self.output_ports_register['node_id'] == node_id]
        for port_id in output_ports['port_id']:
            self.output_ports_register.loc[self.output_ports_register['port_id'] == port_id, 'data_available'] = True
        self.nodes_register.loc[self.nodes_register['node_id'] == node_id, 'output_ports_acknowledged'] = True

    def _update_port_status(self, node_id):
        """Marks input ports as ready and output ports as acknowledged after processing."""
        self.nodes_register.loc[self.nodes_register['node_id'] == node_id, 'input_ports_ready'] = True
        self.nodes_register.loc[self.nodes_register['node_id'] == node_id, 'output_ports_acknowledged'] = True

    def remove_node(self, node_id):
        """Removes a node and its associated ports and parameters from all registers."""
        self.nodes_register = self.nodes_register[self.nodes_register['node_id'] != node_id]
        self.input_ports_register = self.input_ports_register[self.input_ports_register['node_id'] != node_id]
        self.output_ports_register = self.output_ports_register[self.output_ports_register['node_id'] != node_id]
        self.parameters_register = self.parameters_register[self.parameters_register['node_id'] != node_id]

    def connect_ports(self, output_port_id, input_port_id):
        """
        Connects an output port to an input port. Updates the subscriber list of the output port and sets the
        source port ID for the input port.
        """
        # Update input port to link it to the output port
        self.input_ports_register.loc[
            self.input_ports_register['port_id'] == input_port_id, 'source_port_id'
        ] = output_port_id

        # Find if output port already has a subscriber
        output_row = self.output_ports_register[self.output_ports_register['port_id'] == output_port_id]

        if not output_row.empty and pd.notnull(output_row['subscriber_port_id']).all():
            # If output port already has subscribers, add a new row for the new subscriber
            new_row = output_row.iloc[0].copy()
            new_row['subscriber_port_id'] = input_port_id
            self.output_ports_register = pd.concat(
                [self.output_ports_register, pd.DataFrame([new_row])], ignore_index=True
            )
        else:
            # If output port has no subscribers, set the subscriber ID directly
            self.output_ports_register.loc[
                self.output_ports_register['port_id'] == output_port_id, 'subscriber_port_id'
            ] = input_port_id

    def print_input_ports_register(self):
        print(tabulate(self.input_ports_register, headers='keys', tablefmt='pretty'))

    def print_output_ports_register(self):
        print(tabulate(self.output_ports_register, headers='keys', tablefmt='pretty'))

    def print_node_register(self):
        print(tabulate(self.nodes_register, headers='keys', tablefmt='pretty'))

    def print_parameters_register(self):
        print(tabulate(self.parameters_register, headers='keys', tablefmt='pretty'))

    def print_all_registers(self):
        print("Input Ports Register:")
        self.print_input_ports_register()
        print("\nOutput Ports Register:")
        self.print_output_ports_register()
        print("\nNode Register:")
        self.print_node_register()
        print("\nParameters Register:")
        self.print_parameters_register()