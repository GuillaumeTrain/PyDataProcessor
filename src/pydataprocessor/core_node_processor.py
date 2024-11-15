from abc import ABC, abstractmethod
import pandas as pd
import asyncio
from uuid import uuid4


class CoreProcessorBase(ABC):
    def __init__(self, core_type, node_id, nodepool, datapool):
        self.core_type = core_type
        self.node_id = node_id
        self.nodepool = nodepool
        self.datapool = datapool
        self.input_ports = []
        self.output_ports = []
        self.parameters = {}

    def set_parameters(self, params):
        """Sets the necessary parameters for this processing type."""
        self.parameters.update(params)

    def get_input_port_data(self, port_id, data_type):
        """Adds an input port with data type checking."""
        self.input_ports.append((port_id, data_type))

    def add_output_port(self, port_id, data_type):
        """Adds an output port and pre-declares its data in the datapool."""
        self.output_ports.append((port_id, data_type))
        data_id = str(uuid4())
        self.datapool.register_data(data_type, port_id, self.node_id, in_file=False)

    @abstractmethod
    async def process(self):
        """Define this method in each specific CoreProcessor."""
        pass

    def estimate_output_size(self):
        """Estimate output size based on input size and processing type."""
        input_data_sizes = [self.datapool.get_data_info(port[0])['data_object'].data_size_in_bytes
                            for port in self.input_ports]
        estimated_size = sum(input_data_sizes)  # Simple example, will vary by processing type
        return estimated_size


class CoreProcessorFactory:
    @staticmethod
    def create_processor(core_type, node_id, node_pool, datapool):
        processors = {
            "ProducerCore": ProducerCore,
            "TransmitterCore": TransmitterCore,
            "ConsumerCore": ConsumerCore
        }

        if core_type not in processors:
            raise ValueError(f"Unsupported core type: {core_type}")

        # Utilisation correcte sans passer `core_type`
        return processors[core_type](node_id, node_pool, datapool)


class AdditionCoreProcessor(CoreProcessorBase):
    async def process(self):
        print("AdditionCoreProcessor: Processing data for addition.")
        input_data_1 = await self.nodepool.fetch_data_from_input(self.input_ports[0][0])
        input_data_2 = await self.nodepool.fetch_data_from_input(self.input_ports[1][0])

        if isinstance(input_data_1, list) and isinstance(input_data_2, list):
            # Traitement direct si les données sont en RAM
            result = [a + b for a, b in zip(input_data_1, input_data_2)]
        else:
            # Traitement par chunk si les données sont stockées en fichier
            result = []
            chunk_size = 1024  # Taille de chunk, paramètre ajustable
            async for chunk_1, chunk_2 in zip(input_data_1, input_data_2):
                chunk_result = [a + b for a, b in zip(chunk_1, chunk_2)]
                result.extend(chunk_result)

        output_port_id = self.output_ports[0][0]
        self.datapool.store_data(output_port_id, result, self.node_id)
        print(f"AdditionCoreProcessor: Stored data at output port {output_port_id}.")


class CoreProcessor(ABC):
    def __init__(self, node_id, node_pool, datapool):
        self.node_id = node_id
        self.node_pool = node_pool
        self.datapool = datapool

    @abstractmethod
    async def process(self, input_data=None):
        """
        Cette méthode sera implémentée par chaque type de Core spécifique.
        - ProducerCore : Génère et enregistre les données.
        - TransmitterCore : Transmet des données d'entrée à une sortie.
        - ConsumerCore : Consomme les données d'entrée.
        """
        pass


class ProducerCore(CoreProcessor):
    async def process(self):
        print("ProducerCore: Producing data...")
        return [1, 2, 3, 4, 5]


class TransmitterCore(CoreProcessor):
    async def process(self, input_data):
        print("TransmitterCore: Transmitting data...")
        return input_data


class ConsumerCore(CoreProcessor):
    async def process(self, input_data):
        print(f"ConsumerCore: Consuming data: {input_data}")

