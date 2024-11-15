import asyncio
from PyDataCore import DataPool
from pydataprocessor.core_node_pool import NodePool
from pydataprocessor.core_node_processor import ProducerCore, TransmitterCore, ConsumerCore, CoreProcessorFactory


async def example_usage():
    # Initialisation du DataPool et du NodePool
    datapool = DataPool()
    nodepool = NodePool(datapool)
    # Ajout des nœuds avec leurs cores respectifs
    producer_id = nodepool.declare_node("ProducerCore Processor", "Producer", core="ProducerCore")
    transmitter_id = nodepool.declare_node("TransmitterCore Processor", "Transmitter",core="TransmitterCore")
    consumer_id = nodepool.declare_node("ConsumerCore Processor", "Consumer", core="ConsumerCore")

    # Ajout des paramètres pour le producteur
    nodepool.add_parameter(producer_id, "param1", "sampling_rate", "float", 0.1)

    # Ajout des ports pour chaque nœud
    producer_output_port = nodepool.add_output_port(producer_id, port_number=1,port_name="producer output", data_type="int")
    transmitter_input_port = nodepool.add_input_port(transmitter_id, port_number=1,port_name="transmiter input", data_type="int", source_port_id=producer_output_port)
    transmitter_output_port = nodepool.add_output_port(transmitter_id, port_number=1,port_name="transmiter output", data_type="int")
    consumer_input_port = nodepool.add_input_port(consumer_id, port_number=1,port_name="consumer input", data_type="int", source_port_id=transmitter_output_port)

    # Connexion des ports
    nodepool.connect_ports(producer_output_port, transmitter_input_port)
    nodepool.connect_ports(transmitter_output_port, consumer_input_port)

    # Exécution de la chaîne de traitement
    await nodepool.start_processing_chain()

    # Affichage des registres pour visualiser l'état
    nodepool.print_all_registers()

asyncio.run(example_usage())
