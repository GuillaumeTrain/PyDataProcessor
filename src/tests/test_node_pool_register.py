import asyncio

from PyDataCore import DataPool

from pydataprocessor.core_node_processor import NodePool


class Core:
    def execute(self):
        print("CoreLogicProducer: Producing data...")
        return [1, 2, 3, 4, 5]  # Exemple de données générées


async def example_usage():
    # Initialisation du DataPool et du NodePool
    datapool = DataPool()
    nodepool = NodePool(datapool)

    # Ajout d’un nœud de production et d’un nœud de consommation
    nodepool.add_node('node1', 'Producer', core=Core())
    nodepool.print_all_registers()
    nodepool.add_node('node2', 'Consumer', core=Core())
    nodepool.print_all_registers()
    # Ajout des ports
    output_port_id = nodepool.add_output_port('node1', port_number=1, data_type='int')
    nodepool.print_all_registers()
    input_port_id = nodepool.add_input_port('node2', port_number=1, data_type='int')
    nodepool.print_all_registers()
    # Ajout d'un paramètre pour ce nœud
    nodepool.add_parameter('node1', 'param1', 'threshold', 'float', 0.5)
    nodepool.print_all_registers()
    # Connexion des ports
    nodepool.connect_ports(output_port_id, input_port_id)
    nodepool.print_all_registers()


    # Simulation de traitement du nœud
    await nodepool.process_node('node1')
    nodepool.print_all_registers()

asyncio.run(example_usage())
