import os.path
from typing import Dict
import pprint
import yaml


# this information is different per workflow
class WorkflowInfo:
    def __init__(self, workflow_name, templates_infos, raw_data,
                 block_successors, block_predecessors, global_inputs):
        self.workflow_name = workflow_name
        self.templates_infos = templates_infos
        self.data = raw_data
        self.block_successors = block_successors      # edges: who comes after
        self.block_predecessors = block_predecessors  # edges: who must finish before
        self.global_inputs = global_inputs            # global input routing

    @classmethod
    def parse(cls, config_dict):
        workflows_info = {}

        for path in config_dict.values():
            config_file = os.path.join(path, 'workflow_info.yaml')
            with open(config_file, 'r') as f:
                data = yaml.safe_load(f)

            workflow_name = data['workflow_name']
            templates_infos = {}

            block_successors = {}
            block_predecessors = {}

            # Copy template info
            for template_name, template_infos in data['templates'].items():
                templates_infos[template_name] = template_infos
                for block_name, block_infos in template_infos['blocks'].items():
                    key = (template_name, block_name)
                    block_successors[key] = {}
                    block_predecessors[key] = set()

            # 🔧 ADD: Process global_inputs as special template
            global_inputs = data.get('global_inputs', {})
            
            # Add global_inputs as a pseudo-template with successors
            global_inputs_key = ('global_inputs', 'global_inputs')
            block_successors[global_inputs_key] = {}
            block_predecessors[global_inputs_key] = set()
            
            # Build successors from global_inputs to first templates
            for input_name, input_info in global_inputs.items():
                if 'dest' in input_info:
                    successors_for_input = []
                    for dest_template, dest_blocks in input_info['dest'].items():
                        if dest_template != '$USER':
                            for dest_block, dest_inputs in dest_blocks.items():
                                for dest_input in dest_inputs:
                                    successors_for_input.append((dest_template, dest_block, dest_input))
                                    # Register predecessor
                                    block_predecessors[(dest_template, dest_block)].add(global_inputs_key)
                    
                    block_successors[global_inputs_key][input_name] = successors_for_input

            # Build graph edges from template outputs → destinations
            for template_name, template_infos in data['templates'].items():
                for block_name, block_infos in template_infos['blocks'].items():
                    src_key = (template_name, block_name)
                    
                    if 'output_datas' not in block_infos:
                        continue
                        
                    for output_name, output_infos in block_infos['output_datas'].items():
                        if "dest" not in output_infos:
                            continue
                        
                        successors_for_output = []
                        for dest_template, dest_blocks in output_infos["dest"].items():
                            if dest_template == "$USER":  # terminal sink
                                continue
                            for dest_block, dest_inputs in dest_blocks.items():
                                for dest_input in dest_inputs:
                                    successors_for_output.append((dest_template, dest_block, dest_input))
                                    # Register predecessor
                                    block_predecessors[(dest_template, dest_block)].add(src_key)
                        
                        block_successors[src_key][output_name] = successors_for_output

            # 🔍 DEBUG: Print parsed workflow info
            print(f"\n=== Workflow parsed: {workflow_name} ===")
            print("Block Successors:")
            for key, successors in block_successors.items():
                if successors:  # Only print non-empty
                    print(f"  {key}: {successors}")
            print("========================================\n")
            
            workflows_info[workflow_name] = cls(
                workflow_name, templates_infos, data,
                block_successors, block_predecessors, global_inputs
            )

        return workflows_info



# def parse(config_dict: dict):
#     workflows_info = {}
#     for path in config_dict.values():
#         config_file = os.path.join(path, 'workflow_info.yaml')
#         with open(config_file, 'r') as f:
#             data = yaml.safe_load(f)
#         print(data)
#         workflow_name = data['workflow_name']
#         # datas_successors = {}
#         # functions_predecessors = {}
#         templates_infos = {}
#         # user_input_src_infos = {}
#         templates_blocks_predecessor_cnt = {}
#         for template_name, template_infos in data['templates'].items():
#             templates_infos[template_name] = template_infos
#             templates_blocks_predecessor_cnt[template_name] = {}
#             for block_name, block_infos in template_infos['blocks'].items():
#                 templates_blocks_predecessor_cnt[template_name][block_name] = 0
#         for template_name, template_infos in data['templates'].items():
#             for block_name, block_infos in template_infos['blocks'].items():
#                 for output_name, output_infos in block_infos['output_datas'].items():
#                     if output_infos['type'] == 'NORMAL':
#                         for dest_template_name, dest_template_infos in output_infos['dest']:
#                             if dest_template_name == '$USER':
#                                 pass
#                             else:
#                                 for dest_block_name, dest_block_infos in dest_template_infos:
#                                     for dest_input_name in dest_block_infos:
#                                         dest_input_type = \
#                                             data['templates'][dest_template_name]['blocks'][dest_block_name][
#                                                 'input_datas'][dest_input_name]['type']
#                                         templates_blocks_predecessor_cnt[dest_template_name][dest_block_name] += 1
#                                         if dest_input_type == 'NORMAL':
#                                             pass
#                                         elif dest_input_type == 'LIST':
#                                             pass
#                                         else:
#                                             raise Exception('undefined input type: ', dest_input_type)
#                     else:
#                         raise Exception('undefined output type: ', output_infos['type'])
#
#         # function_name = template['name']
#         # functions_infos[function_name] = func
#         # functions_predecessors[function_name] = []
#         # input_datas: dict = func['input_datas']
#         # for parameter, info in input_datas.items():
#         #     src = []
#         #     if info['type'] == 'NORMAL':
#         #         src.append(info['src'])
#         #     elif info['type'] == 'LIST':
#         #         src = info['src']
#         #     for input_data in src:
#         #         if '$USER' in input_data:
#         #             if input_data not in user_input_src_infos:
#         #                 user_input_src_infos[input_data] = []
#         #             user_input_src_infos[input_data].append(function_name)
#         #         if input_data not in datas_successors:
#         #             datas_successors[input_data] = []
#         #         datas_successors[input_data].append(function_name)
#         #         functions_predecessors[function_name].append(input_data)
#         workflows_info[workflow_name] = WorkflowInfo(workflow_name, templates_infos, templates_blocks_predecessor_cnt,
#                                                      data)
#     return workflows_info

# a = WorkflowInfo.parse({'': '../../benchmark/file_processing'})
# print(a)
