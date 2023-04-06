import os
import socket
import threading
from json import JSONDecodeError
from pathlib import Path
from xml.etree import ElementTree

import ray
import logging

from loguru import logger
from funcy import retry, chunks
from collections import Counter

from paramiko.ssh_exception import SSHException

from utils.path import join_path
from utils.storage import open_storage_manager, StoragePath, StorageManagerFactory

local = threading.local()


@retry(5, timeout=1)
def generate_storage_manager(host_id):
    try:
        local.storage_manager
    except AttributeError:
        local.storage_manager = StorageManagerFactory.create(host_id)
    return local.storage_manager


def save_converted_gt(writer, converted_gt, output_path_stem, ext):
    output_path = f'{output_path_stem}.{ext}'

    if ext == 'json':
        writer.write_json(path=output_path, value=converted_gt)
    elif ext == 'txt':
        writer.write(output_path, converted_gt)
    elif ext == 'xml':
        from xml.dom import minidom
        xml_tree_string = minidom.parseString(
            ElementTree.tostring(converted_gt.getroot(), encoding='unicode')).toprettyxml(indent="\t")
        writer.write(data=xml_tree_string, path=output_path)
    else:
        raise Exception('Wrong output file extension.')


converter = NotImplementedError


@ray.remote(scheduling_strategy="SPREAD")
def run(file_path, output_path_stem, host_id):
    storage_manager = generate_storage_manager(host_id)

    try:
        gt = storage_manager.reader.read_json(file_path)
    except JSONDecodeError:
        raise Exception(f'Failed to read json file - {file_path}')
    except SSHException:
        raise Exception(f'Connection error - {file_path}')

    converted_gt = converter.convert(gt)
    save_converted_gt(storage_manager.writer, converted_gt, output_path_stem, converter.ext)
    return socket.gethostbyname(socket.gethostname())


MAX_PENDING_TASKS_NUM = 100
MAX_WORKER_RETRY = 5


class RayTaskConvertProcess:
    def __init__(self, input_root, output_root):
        self._init_ray()
        self.input_root = StoragePath.create(input_root)
        self.output_root = StoragePath.create(output_root)
        self.host_id = self.input_root.host_id

    @staticmethod
    def _init_ray():
        ray.init()
        print('''This cluster consists of
            {} nodes in total
            {} CPU resources in total
            {} CPU resources are available
        '''.format(len(ray.nodes()), ray.cluster_resources().get('CPU'), ray.available_resources().get('CPU')))

    def generate_output_path_stem(self, file_path):
        parent_path = Path(file_path.replace(str(self.input_root.abs_path), str(self.output_root.abs_path))).parent
        filename = Path(file_path).stem
        return join_path(parent_path, f"{filename}")

    def run(self, file_path, output_path):
        return run.remote(file_path, output_path, self.host_id)

    def _run_task(self, file_paths, completed_object_ids):
        ray_task_ids = []

        for file_path in file_paths:
            while len(ray_task_ids) > MAX_PENDING_TASKS_NUM:
                ready_ref, ray_task_ids = ray.wait(ray_task_ids)  # Return 1 ready object, not ready object lists
                completed_object_ids.extend(ray.get(ready_ref))  # get : block until a task finishes the execution

            output_path = self.generate_output_path_stem(file_path)
            ray_task_ids.append(self.run(file_path, output_path))

        return ray_task_ids

    def _run_tasks(self, file_paths):
        completed_object_ids = []

        try:
            result_refs = self._run_task(file_paths, completed_object_ids)
            completed_object_ids.extend(ray.get(result_refs))

        except ray.exceptions.RayActorError:
            return False, []

        return True, completed_object_ids

    def execute(self, path_chunks: int = 10):
        with open_storage_manager(self.input_root.host_id) as storage_manager:
            gt_paths = storage_manager.walker.walk(path=self.input_root.abs_path, patterns=["*.json"])
            completed_work_ids = []

            for paths in chunks(path_chunks, gt_paths):
                for retry_count in range(MAX_WORKER_RETRY):
                    success, results = self._run_tasks(paths)

                    if success:
                        completed_work_ids.extend(results)
                        logger.info(f'processed_count : {len(completed_work_ids)}')
                        break

                    else:
                        if retry_count == MAX_WORKER_RETRY - 1:
                            raise Exception('Ray job Failed')

            print('Tasks executed')
            for ip_address, num_tasks in Counter(completed_work_ids).items():
                print('    {} tasks on {}'.format(num_tasks, ip_address))
