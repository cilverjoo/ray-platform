import os
import socket
import threading
from pathlib import Path

import ray
import logging

from funcy import retry
from collections import Counter

from tqdm import tqdm

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


def flatten(x):
    if isinstance(x, list):
        return [a for i in x for a in flatten(i)]
    else:
        return [x]


@ray.remote(num_cpus=1)
class FilePathQueue:
    def __init__(self, file_paths, input_root, output_root):
        self.file_paths = file_paths
        self.input_root = input_root
        self.output_root = output_root

    def get_work_path(self, extension):
        if self.file_paths:
            file_path = self.file_paths.pop(0)
            parent_path = str(Path(file_path).parent).replace(self.input_root, "")
            file_stem = Path(file_path).stem
            output_path = f'{self.output_root}{parent_path}/{file_stem}.{extension}'
            return file_path, output_path

        else:
            return None, None


class RayConverter:
    """
    RayConverter를 상속받아 Converter를 사용할 때 @ray.remote 데코레이터를 반드시 붙여야 한다.
    """
    def __init__(self, host_id, queue):
        self.storage_manager = generate_storage_manager(host_id)
        self.work_queue = queue
        self.work_path_ref = None
        self.reader = self.storage_manager.reader
        self.writer = self.storage_manager.writer
        self.converter = NotImplementedError

    @property
    def ext(self):
        return self.converter.ext

    def run(self):
        worker_ids = []
        self.work_path_ref = self.work_queue.get_work_path.remote(self.ext)

        while True:
            gt_path, output_path = ray.get(self.work_path_ref)

            if gt_path is None:
                return worker_ids

            gt = self.reader.read_json(gt_path)
            converted_gt = self.converter.convert(gt)
            self.writer.write_json(path=output_path, value=converted_gt)
            worker_ids.append(socket.gethostbyname(socket.gethostname()))
            self.work_path_ref = self.work_queue.get_work_path.remote(self.ext)


MAX_WORKER_RETRY = 5


class RayActorConvertProcess:
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

    def worker(self, queue):
        return RayConverter.remote(self.host_id, queue)

    def execute(self, ray_worker_num=None):
        """
        *중요* ray_worker_num값은 kubernetes 위의 ray-cluster 에서 코드를 돌릴 때만 지정
        """
        if not ray_worker_num:
            ray_worker_num = os.cpu_count() - 1

        actor_worked_node_address = []

        with open_storage_manager(self.host_id) as storage_manager:
            gt_paths = list(storage_manager.walker.walk(path=self.input_root.abs_path, patterns=["*.json"]))
            tqdm_bar = tqdm(total=len(gt_paths), postfix='\n')

            path_queue = FilePathQueue.remote(gt_paths, str(self.input_root.abs_path), str(self.output_root.abs_path))
            workers = [self.worker(path_queue) for _ in range(ray_worker_num)]
            futures = [worker.run.remote() for worker in workers]

            result_refs = futures

            try:
                while len(result_refs) > 0: # actor의 작업은 task처럼 file_path 별로 쪼갤 수 없기 때문에, 0을 기준으로 반복한다.
                    ready_ref, result_refs = ray.wait(result_refs)
                    completed_work_id = flatten(ray.get(ready_ref)) # list 안의 list 형태로 리턴 되어 flatten 추가
                    actor_worked_node_address.extend(completed_work_id)
                    tqdm_bar.update(len(completed_work_id))

            except ray.exceptions.RayActorError as e:
                raise Exception(f'Ray Actor Error raised : {e}')

            print('Tasks executed')
            for ip_address, num_tasks in Counter(actor_worked_node_address).items():
                print('    {} tasks on {}'.format(num_tasks, ip_address))
