import socket

import ray
import traceback

from abc import ABC, abstractmethod
from loguru import logger
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class PathItem:
    def is_empty(self):
        return all(getattr(self, path_name) is None for path_name in self.__dict__.keys())

    def get_all(self):
        return [getattr(self, path_name) for path_name in self.__dict__.keys()]


class PathQueue(ABC):
    def __init__(self):
        self._num_tasks_executed = 0

    def get_num_tasks_executed(self):
        return self._num_tasks_executed

    @abstractmethod
    def item_schema(self) -> PathItem:
        pass

    @abstractmethod
    def output_path(self, gt_path, extension):
        pass

    @abstractmethod
    def get_path_item(self, extension) -> PathItem:
        pass


class RayWorker:
    def __init__(self, host_id, path_queue, catch_general_exception=False):
        self.path_queue = path_queue
        self.path_item_ref = None
        self.catch_general_exception = catch_general_exception
        self.storage_manager = ThreadedStorageManagerFactory.create(host_id)

    @property
    def worker_address(self):
        return socket.gethostbyname(socket.gethostname())

    @property
    def reader(self):
        return self.storage_manager.reader

    @property
    def writer(self):
        return self.storage_manager.writer

    @staticmethod
    def exception_log(path, error_msg):
        return [path, error_msg]

    @property
    def ext(self):
        raise NotImplementedError

    def run_task(self, path_item: PathItem):
        raise NotImplementedError

    def run(self):
        exception_logs = []
        num_worker_executed_tasks = 0
        self.path_item_ref = self.path_queue.get_path_item.remote(self.ext)

        while True:
            path_item = ray.get(self.path_item_ref)
            if path_item.is_empty():
                break

            try:
                self.run_task(path_item)

                num_total_tasks_executed = ray.get(self.path_queue.get_num_tasks_executed.remote())
                self.path_item_ref = self.path_queue.get_path_item.remote(self.ext)

            except Exception as e:
                error_msg = handle_exception(path_item.gt_path, e, traceback.format_exc(), self.catch_general_exception)
                exception_logs.append(self.exception_log(path_item.gt_path, error_msg))
                continue

            num_worker_executed_tasks += 1
            if num_total_tasks_executed % 100 == 0:
                ray.logger.info(f"{num_total_tasks_executed} tasks executed.")

        return self.worker_address, num_worker_executed_tasks, exception_logs


class RayProcess:
    def __init__(self):
        self._init_ray()

    @staticmethod
    def _init_ray():
        ray.init(ignore_reinit_error=True, runtime_env={'excludes': ['/Users/eden/gtaas-processing/.git/*']})
        print('''This cluster consists of
            {} nodes in total
            {} CPU resources in total
            {} CPU resources are available
        '''.format(len(ray.nodes()), ray.cluster_resources().get('CPU'), ray.available_resources().get('CPU')))

    @staticmethod
    def kill_actors(path_queue, workers):
        logger.info('UnhandledExceptionError occurred, killing actors...')
        [ray.kill(worker) for worker in workers]
        ray.kill(path_queue)

    def path_queue(self):
        raise NotImplementedError

    def worker(self, path_queue):
        raise NotImplementedError

    def execute(self, ray_worker_num=None):
        if not ray_worker_num:
            ray_worker_num = int((ray.available_resources().get('CPU') - 1))

        logger.info('Loading path queue...')
        path_queue = self.path_queue()

        logger.info('Creating ray_cluster workers...')
        workers = [self.worker(path_queue) for _ in range(ray_worker_num)]
        task_refs = [worker.run.remote() for worker in workers]

        exception_logs = []
        worker_executed_tasks = defaultdict(int)

        logger.info('Executing ray_cluster workers...')
        for _ in range(3):
            try:
                while len(task_refs) > 0:
                    ready_ref, task_refs = ray.wait(task_refs)
                    worker_address, num_worker_executed_tasks, exception_log = ray.get(ready_ref)[0]
                    logger.info(worker_address, num_worker_executed_tasks, exception_log)
                    worker_executed_tasks[worker_address] += num_worker_executed_tasks
                    exception_logs.extend(exception_log)
                break

            except (UnhandledException, KeyboardInterrupt):
                return self.kill_actors(path_queue, workers)

            except ray.exceptions.RayActorError as e:
                logger.error(f'RayActorError: {e}')

        print(f'Total {sum(worker_executed_tasks.values())} tasks executed, {len(exception_logs)} exceptions occurred.')
        for worker_address, num_worker_executed_tasks in worker_executed_tasks.items():
            print(f'{worker_address} executed {num_worker_executed_tasks} tasks.')

        return exception_logs
