import ray

from typing import List
from dataclasses import dataclass
from xml.etree import ElementTree
from pathlib import Path, PosixPath

from ray_template import RayProcess, RayWorker, PathItem, PathQueue


@dataclass
class GtPathItem(PathItem):
    gt_path: str = None
    output_path: str = None


class GtPathQueue(PathQueue):
    def __init__(self, gt_paths: List, gt_root: PosixPath, output_root: PosixPath):
        super().__init__()
        self.gt_paths = gt_paths
        self.gt_root = gt_root
        self.output_root = output_root

    @staticmethod
    def item_schema(gt_path=None, output_path=None):
        return GtPathItem(gt_path, output_path)

    def output_path(self, gt_path, save_extension):
        parent_path = Path(gt_path).relative_to(self.gt_root).parent
        file_stem = Path(gt_path).stem
        return join_path(self.output_root, parent_path, f'{file_stem}.{save_extension}')

    def get_path_item(self, save_extension):
        if self.gt_paths:
            gt_path = self.gt_paths.pop(0)
            self._num_tasks_executed += 1
            return self.item_schema(gt_path=gt_path, output_path=self.output_path(gt_path, save_extension))
        return self.item_schema()


class GtConvertRayWorker(RayWorker):
    @property
    def converter(self):
        raise NotImplementedError

    @property
    def ext(self):
        return self.converter.ext

    def read_gt(self, gt_path):
        return self.reader.read_json(gt_path)

    def write_gt(self, converted_gt, output_path):
        if self.ext == 'json':
            self.storage_manager.writer.write_json(output_path, converted_gt)
        elif self.ext == 'txt':
            self.storage_manager.writer.write(output_path, converted_gt)
        elif self.ext == 'csv':
            self.storage_manager.writer.write_csv(path=output_path, rows=converted_gt, header=self.converter.header)
        elif self.ext == 'xml':
            from xml.dom import minidom
            xml_tree_string = minidom.parseString(
                ElementTree.tostring(converted_gt.getroot(), encoding='unicode')).toprettyxml(indent="\t")
            self.storage_manager.writer.write(output_path, xml_tree_string)
        else:
            raise ValueError('Unsupported extension')

    def gt_convert_condition(self, gt):
        # example: return len(gt['annotations'] > 0
        return True

    def run_task(self, path_item: PathItem):
        gt_path, output_path = path_item.get_all()

        gt = self.read_gt(gt_path)
        if not self.gt_convert_condition(gt):
            return

        converted_gt = self.converter.convert(gt)
        self.write_gt(converted_gt=converted_gt, output_path=output_path)


class GtConvertRayProcess(RayProcess):
    def __init__(self, host_id, gt_root: str, output_root: str):
        super().__init__()
        self.gt_root = gt_root
        self.output_root = output_root
        self.host_id = host_id

    def path_queue(self):
        with ThreadedStorageManagerFactory.create(self.host_id) as storage_manager:
            gt_paths = list(storage_manager.walker.walk(path=self.gt_root.abs_path, patterns=['*.json']))
            queue_ref = ray.remote(GtPathQueue)
            return queue_ref.remote(gt_paths, self.gt_root, self.output_root)

    def worker(self, path_queue):
        return GtConvertRayWorker.remote(self.host_id, path_queue)
