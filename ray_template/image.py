import io
from dataclasses import dataclass

import ray

from pathlib import Path, PosixPath

from ray_template import PathItem, PathQueue, RayWorker, RayProcess


@dataclass
class GtImagePathItem(PathItem):
    gt_path: str = None
    image_path: str = None
    output_path: str = None


class GtImagePathQueue(PathQueue):
    def __init__(self, gt_paths, gt_root: PosixPath, image_root: PosixPath, output_root: PosixPath):
        super().__init__()
        self.gt_paths = gt_paths
        self.gt_root = gt_root
        self.image_root = image_root
        self.output_root = output_root

    @staticmethod
    def item_schema(gt_path=None, image_path=None, output_path=None):
        return GtImagePathItem(gt_path, image_path, output_path)

    def image_path(self, gt_path):
        parent_path = Path(gt_path).relative_to(self.gt_root).parent
        file_stem = Path(gt_path).stem
        return f"{self.image_root}/{parent_path}/{file_stem}"

    def output_path(self, gt_path, extension):
        parent_path = Path(gt_path).relative_to(self.gt_root).parent
        file_stem = Path(gt_path).stem
        return f"{self.output_root}/{parent_path}/{file_stem}.{extension}"

    def get_path_item(self, extension):
        if self.gt_paths:
            gt_path = self.gt_paths.pop(0)
            self._num_tasks_executed += 1
            return self.item_schema(gt_path, self.image_path(gt_path), self.output_path(gt_path, extension))
        return self.item_schema()


class GtImageConvertRayWorker(RayWorker):
    @property
    def image_validator(self):
        raise NotImplementedError

    @property
    def image_converter(self):
        raise NotImplementedError

    @property
    def ext(self):
        return 'png'

    def read_gt(self, gt_path):
        return self.reader.read_json(gt_path)

    def read_image(self, image_path):
        image_buffer = io.BytesIO(self.reader.open_file(image_path, 'rb').read())
        image_validation = self.image_validator.load_valid_image(image_buffer)
        if not image_validation.is_valid:
            raise ImageValidationError(image_validation.message)
        return image_validation.valid_image

    def write_image(self, image, output_path):
        self.writer.write_image(image, output_path, format='PNG')

    def run_task(self, path_item):
        gt_path, image_path, output_path = path_item.get_all()

        gt = self.read_gt(gt_path)
        image = self.read_image(image_path)

        converted_image = self.image_converter.convert(gt, image)
        self.write_image(converted_image, output_path)


class GtImageConvertRayProcess(RayProcess):
    def __init__(self, gt_root, image_root, output_root):
        super().__init__()
        self.gt_root = gt_root
        self.image_root = image_root
        self.output_root = output_root
        self.host_id = self.gt_root.host_id

    def worker(self, path_queue) -> GtImageConvertRayWorker:
        return GtImageConvertRayWorker.remote(self.host_id, path_queue)

    def path_queue(self):
        with ThreadedStorageManagerFactory.create(self.host_id) as storage_manager:
            gt_paths = list(storage_manager.walker.walk(path=self.gt_root.abs_path, patterns=['*.json']))
            queue_ref = ray.remote(GtImagePathQueue)
            return queue_ref.remote(gt_paths, self.gt_root, self.image_root, self.output_root)
