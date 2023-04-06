import socket

import ray

import GtJsonConverter
from ray_on_task import RayTaskConvertProcess, generate_storage_manager, save_converted_gt


class SampleGtConverter(GtJsonConverter):
    def convert(self, gt):
        return gt


converter = SampleGtConverter()


@ray.remote(scheduling_strategy="SPREAD")
def run(file_path, output_path_stem, host_id):
    """
    run 메서드에 파일을 읽고 변환해서 저장하는 코드가 포함되어야 합니다.
    파일 단위로 요구되는 작업에 소요되는 리소스가 어느 정도인지 파악하기 어려울 때 task로 실행을 권장합니다.
    """
    storage_manager = generate_storage_manager(host_id)
    gt = storage_manager.reader.read_json(file_path)
    converted_gt = converter.convert(gt)
    save_converted_gt(storage_manager.writer, converted_gt, output_path_stem, converter.ext)
    return socket.gethostbyname(socket.gethostname())


class RayTaskTestProcess(RayTaskConvertProcess):
    def run(self, file_path, output_path):
        return run.remote(file_path, output_path, self.host_id)
