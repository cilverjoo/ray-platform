import ray

import GtJsonConverter
from ray_on_actor import RayActorConvertProcess, RayConverter


class SampleGtConverter(GtJsonConverter):
    def convert(self, gt):
        return gt


@ray.remote(num_cpus=1)
class RayActorConverter(RayConverter):
    def __init__(self, host_id, queue):
        super().__init__(host_id, queue)
        self.converter = SampleGtConverter()


class RayActorProcess(RayActorConvertProcess):
    def worker(self, queue):
        return RayActorConverter.remote(self.host_id, queue)
