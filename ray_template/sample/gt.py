import ray

from ray_template.gt import GtConvertRayWorker, GtConvertRayProcess


class TestGtConverter:
    def convert(self, gt):
        print('Process Gt')
        return gt


@ray.remote(num_cpus=1)
class RayTestGtConverter(GtConvertRayWorker):
    @property
    def converter(self):
        return TestGtConverter()


class RayTestGtConvertProcess(GtConvertRayProcess):
    def worker(self, queue):
        return RayTestGtConverter.remote(self.host_id, queue)
