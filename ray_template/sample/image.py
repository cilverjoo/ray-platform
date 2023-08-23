import cv2
import numpy as np
import ray
from PIL import Image

from ray_template.image import GtImageConvertRayWorker, GtImageConvertRayProcess


def get_xywh(points):
    x_coordinates, y_coordinates = zip(*points)

    return [min(x_coordinates),
            min(y_coordinates),
            max(x_coordinates) - min(x_coordinates),
            max(y_coordinates) - min(y_coordinates)]


class TestImageConverter:
    def convert(self, gt, image):
        xywh = [get_xywh(annotation["points"]) for annotation in gt["annotations"]]
        np_image = np.array(image, dtype=np.uint8)

        for x, y, width, height in xywh:
            region_of_interest = np_image[y:y + height, x:x + width]
            blurred_roi = cv2.blur(region_of_interest, ksize=(30, 30))
            np_image[y:y + height, x:x + width] = blurred_roi

        return Image.fromarray(np_image)


@ray.remote(num_cpus=1)
class TestGtImageConvertRayWorker(GtImageConvertRayWorker):
    @property
    def image_converter(self):
        return TestImageConverter()


class RayTestImageConvertProcess(GtImageConvertRayProcess):
    def worker(self, path_queue):
        return TestGtImageConvertRayWorker.remote(self.host_id, path_queue)
