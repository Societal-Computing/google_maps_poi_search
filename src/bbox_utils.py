from shapely.geometry import box
from geopy.distance import geodesic

class BBoxUtils:
    @staticmethod
    def get_longest_side(shapely_box):
        min_lng, min_lat, max_lng, max_lat = shapely_box.bounds
        width = geodesic((min_lat, min_lng), (min_lat, max_lng)).meters
        height = geodesic((min_lat, min_lng), (max_lat, min_lng)).meters
        return max(width, height)

    @staticmethod
    def calculate_dynamic_overlap(longest_side):
        if longest_side > 1000:
            return 0.0018
        if longest_side > 500:
            return 0.0009
        if longest_side > 200:
            return 0.0005
        if longest_side > 100:
            return 0.0003
        return 0.0001 if longest_side > 50 else 0.0

    @staticmethod
    def divide_box(parent_box, overlap):
        min_lng, min_lat, max_lng, max_lat = parent_box.bounds
        mid_lng = (min_lng + max_lng) / 2
        mid_lat = (min_lat + max_lat) / 2
        return [
            box(min_lng, min_lat, mid_lng + overlap, mid_lat + overlap),
            box(mid_lng - overlap, min_lat, max_lng, mid_lat + overlap),
            box(min_lng, mid_lat - overlap, mid_lng + overlap, max_lat),
            box(mid_lng - overlap, mid_lat - overlap, max_lng, max_lat)
        ]

    @staticmethod
    def normalize_coords(bounds, precision):
        return tuple(round(c, precision) for c in bounds)