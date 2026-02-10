from geopy.distance import geodesic
from shapely.geometry import box

class BBoxUtils:
    """
    Utility class for working with bounding boxes, including size calculations,
    dynamic overlap determination, and box division.
    """
    @staticmethod
    def get_longest_side(bbox):
        """
        Computes the longest side of a bounding box in meters.

        Args:
            bbox (shapely.geometry.Polygon): A bounding box geometry.

        Returns:
            float: The length of the longest side in meters.
        """
        min_lng, min_lat, max_lng, max_lat = bbox.bounds
        min_lng, min_lat, max_lng, max_lat = bbox.bounds

        # Calculate width and height using geodesic distance
        width = geodesic((min_lat, min_lng), (min_lat, max_lng)).meters
        height = geodesic((min_lat, min_lng), (max_lat, min_lng)).meters

        return max(width, height)

    @staticmethod
    def calculate_dynamic_overlap(longest_side):
        """
        Determines the dynamic overlap factor based on the size of the bounding box.

        Args:
            longest_side (float): The longest side of the bounding box in meters.

        Returns:
            float: The overlap factor to be applied when dividing the box.
        """
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
    def divide_box(parent_box):
        """
        Splits a bounding box into four smaller boxes with a slight overlap.

        Args:
            parent_box (shapely.geometry.Polygon): The bounding box to be divided.

        Returns:
            list[shapely.geometry.Polygon]: A list of four divided bounding boxes.
        """
        min_lng, min_lat, max_lng, max_lat = parent_box.bounds

        # Compute the midpoint for division
        mid_lng = (min_lng + max_lng) / 2
        mid_lat = (min_lat + max_lat) / 2

        # Determine the longest side and the corresponding overlap factor
        longest_side = BBoxUtils.get_longest_side(parent_box)
        overlap = BBoxUtils.calculate_dynamic_overlap(longest_side)
        
        # Create four sub-boxes with slight overlap to ensure coverage
        divided_boxes = [
            box(min_lng, min_lat, mid_lng + overlap, mid_lat + overlap),
            box(mid_lng - overlap, min_lat, max_lng, mid_lat + overlap),
            box(min_lng, mid_lat - overlap, mid_lng + overlap, max_lat),
            box(mid_lng - overlap, mid_lat - overlap, max_lng, max_lat)
        ]
        return divided_boxes

    @staticmethod
    def normalize_coords(coords, precision):
        """
        Rounds coordinate values to the specified precision.

        Args:
            coords (tuple[float, float]): A tuple representing latitude and longitude.
            precision (int): The number of decimal places to round to.

        Returns:
            tuple[float, float]: The rounded coordinates.
        """
        return tuple(round(c, precision) for c in coords)