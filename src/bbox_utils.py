from shapely.geometry import box
from geopy.distance import geodesic

class BBoxUtils:
    """
    A utility class for working with bounding boxes, including calculating 
    their longest side, determining dynamic overlaps, dividing boxes, and 
    normalizing coordinates.
    """

    @staticmethod
    def get_longest_side(shapely_box):
        """
        Calculate the longest side of a given bounding box using geographic distance.

        Args:
            shapely_box (shapely.geometry.Polygon): The bounding box whose sides are to be measured.

        Returns:
            float: The length of the longest side of the bounding box in meters.
        """
        # Extract the coordinates of the bounding box (min_lng, min_lat, max_lng, max_lat)
        min_lng, min_lat, max_lng, max_lat = shapely_box.bounds
        # Calculate width (horizontal distance) and height (vertical distance) using geodesic distance
        width = geodesic((min_lat, min_lng), (min_lat, max_lng)).meters
        height = geodesic((min_lat, min_lng), (max_lat, min_lng)).meters
        # Return the longest side
        return max(width, height)

    @staticmethod
    def calculate_dynamic_overlap(longest_side):
        """
        Calculate a dynamic overlap value based on the longest side of the bounding box.

        Args:
            longest_side (float): The length of the longest side of the bounding box.

        Returns:
            float: The overlap value that scales with the longest side of the box.
        """
        # Return different overlap values based on the length of the longest side
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
        """
        Divide a bounding box into four smaller boxes with a given overlap.

        Args:
            parent_box (shapely.geometry.Polygon): The parent bounding box to divide.
            overlap (float): The amount of overlap to apply when dividing the box.

        Returns:
            list: A list of four shapely box objects representing the divided sections.
        """
        # Extract the coordinates of the parent bounding box
        min_lng, min_lat, max_lng, max_lat = parent_box.bounds
        # Calculate the midpoints of the bounding box
        mid_lng = (min_lng + max_lng) / 2
        mid_lat = (min_lat + max_lat) / 2
        # Return four sub-boxes with specified overlap
        return [
            box(min_lng, min_lat, mid_lng + overlap, mid_lat + overlap),  # Top-left box
            box(mid_lng - overlap, min_lat, max_lng, mid_lat + overlap),  # Top-right box
            box(min_lng, mid_lat - overlap, mid_lng + overlap, max_lat),  # Bottom-left box
            box(mid_lng - overlap, mid_lat - overlap, max_lng, max_lat)   # Bottom-right box
        ]

    @staticmethod
    def normalize_coords(bounds, precision):
        """
        Normalize the coordinates of a bounding box to a specified decimal precision.

        Args:
            bounds (tuple): A tuple of the bounding box's coordinates (min_lng, min_lat, max_lng, max_lat).
            precision (int): The number of decimal places to round each coordinate.

        Returns:
            tuple: A tuple of the normalized coordinates rounded to the specified precision.
        """
        # Round each coordinate value to the specified precision and return the result
        return tuple(round(c, precision) for c in bounds)
