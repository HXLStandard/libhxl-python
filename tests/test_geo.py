"""
Unit tests for the hxl.geo module
David Megginson
February 2018

License: Public Domain
"""

import hxl.geo, unittest

class TestLatLon(unittest.TestCase):

    LATITUDE_SAMPLES = (
        ('45.5', 45.5),
        ('45N30', 45.5),
        ('N45:30:30', 45.508333,),
        ('N45°30\' 30"', 45.508333,),
        ('45°N30\' 30"', 45.508333,),
        ('S 45 30.5', -45.508333,),
    )

    LONGITUDE_SAMPLES = (
        ('-75.5', -75.5),
        ('75W30', -75.5),
        ('W75:30:30', -75.508333,),
        ('W75°30\' 30\"', -75.508333,),
        ('75°W30\' 30\"', -75.508333,),
        ('W 75 30.5', -75.508333,),
    )

    COORDINATE_SAMPLES = (
        ('45.5,-75.5', (45.5, -75.5),),
        ('45N30 / 75W30', (45.5, -75.5),),
        ('N45:30:30;W75:30:30', (45.508333, -75.508333,),),
        ('N45.5,W75.5', (45.5, -75.5)),
    )

    def test_parse_lat(self):
        for s, n in self.LATITUDE_SAMPLES:
            self.assertAlmostEqual(n, hxl.geo.parse_lat(s), places=6)

    def test_parse_lon(self):
        for s, n in self.LONGITUDE_SAMPLES:
            self.assertAlmostEqual(n, hxl.geo.parse_lon(s), places=6)

    def test_parse_coord(self):
        for s, c in self.COORDINATE_SAMPLES:
            coord = hxl.geo.parse_coord(s)
            self.assertAlmostEqual(c[0], coord[0], places=6)
            self.assertAlmostEqual(c[1], coord[1], places=6)

    def test_lat_out_of_range(self):
        with self.assertRaises(ValueError):
            hxl.geo.parse_lat('91 00 00')
        with self.assertRaises(ValueError):
            hxl.geo.parse_lat('-91 00 00')
        with self.assertRaises(ValueError):
            hxl.geo.parse_lat('45 60 00')
        with self.assertRaises(ValueError):
            hxl.geo.parse_lat('45 00 60')

    def test_lon_out_of_range(self):
        with self.assertRaises(ValueError):
            hxl.geo.parse_lon('181 00 00')
        with self.assertRaises(ValueError):
            hxl.geo.parse_lon('-181 00 00')
        with self.assertRaises(ValueError):
            hxl.geo.parse_lon('-75 60 00')
        with self.assertRaises(ValueError):
            hxl.geo.parse_lon('-75 00 60')

    def test_coord_out_of_range(self):
        with self.assertRaises(ValueError):
            hxl.geo.parse_coord('45.5,181.5')
