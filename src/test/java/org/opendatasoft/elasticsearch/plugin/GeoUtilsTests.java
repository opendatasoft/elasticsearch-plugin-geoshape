package org.opendatasoft.elasticsearch.plugin;

import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.legacygeo.builders.CoordinatesBuilder;
import org.elasticsearch.legacygeo.builders.GeometryCollectionBuilder;
import org.elasticsearch.legacygeo.builders.LineStringBuilder;
import org.elasticsearch.legacygeo.builders.MultiPolygonBuilder;
import org.elasticsearch.legacygeo.builders.PolygonBuilder;
import org.elasticsearch.test.ESTestCase;
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;

/**
 * Test simple pour vérifier que la configuration fonctionne
 */
public class GeoUtilsTests extends ESTestCase {

    public void testBasicConfiguration() {
        assertTrue("Configuration OK", true);
        assertEquals("Dumb basic assertion", 1, 1);
    }

    public void testRandomizedTesting() {
        // Test des capacités de génération aléatoire d'ES
        double randomValue = randomDouble();
        assertTrue("Valeur aléatoire dans les limites", randomValue >= 0.0 && randomValue <= 1.0);

        int randomInt = randomIntBetween(1, 100);
        assertTrue("Entier aléatoire dans les limites", randomInt >= 1 && randomInt <= 100);
    }

    public void testLegacyRemoveConsecutiveDuplicateCoordinatesFromLine() {
        List<Coordinate> coordinates = new ArrayList<>();
        coordinates.add(new Coordinate(0.0, 0.0));
        coordinates.add(new Coordinate(1.0, 0.0));
        coordinates.add(new Coordinate(1.0, 0.0)); // duplicated
        coordinates.add(new Coordinate(1.0, 1.0));
        coordinates.add(new Coordinate(0.0, 1.0));

        LineStringBuilder original = new LineStringBuilder(coordinates);
        LineStringBuilder result = GeoUtils.removeDuplicateCoordinates(original);

        Geometry geom = result.buildGeometry();
        Line line = (Line) geom;
        assertEquals("Should find 4 coordinates instead of 5", 4, line.length());
    }

    public void testRemoveConsecutiveDuplicateCoordinatesFromLine() {
        // The third point is duplicated.
        double[] coordX = { 0.0, 1.0, 1.0, 1.0, 0.0 };
        double[] coordY = { 0.0, 0.0, 0.0, 1.0, 1.0 };
        Line original = new Line(coordX, coordY);
        Line result = GeoUtils.removeDuplicateCoordinates(original);
        assertEquals("Should find 4 coordinates instead of 5", 4, result.length());
    }

    public void testLegacyRemoveConsecutiveDuplicateCoordinatesFromPolygon() {
        List<Coordinate> coordinates = createRectangleCoordinatesWithDuplicates(3, 3, 5, 5);
        CoordinatesBuilder builder = new CoordinatesBuilder();
        builder.coordinates(coordinates);
        PolygonBuilder original = new PolygonBuilder(builder);
        PolygonBuilder result = GeoUtils.removeDuplicateCoordinates(original);
        Geometry geom = result.buildGeometry();
        Polygon polygon = (Polygon) geom;
        assertEquals("Should find 5 coordinates instead of 7", 5, polygon.getPolygon().length());
    }

    public void testRemoveConsecutiveDuplicateCoordinatesFromPolygon() {
        List<Coordinate> coordinates = createRectangleCoordinatesWithDuplicates(3, 3, 5, 5);
        LinearRing ring = createLinearRingFromCoordinates(coordinates);
        Polygon original = new Polygon(ring);
        Polygon result = GeoUtils.removeDuplicateCoordinates(original);
        assertEquals("Should find 5 coordinates instead of 7", 5, result.getPolygon().length());
    }

    public void testLegacyRemoveConsecutiveDuplicateCoordinatesFromRealWorldPolygon() {
        List<Coordinate> coordinates = new ArrayList<>();
        coordinates.add(new Coordinate(2.3522219, 48.856614));  // Paris
        coordinates.add(new Coordinate(2.3522219, 48.856614));  // duplicate
        coordinates.add(new Coordinate(2.3541049, 48.856614001)); // very close
        coordinates.add(new Coordinate(2.3541049, 48.856614001)); // duplicate exact
        coordinates.add(new Coordinate(2.3541049, 48.8586972));
        coordinates.add(new Coordinate(2.3522219, 48.8586972));
        coordinates.add(new Coordinate(2.3522219, 48.856614));  // close
        CoordinatesBuilder builder = new CoordinatesBuilder();
        builder.coordinates(coordinates);
        PolygonBuilder original = new PolygonBuilder(builder);
        PolygonBuilder result = GeoUtils.removeDuplicateCoordinates(original);
        Geometry geom = result.buildGeometry();
        Polygon polygon = (Polygon) geom;
        assertTrue("Should find less that 7", polygon.getPolygon().length() < 7);
    }

    public void testRemoveConsecutiveDuplicateCoordinatesFromRealWorldPolygon() {
        List<Coordinate> coordinates = new ArrayList<>();
        coordinates.add(new Coordinate(2.3522219, 48.856614));  // Paris
        coordinates.add(new Coordinate(2.3522219, 48.856614));  // duplicate
        coordinates.add(new Coordinate(2.3541049, 48.856614001)); // very close
        coordinates.add(new Coordinate(2.3541049, 48.856614001)); // duplicate exact
        coordinates.add(new Coordinate(2.3541049, 48.8586972));
        coordinates.add(new Coordinate(2.3522219, 48.8586972));
        coordinates.add(new Coordinate(2.3522219, 48.856614));  // close
        coordinates.add(new Coordinate(2.3522219, 48.856614));  // duplicate
        LinearRing ring = createLinearRingFromCoordinates(coordinates);
        Polygon original = new Polygon(ring);
        Polygon result = GeoUtils.removeDuplicateCoordinates(original);
        assertTrue("Should find less that 6", result.getPolygon().length() < 6);
    }

    public void testLegacyRemoveDuplicatesInHoles() {
        // A Polygon with a hole. The hole can have some duplicates.
        List<Coordinate> rectCoordinates = new ArrayList<>();
        rectCoordinates.add(new Coordinate(-2.0, -2.0));
        rectCoordinates.add(new Coordinate(2.0, -2.0));
        rectCoordinates.add(new Coordinate(2.0, 2.0));
        rectCoordinates.add(new Coordinate(-2.0, 2.0));
        rectCoordinates.add(new Coordinate(-2.0, -2.0));
        PolygonBuilder original = new PolygonBuilder(new LineStringBuilder(rectCoordinates), Orientation.RIGHT);

        List<Coordinate> holes = new ArrayList<>();
        holes.add(new Coordinate(-1, 0));
        holes.add(new Coordinate(1, 0));
        holes.add(new Coordinate(1, 0)); // duplicate
        holes.add(new Coordinate(0, 0));
        holes.add(new Coordinate(0, 0)); // duplicate
        holes.add(new Coordinate(-1.0, 0));
        LineStringBuilder holeWithDuplicates = new LineStringBuilder(holes);

        original.hole(holeWithDuplicates, false);

        PolygonBuilder result = GeoUtils.removeDuplicateCoordinates(original);

        assertEquals("There should be one hole", 1, result.holes().size());
        Geometry hole = result.holes().get(0).buildGeometry();
        Line line = (Line) hole;
        assertEquals("Should find 4 coordinates instead of 6", 4, line.length());
    }

    public void testRemoveDuplicatesInHoles() {
        // A Polygon with a hole. The hole can have some duplicates.
        List<Coordinate> rectCoordinates = new ArrayList<>();
        rectCoordinates.add(new Coordinate(-2.0, -2.0));
        rectCoordinates.add(new Coordinate(2.0, -2.0));
        rectCoordinates.add(new Coordinate(2.0, -2.0)); // duplicate
        rectCoordinates.add(new Coordinate(2.0, 2.0));
        rectCoordinates.add(new Coordinate(-2.0, 2.0));
        rectCoordinates.add(new Coordinate(-2.0, -2.0));
        LinearRing ring = createLinearRingFromCoordinates(rectCoordinates);

        List<Coordinate> holeCoordinates = new ArrayList<>();
        holeCoordinates.add(new Coordinate(-1, 0));
        holeCoordinates.add(new Coordinate(1, 0));
        holeCoordinates.add(new Coordinate(1, 0));  // duplicate
        holeCoordinates.add(new Coordinate(0, 0));
        holeCoordinates.add(new Coordinate(0, 0));  // duplicate
        holeCoordinates.add(new Coordinate(-1.0, 0)); // close
        holeCoordinates.add(new Coordinate(-1.0, 0)); // duplicate
        List<LinearRing> holes = new ArrayList<>();
        LinearRing hole = createLinearRingFromCoordinates(holeCoordinates);
        holes.add(hole);

        // A polygon with a single hole.
        Polygon original = new Polygon(ring, holes);
        Polygon result = GeoUtils.removeDuplicateCoordinates(original);
        assertEquals("There should be one hole", 1, result.getNumberOfHoles());
        assertEquals("Should find 4 coordinates in the hole instead of 6", 4, result.getHole(0).length());
        assertEquals("Should find 5 coordinates instead of 6", 5, result.getPolygon().length());
    }

    public void testLegacyRemoveDuplicateCoordForMultiPolygon() {
        // Given
        MultiPolygonBuilder original = new MultiPolygonBuilder(Orientation.RIGHT);

        // Premier polygone avec doublons
        List<Coordinate> poly1Coords = createRectangleCoordinatesWithDuplicates(0, 0, 2, 2);
        LineStringBuilder poly1Ring = new LineStringBuilder(poly1Coords);
        PolygonBuilder poly1 = new PolygonBuilder(poly1Ring, Orientation.RIGHT, false);
        original.polygon(poly1);

        // Deuxième polygone avec doublons
        List<Coordinate> poly2Coords = createRectangleCoordinatesWithDuplicates(5, 5, 7, 7);
        LineStringBuilder poly2Ring = new LineStringBuilder(poly2Coords);
        PolygonBuilder poly2 = new PolygonBuilder(poly2Ring, Orientation.RIGHT, false);
        original.polygon(poly2);

        // When
        MultiPolygonBuilder result = GeoUtils.removeDuplicateCoordinates(original);

        // Then - Utiliser buildGeometry() pour obtenir le MultiPolygon ES
        MultiPolygon esMultiPolygon = (MultiPolygon) result.buildGeometry();

        assertEquals("Should have 2 polygons", 2, esMultiPolygon.size());

        // Vérifier chaque polygone
        for (int i = 0; i < esMultiPolygon.size(); i++) {
            Polygon polygon = esMultiPolygon.get(i);
            LinearRing exteriorRing = polygon.getPolygon();
            assertEquals("Each polygon should have 5 points", 5, exteriorRing.length());
            assertValidClosedRing(exteriorRing);
        }
    }

    public void testRemoveDuplicateCoordForMultiPolygon() {
        List<Polygon> polygons = new ArrayList<>();

        // First polygon with some duplicated coordinates
        List<Coordinate> poly1Coords = createRectangleCoordinatesWithDuplicates(0, 0, 2, 2);
        polygons.add(new Polygon(createLinearRingFromCoordinates(poly1Coords)));

        // Second polygon with some duplicated coordinates
        List<Coordinate> poly2Coords = createRectangleCoordinatesWithDuplicates(5, 5, 7, 7);
        polygons.add(new Polygon(createLinearRingFromCoordinates(poly2Coords)));

        MultiPolygon original = new MultiPolygon(polygons);
        MultiPolygon result = GeoUtils.removeDuplicateCoordinates(original);

        assertEquals("Original polygon should have 2 polygons", 2, original.size());
        assertEquals("Should have 2 polygons", 2, result.size());

        // Check each polygon
        for (int i = 0; i < result.size(); i++) {
            Polygon polygon = result.get(i);
            LinearRing exteriorRing = polygon.getPolygon();
            assertEquals("Each polygon should have 5 points", 5, exteriorRing.length());
            assertValidClosedRing(exteriorRing);
        }
    }

    public void testLegacyRemoveDuplicateCoordsForGeometryCollection() {
        // Given - Collection avec différents types de géométries
        GeometryCollectionBuilder original = new GeometryCollectionBuilder();

        // Ajouter une ligne avec doublons
        List<Coordinate> lineCoords = new ArrayList<>();
        lineCoords.add(new Coordinate(0.0, 0.0));
        lineCoords.add(new Coordinate(0.0, 0.0)); // duplicate
        lineCoords.add(new Coordinate(1.0, 1.0));
        lineCoords.add(new Coordinate(2.0, 2.0));

        LineStringBuilder line = new LineStringBuilder(lineCoords);
        original.shape(line);

        // Ajouter un polygone avec doublons
        List<Coordinate> polygonCoords = createRectangleCoordinatesWithDuplicates(3, 3, 5, 5);
        LineStringBuilder polygonRing = new LineStringBuilder(polygonCoords);
        PolygonBuilder polygon = new PolygonBuilder(polygonRing, Orientation.RIGHT, false);
        original.shape(polygon);

        // When
        GeometryCollectionBuilder result = GeoUtils.removeDuplicateCoordinates(original);

        // Then - Utiliser buildGeometry() pour obtenir la GeometryCollection ES
        GeometryCollection<Geometry> esCollection = (GeometryCollection<Geometry>) result.buildGeometry();

        assertEquals("Should have 2 geometries", 2, esCollection.size());

        // Vérifier le premier élément (ligne)
        Geometry firstGeom = esCollection.get(0);
        if (firstGeom instanceof Line) {
            Line esLine = (Line) firstGeom;
            assertTrue("Should have less points after deduplication", esLine.length() < lineCoords.size());
        }

        // Vérifier le deuxième élément (polygone)
        Geometry secondGeom = esCollection.get(1);
        if (secondGeom instanceof Polygon) {
            Polygon esPolygon = (Polygon) secondGeom;
            LinearRing exteriorRing = esPolygon.getPolygon();
            assertEquals("Should have 5 points", 5, exteriorRing.length());
            assertValidClosedRing(exteriorRing);
        }
    }

    private void assertValidClosedRing(LinearRing ring) {
        assertTrue("The ring should have at least 4 points", ring.length() >= 4);

        double[] lats = ring.getLats();
        double[] lons = ring.getLons();

        assertEquals("The ring should be closed (latitude)", lats[0], lats[lats.length - 1], 1e-10);
        assertEquals("The ring should closed (longitude)", lons[0], lons[lons.length - 1], 1e-10);
    }

    private List<Coordinate> createRectangleCoordinates(double minX, double minY, double maxX, double maxY) {
        List<Coordinate> coords = new ArrayList<>();
        coords.add(new Coordinate(minX, minY));
        coords.add(new Coordinate(maxX, minY));
        coords.add(new Coordinate(maxX, maxY));
        coords.add(new Coordinate(minX, maxY));
        coords.add(new Coordinate(minX, minY)); // fermeture
        return coords;
    }

    private List<Coordinate> createRectangleCoordinatesWithDuplicates(double minX, double minY, double maxX, double maxY) {
        List<Coordinate> coords = new ArrayList<>();
        coords.add(new Coordinate(minX, minY));
        coords.add(new Coordinate(minX, minY)); // duplicate
        coords.add(new Coordinate(maxX, minY));
        coords.add(new Coordinate(maxX, maxY));
        coords.add(new Coordinate(maxX, maxY)); // duplicate
        coords.add(new Coordinate(minX, maxY));
        coords.add(new Coordinate(minX, minY)); // fermeture
        return coords;
    }

    private LinearRing createLinearRingFromCoordinates(List<Coordinate> coordinates) {
        // Extraire les x (longitudes) et y (latitudes) avec des streams
        double[] lons = coordinates.stream().mapToDouble(coord -> coord.x).toArray();

        double[] lats = coordinates.stream().mapToDouble(coord -> coord.y).toArray();

        return new LinearRing(lats, lons);
    }
}
