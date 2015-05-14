
package com.umaircheema.sbda.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.EsriFeature;
import com.esri.json.EsriFeatureClass;


/**
 * A parser that converts GeoJSON stream into Esri Features
 * @author umaircheema
 *
 */
public final class FeatureFactory {

	// Using Jackson parser library to parse JSON 
	private  final ObjectMapper mapper = new ObjectMapper();

	// geometries in GeoJSON are assumed to be in CRS84 (Esri Wkid = 4326)
	private final SpatialReference inSR = SpatialReference.create(4326);

	// output CRS can be configured to be different
	private SpatialReference outSR = null;

	// field names defined in the GeoJson spec
	private final static String FIELD_COORDINATES = "coordinates";
	private final static String FIELD_FEATURE = "Feature";
	private final static String FIELD_FEATURES = "features";
	private final static String FIELD_FEATURE_COLLECTION = "FeatureCollection";
	private final static String FIELD_GEOMETRY = "geometry";
	private final static String FIELD_GEOMETRIES = "geometries";
	private final static String FIELD_GEOMETRY_COLLECTION = "GeometryCollection";
	private final static String FIELD_PROPERTIES = "properties";
	private final static String FIELD_TYPE = "type";

	private enum GeometryType {
		POINT("Point"),
		MULTI_POINT("MultiPoint"),
		LINE_STRING("LineString"),
		MULTI_LINE_STRING("MultiLineString"),
		POLYGON("Polygon"),
		MULTI_POLYGON("MultiPolygon");

		private final String val;

		GeometryType(String val) {
			this.val = val;  
		}
		/**
		 * Get Geometry type
		 * @param val
		 * @return
		 */
		public static  GeometryType fromString(String val) {
			for (GeometryType type : GeometryType.values()) {
				if (type.val.equals(val)) {
					return type;
				}
			}
			return null;
		}
	}
	/**
	 * Get List of features
	 * @param inputStream
	 * @return
	 */
	public  List<EsriFeature> parseFeatures(InputStream inputStream) {
		try {

			JsonParser parser = new JsonFactory().createJsonParser(inputStream);
			return parseFeatures(parser);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Get List of Geometries
	 * @param file
	 * @return
	 */
	public  List<Geometry> parseGeometries(File file) {
		try {
			JsonParser parser = new JsonFactory().createJsonParser(file);
			return parseGeometries(parser);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Get list of geometries
	 * @param str
	 * @return
	 */
	public  List<Geometry> parseGeometries(String str) {
		try {
			JsonParser parser = new JsonFactory().createJsonParser(str);
			return parseGeometries(parser);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Get List of Esri Features
	 * @param parser
	 * @return
	 */
	private  List<EsriFeature> parseFeatures(JsonParser parser) {

		try {
			JsonNode node = mapper.readTree(parser);
			String type = node.path(FIELD_TYPE).getTextValue();
			if (type.equals(FIELD_FEATURE_COLLECTION)) {
				ArrayNode jsonFeatures = (ArrayNode) node.path(FIELD_FEATURES);
				return parseFeatures(jsonFeatures);
			} else if (type.equals(FIELD_GEOMETRY_COLLECTION)) {
				ArrayNode jsonFeatures = (ArrayNode) node.path(FIELD_GEOMETRIES);
				List<Geometry> geometries = parseGeometries(jsonFeatures);
				List<EsriFeature> features = new ArrayList<EsriFeature>();

				return features;
			}
		} catch (Exception ex) {
			return new ArrayList<EsriFeature>();
		}

		return new ArrayList<EsriFeature>();
	}

	/**
	 * Get List of Esri Features
	 * @param inputStream
	 * @return
	 */
	public List<EsriFeature> parseESRIJSONFeatures(InputStream inputStream){

		List<EsriFeature> features = new ArrayList<EsriFeature>();
		try {
			EsriFeatureClass esriFeatureClass=EsriFeatureClass.fromJson(inputStream);
			if(esriFeatureClass!=null){
				EsriFeature[] esrifeatures=esriFeatureClass.features;
				features=new ArrayList<EsriFeature>(Arrays.asList(esrifeatures));
			}
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return features;
	}

	/**
	 * Get list of Esri Features
	 * @param jsonFeatures
	 * @return
	 */
	private  List<EsriFeature> parseFeatures(ArrayNode jsonFeatures) {
		List<EsriFeature> features = new ArrayList<EsriFeature>();
		for (JsonNode jsonFeature : jsonFeatures) {
			String type = jsonFeature.path(FIELD_TYPE).getTextValue();
			if (!FIELD_FEATURE.equals(type)) {
				continue;
			}
			Geometry g = parseGeometry(jsonFeature.path(FIELD_GEOMETRY));

			Map<String, Object> attributes = parseProperties(jsonFeature.path(FIELD_PROPERTIES));
			EsriFeature ef = new EsriFeature();
			ef.geometry=g;
			ef.attributes=attributes;
			features.add(ef);
		} 
		return features; 
	}

	/**
	 * Get List of Geometries given a Parser
	 * @param parser
	 * @return
	 */
	private  List<Geometry> parseGeometries(JsonParser parser) {
		try {
			JsonNode node = mapper.readTree(parser);
			String type = node.path(FIELD_TYPE).getTextValue();
			if (type.equals(FIELD_GEOMETRY_COLLECTION)) {
				ArrayNode jsonFeatures = (ArrayNode) node.path(FIELD_GEOMETRIES);
				return parseGeometries(jsonFeatures);
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		return Collections.emptyList();
	}

	/**
	 * Parse Geometries
	 * @param jsonGeometries
	 * @return
	 */
	private  List<Geometry> parseGeometries(ArrayNode jsonGeometries) {
		List<Geometry> geometries = new ArrayList<Geometry>();
		for (JsonNode jsonGeometry : jsonGeometries) {
			Geometry g = parseGeometry(jsonGeometry);
			geometries.add(g);
		} 
		return geometries; 
	}

	/**
	 * Parse properties
	 * @param node
	 * @return
	 */
	private  Map<String, Object> parseProperties(JsonNode node) {
		Map<String, Object> properties = new HashMap<String, Object>();
		Iterator<Map.Entry<String, JsonNode>> propertyInterator = node.getFields(); 
		while (propertyInterator.hasNext()) {
			Map.Entry<String, JsonNode> property = propertyInterator.next();
			JsonNode jsonValue = property.getValue();
			if (jsonValue.isInt()) {
				properties.put(property.getKey(), Integer.parseInt(property.getValue().toString()));
			} else if (jsonValue.isDouble()) {
				properties.put(property.getKey(), Double.parseDouble(property.getValue().toString()));
			} else if (jsonValue.isTextual()) {
				properties.put(property.getKey(), property.getValue().toString());
			}
		}
		return properties;
	}

	/**
	 * Parse Geometry
	 * @param node
	 * @return
	 */
	private  Geometry parseGeometry(JsonNode node) {
		GeometryType type = GeometryType.fromString(node.path(FIELD_TYPE).getTextValue());
		return parseCoordinates(type, node.path(FIELD_COORDINATES));
	}

	/**
	 * Parse Coordinates
	 * @param type
	 * @param node
	 * @return
	 */
	private  Geometry parseCoordinates(GeometryType type, JsonNode node) {
		Geometry g = null;
		switch (type) {
		default:
		case POINT:
			g = parsePointCoordinates(node);    
			break;
		case MULTI_POINT:
			g = parseMultiPointCoordinates(node);
			break;
		case LINE_STRING:
			g = parseLineStringCoordinates(node);
			break;
		case MULTI_LINE_STRING:
			g = parseMultiLineStringCoordinates(node);
			break;
		case POLYGON:
			g = parsePolygonCoordinates(node);
			break;
		case MULTI_POLYGON:
			g = parseMultiPolygonCoordinates(node);
			break;
		}
		return g;
	}

	/**
	 * Parse Point Coordinates
	 * @param node
	 * @return
	 */
	private  Point parsePointCoordinates(JsonNode node) {
		Point p = new Point();
		p.setXY(Double.parseDouble(node.get(0).toString()),Double.parseDouble(node.get(1).toString()));
		if (node.size() == 3) {
			p.setZ(Double.parseDouble(node.get(2).toString()));
		}
		return p;
	}

	/**
	 * Parse multipoint coordinates
	 * @param node
	 * @return
	 */
	private  MultiPoint parseMultiPointCoordinates(JsonNode node) {
		MultiPoint p = new MultiPoint();
		ArrayNode jsonPoints = (ArrayNode) node;
		for (JsonNode jsonPoint : jsonPoints) {
			Point point = parsePointCoordinates(jsonPoint);
			p.add(point);
		}
		return p;
	}

	/**
	 * Parse Line String Coordinates
	 * @param node
	 * @return
	 */
	private  Polyline parseLineStringCoordinates(JsonNode node) {
		Polyline g = new Polyline();
		boolean first = true;
		ArrayNode points = (ArrayNode) node;
		for (JsonNode point : points) {
			Point p = parsePointCoordinates(point);
			if (first) {
				g.startPath(p);
				first = false;
			} else {
				g.lineTo(p);
			}  
		}
		return g;
	}

	/**
	 * Parse Multiline Coordinates
	 * @param node
	 * @return
	 */
	private  Polyline parseMultiLineStringCoordinates(JsonNode node) {
		Polyline g = new Polyline();
		ArrayNode jsonLines = (ArrayNode) node;
		for (JsonNode jsonLine : jsonLines) {
			Polyline line = parseLineStringCoordinates(jsonLine);
			g.add(line, false);
		}
		return g;
	}

	/**
	 * Parse Simple Polygon Coordinates
	 * @param node
	 * @return
	 */
	private  Polygon parseSimplePolygonCoordinates(JsonNode node) {
		Polygon g = new Polygon();
		boolean first = true;
		ArrayNode points = (ArrayNode) node;
		for (JsonNode point : points) {
			Point p = parsePointCoordinates(point);
			if (first) {
				g.startPath(p);
				first = false;
			} else {
				g.lineTo(p);
			}  
		}
		g.closeAllPaths();
		return g;
	}

	/**
	 * Parse Polygon Coordinates
	 * @param node
	 * @return
	 */
	private  Polygon parsePolygonCoordinates(JsonNode node) {
		Polygon g = new Polygon();
		ArrayNode jsonPolygons = (ArrayNode) node;
		for (JsonNode jsonPolygon : jsonPolygons) {
			Polygon simplePolygon = parseSimplePolygonCoordinates(jsonPolygon);
			g.add(simplePolygon, false);
		}
		return g;
	}

	/**
	 * Parse MultiPolygon Coordinates
	 * @param node
	 * @return
	 */
	private  Polygon parseMultiPolygonCoordinates(JsonNode node) {
		Polygon g = new Polygon();
		ArrayNode jsonPolygons = (ArrayNode) node;
		for (JsonNode jsonPolygon : jsonPolygons) {
			Polygon simplePolygon = parsePolygonCoordinates(jsonPolygon);
			g.add(simplePolygon, false);
		}
		return g;
	}
}