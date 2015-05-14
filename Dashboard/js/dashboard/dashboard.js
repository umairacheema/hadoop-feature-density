/**
Creates Dashboard
**/
function init() {

    //Number of equal interval classes
    //in the thematic map
    var numberOfClasses = 7;
    //Key attribute
    var keyAttributeName = getQueryStringParameter("attribute");
    //JSON format
    var jsonFormat = getQueryStringParameter("format");
    //Feature Density Range
    var featureDensityRange = 0;
    //Class intervals
    var classInterval = 0;
    //Objects to store Key Value Pairs
    var featureCount = [];
    var featureAreas = [];
    var featureDensity = [];
    var totalFeatures = 0;
    var fieldNames = [];
    //Feature Density Min, Max
    var minimumFeatureDensity = Number.MAX_VALUE;
    var maximumFeatureDensity = 0;

    //Thematic Map Layer
    var thematicMapLayer;

    //Set Data Format
    var dataFormat = null;
    if (jsonFormat === "geojson") {
        dataFormat = new ol.format.GeoJSON();
    } else {
       
        dataFormat = new ol.format.EsriJSON();
    }

    //Set up Popup

    var container = document.getElementById('popup');
    var content = document.getElementById('popup-content');
    var closer = document.getElementById('popup-closer');

    /**
     * Add a click handler to hide the popup.
     * @return {boolean} Don't follow the href.
     */
    closer.onclick = function () {
        overlay.setPosition(undefined);
        closer.blur();
        return false;
    };


    /**
     * Create an overlay to anchor the popup to the map.
     */
    var overlay = new ol.Overlay( /** @type {olx.OverlayOptions} */ ({
        element: container,
        autoPan: true,
        autoPanAnimation: {
            duration: 250
        }
    }));

    //Load the results.txt file    
    $.ajax({
        url: "results.txt",
        dataType: "text",
        success: function (data) {
            //Break the data into lines
            var resultlines = data.match(/^.*([\n\r]+|$)/gm);
            if (resultlines.length > 0) {
                for (var i = 0; i < resultlines.length; i++) {
                    if (resultlines[i].length > 0) {
                        keyValuePair = resultlines[i].split(',');
                        if (keyValuePair.length == 2) {
                            featureCount[keyValuePair[0]] = keyValuePair[1];
                            fieldNames.push(keyValuePair[0]);
                        }
                    }
                }
            }

        },
        complete: displayFeatureCount

    });


    /**
    Display Feature Count List
    **/

    function displayFeatureCount() {

        var table = "<table class=\"table table-striped\" <thead><th>Feature</th><th>Count</th>";
        table = table + "</thead><tbody>";
        //Copy Attribute Values
        for (var i = 0; i < fieldNames.length; i++) {
            table = table + "<tr>";
            table = table + "<td>" + fieldNames[i] + "</td>";
            table = table + "<td>" + featureCount[fieldNames[i]] + "</td>";
            table = table + "</tr>";
        }
        //Close table with body and table tags
        table = table + "</tbody></table>"
        document.getElementById('feature-count').innerHTML = table || '&nbsp';

        loadMap();
    }

    /**
    Loads Map
    **/
    function loadMap() {


            //Default Style for Thematic Map    
            var thematicMapLayerStyle = {

                'Polygon': [new ol.style.Style({
                    fill: new ol.style.Fill({
                        color: 'rgba(0,0,255,0.5)'
                    }),
                    stroke: new ol.style.Stroke({
                        color: '#00f',
                        width: 1
                    })
                })],
                'MultiPolygon': [new ol.style.Style({
                    fill: new ol.style.Fill({
                        color: 'rgba(0,0,255,0.5)'
                    }),
                    stroke: new ol.style.Stroke({
                        color: '#00f',
                        width: 1
                    })
                })]
            };


            //Define base layers
            var layers = [
  new ol.layer.Tile({
                    style: 'AerialWithLabels',
                    source: new ol.source.MapQuest({
                        layer: 'osm'
                    })
                })
];

            //Create view centered in the middle of WGS84 datum   
            var view = new ol.View({
                center: ol.proj.transform(
        [0, 0], 'EPSG:4326', 'EPSG:3857'),
                zoom: 3,
                maxZoom: 10,
                minZoom: 2
            });

            //Prepare the map and add it to Dashboard map container   
            var map = new ol.Map({
                layers: layers,
                overlays: [overlay],
                target: 'map',
                view: view
            });

            //Parse vector GeoJSON data for overlaying
            var thematicMapSource = new ol.source.Vector();

            $.ajax({
                url: 'data.json',
                type: 'get',
                dataType: 'json',
                error: function (data) {},
                success: function (data) {
                    var geojsonFormat = dataFormat;
                    var features = geojsonFormat.readFeatures(data, {
                        dataProjection: 'EPSG:4326',
                        featureProjection: 'EPSG:3857'
                    });

                    computeFeatureDensities(features);
                    addVectorLayer();
                }
            });

            /**
            Compute Feature Density
            **/
            function computeFeatureDensities(features) {

                for (var i = 0; i < features.length; i++) {
                    var name = features[i].get(keyAttributeName);

                    //TO DO Use Proj4S Library and Robinson Projection 
                    //proj4.defs("ESRI:54030","+proj=robin +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs");
                    var area = features[i].getGeometry().getArea();
                    //Convert area into Kilometer Square
                    area = area * 1e-6;
                    //Number of Features per square kilometers
                    var density = 0;
                    if (featureCount[name] != null) {
                        featureAreas[name] = area;
                        density = parseFloat(featureCount[name]) / area;
                        featureDensity[name] = density;

                    } else {
                        featureAreas[name] = 0;
                        featureDensity[name] = 0;
                    }
                    if (density <= minimumFeatureDensity) {
                        minimumFeatureDensity = density;
                    } else if (density > maximumFeatureDensity) {
                        maximumFeatureDensity = density;
                    }

                }

                //Compute Feature Density Range and Class Intervals
                featureDensityRange = maximumFeatureDensity - minimumFeatureDensity;
                classInterval = featureDensityRange / numberOfClasses;
            }

            /**
            Compute Class using the Density
            **/
            function getClass(fDensity) {

                var classID = 0;
                for (var i = 0; i < numberOfClasses; i++) {
                    classBreak = minimumFeatureDensity + (i * classInterval);
                    if (fDensity >= classBreak)
                        classID = i + 1;
                }

                return classID;
            }

            //Style function to be applied to features
            var styleFunction = function (feature, resolution) {
                name = feature.get(keyAttributeName);
                classID = getClass(featureDensity[name]);
                styler = getStyleForClass(classID);

                return styler;

            };

            //Classification Style
            function getStyleForClass(classID) {
                //Colors should be selected using ColorBrewer2 site
                colorRamp = ['rgba(255,255,204,0.8)',
                            'rgba(255,237,160,0.8)',
                            'rgba(254,217,118,0.8)',
                            'rgba(254,178,76,0.8)',
                            'rgba(253,141,60,0.8)',
                            'rgba(252,78,42,0.8)',
                            'rgba(227,26,28,0.8)',
                            'rgba(189,0,38,0.8)',
                            'rgba(128,0,38,0.8)'];

                classColor = 'rgba(0,0,255,0.5)';
                if (classID < colorRamp.length) {
                    classColor = colorRamp[classID - 0];
                }

                return [new ol.style.Style({
                    fill: new ol.style.Fill({
                        color: classColor
                    }),
                    stroke: new ol.style.Stroke({
                        color: '#888',
                        width: 1
                    })
                })];

            }


            /**
            Add Vector Layer
            **/
            function addVectorLayer() {


                    thematicMapSource = new ol.source.Vector({
                        url: 'data.json',
                        format: dataFormat
                    });


                    //Create a vector layer
                    thematicMapLayer = new ol.layer.Vector({
                        title: 'Thematic Map',
                        source: thematicMapSource,
                        style: styleFunction,
                        visible: true
                    });

                    //Register change event    

                    thematicMapSource.once('change', function (event) {
                        if (thematicMapSource.getState() == 'ready') {
                            //Pan to the new extent
                            var layerExtent = thematicMapSource.getExtent();
                            view.fitExtent(layerExtent);


                        }

                    });

                    map.addLayer(thematicMapLayer);

                } //add vector layer

            var updateViews = function (pixel) {
                var features = [];
                map.forEachFeatureAtPixel(pixel, function (feature, layer) {
                    if (layer === thematicMapLayer)
                        features.push(feature);
                });
                if (features.length > 0) {
                    var keys = features[0].getKeys();
                    var popupInfo = "<b>" + keyAttributeName + "</b><br/>" + features[0].get(keyAttributeName) + "<br/><br/>";
                    popupInfo = popupInfo + "<b>Count</b><br/>" + featureCount[features[0].get(keyAttributeName)] + "<br/><br/>";
                    for (var i = 0; i < keys.length; i++) {
                        popupInfo = popupInfo + "<b>" + keys[i] + "</b>" + "<br/>" + features[0].get(keys[i]) + "<br/><br/>";
                    }
                    content.innerHTML = popupInfo;

                }
            };

            map.on('click', function (evt) {
                updateViews(evt.pixel);
                overlay.setPosition(evt.coordinate);
            });

        }
        /**
        Get Query String Parameters
        **/

    function getQueryStringParameter(param) {
        var parameters = [];
        var parameterValue;
        var value = "";
        if (param == "attribute") {
            value = "name";
        } else if (param == "format") {
            value = "geojson";
        }
        var queryString = document.URL.split('?')[1];
        if (queryString != undefined) {
            queryString = queryString.split('&');
            for (var i = 0; i < queryString.length; i++) {
                parameterValue = queryString[i].split('=');
                parameters.push(parameterValue[1]);
                parameters[parameterValue[0]] = parameterValue[1];
            }
            return parameters[param];
        }
        return value;
    }

}
