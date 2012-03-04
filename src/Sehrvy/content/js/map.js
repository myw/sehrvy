// Map manipulation code
(function () {

    window.Map = function ($id, label, url) {
        var data, options, chart, json;

        json = $.ajax({
            url: url,
            dataType: "json",
            async: false
        }).responseText;

        data = new google.visualization.DataTable(json);

        options = {
          region: 'US',
          resolution: 'provinces',
          datalessRegionColor: 'FFFFFF'
        };

        chart = new google.visualization.GeoChart($($id).get(0));
        chart.draw(data, options);
    };

})();

// vi:sw=2 ts=2 sts=2 et:

