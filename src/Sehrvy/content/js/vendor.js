// Vendor page code
(function () {
    window.addText = function() {

	var json_object = $.parseJSON(
	    $.ajax({
		url: '/query',
		dataType: "json",
		async: false
            }).responseText
	);
	
	jQuery.each(json_object.rows,function(){
	    $('<tr>').appendTo('#product_table_body')
		.append($('<td>').text(this.c[0].v))
		.append($('<td>').text(this.c[1].v));
	});
    };
})();

// vi:sw=2 ts=2 sts=2 et:
