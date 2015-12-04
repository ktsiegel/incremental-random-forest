// -- DOM Elements ---
// Element - details
var dets = $('<tr>', {'class': 'row', 'id': 'details'});

// Element - blurb
var detsBlurb = $('<p></p>');
detsBlurb.append("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");

// Element - button
var detsButton = $(' <button type=\"button\">More Details</button> ');

// Element - image
var detsImage = $('<img src=\"http://www.clipartbest.com/cliparts/RiA/6Lx/RiA6LxxLT.png\">');

// arrange DOM elements as desired
var detsContent = [];
for (var i = 0; i < 2; i++) {
	detsContent.push( $('<td>', {'class': 'col-sm-2'}) );
}

detsContent[0].append(detsImage);
detsContent[1].append(detsBlurb);
detsContent[1].append(detsButton);
dets.append(detsContent[0]);
dets.append(detsContent[1]);


// --- Event Handlers ---
$(document).ready( function() {
	// make each row of table appear clickable
	$('.row').css("cursor", "pointer");
	
	// add on click event handler - toggles detail view
	$('.row').toggle( function(event) {
		var id = $(this).attr('id');
		if (typeof id != 'undefined') {
			var row_id = '.row #' + id;
			$(row_id).css({"background-color": "yellow"});
			$(row_id).after(dets);
		}
	}, function(event) {
		var id = $(this).attr('id');
		if (typeof id != 'undefined') {
			var row_id = '.row #' + id; 
			$(row_id).css({"background-color": "red"});
			$(row_id).closest("tbody").find("#details").detach();
		}
	})
});
