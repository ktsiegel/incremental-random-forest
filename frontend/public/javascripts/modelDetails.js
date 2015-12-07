// -- Constants --
const COL_INDEX_UID = 0;
const COL_INDEX_TYPE = 1;
const COL_INDEX_FEATURES = 2;
const COL_INDEX_PARAMS = 3;
const COL_INDEX_STATUS = 4;
const NUM_COLS = 6;

// -- Variables --
var cur_row = "";

// -- DOM Elements ---
// Element - details
var dets = $('<tr>', {'class': 'row details'});

// Element - blurb
// TODO: replace this with meaningful information
var detsBlurb = $('<p></p>');
detsBlurb.append("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");

// Element - button
var detsButton = $(' <button type=\"button\">More Details</button> ');

// Element - image
// TODO: replace this with a Google Charts of the model training history loss vs. iterations 
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


// -- Socket IO --
var socket = io.connect('http://localhost:3000');


// Functions
// ----------------------------------------------------------------------------
/* 
    This function adds a row to the Model page's table of models
    @param uid - unique identifier of model
    @param stats - status of model
 */
function addRow(uid, state) {
    // build row element
    var tr = $('<tr>', {'class': 'row', 'id': uid})
    for (var i = 0; i < NUM_COLS; i++) {
        var td = $('<td>', {'class': 'col-sm-2'});
        var sp = $('<span>', {'class': 'replace'});
        var id_prefix = "def-";
        
        sp.text('?');
        td.append(sp);
        switch(i) {
            case COL_INDEX_UID:
                sp.text(uid);
                break;
                
            case COL_INDEX_TYPE:
                id_prefix = 'type-';
                break;
                
            case COL_INDEX_FEATURES:
                id_prefix = 'features-';
                break;
                
            case COL_INDEX_PARAMS:
                id_prefix = 'params-';
                break;
                
            case COL_INDEX_STATUS:
                id_prefix = 'stat-';
                sp.text(state);
                break;
                
            default:
                break;
        }
        
        td.attr('id', (id_prefix + uid));
        tr.append(td);
    }
    
    // add row to DOM
    tr.toggle(showModel, hideModel);
    $('tbody').append(tr);
}

/* This function hides details about a clicked model. */
function hideModel() {
	var id = $(this).attr('id');
	if (typeof id != 'undefined') {
		var row_id = '.row #' + id; 
		$(row_id).css({"background-color": "red"});
		$(row_id).closest("tbody").find(".row.details").detach();
	}
}

/* This function displays details about a clicked model. */
function showModel() {
	var id = $(this).attr('id');
	if (typeof id != 'undefined') {
		var row_id = '.row #' + id;
		$(row_id).css({"background-color": "yellow"});
		$(row_id).after(dets);
	}
}

/*
    This function updates a row in the Model page's table of models with the given status
    @param uid - unique identifier of model
    @param spec - model specification
*/
function updateModelSpec(uid, spec) {
    var psv = spec.split('(');          // parenthesis-separated values
    
    // update type field
    var type = psv[0] + "Model";
    $('#type-' + uid).find('.replace').text(type);
    
    // transform the 2nd element of psv into a more malleable form
    var params = psv[1].slice(0, psv[1].length - 1);    // remove trailing ')'
    params = params.replace(/ /g, '');                  // remove all spaces
    
    // update features field
    var features = params.substring(params.search(/\[/) + 1, params.search(/\]/));
    $('#features-' + uid).find('.replace').text(features);
    
    // update params field
    params = params.replace(/=/g, ':');
    var csv = params.split(',');
    var parameters = csv[2] + '\n' + csv[3];
    $('#params-' + uid).find('.replace').text(parameters);
}

/*
    This function updates a row in the Model page's table of models with the given status
    @param uid - unique identifier of model
    @param stats - status of model
*/
function updateStatus(uid, state) {
    $('#stat-' + uid).find('.replace').text(state);
}
// ----------------------------------------------------------------------------


// Event Handlers
// ----------------------------------------------------------------------------
/* handles page loads */
$(document).ready( function() {
	// make each row of table appear clickable
	$('.row').css("cursor", "pointer");
	
	// add on click event handler - toggles detail view
	$('.row').toggle( showModel, hideModel);
});

/* handles socket connections */
socket.on('connection', function (data) {
    console.log("Socket connected.");
    socket.emit('ack', { status: 'Success!' });
});

/* handles socket messages */
socket.on('message', function(data) {
	var message = data.message.trim();
    
    // change status field of appropriate row to "Running"
	if (message.search("Running") > -1) {
        var ssv = message.split(" ");  // space-separated values
        cur_row = ssv[3];
        addRow(cur_row, ssv[1]);	
	}
    
    // change status field of appropriate row to "Finished"
    if (message.search("Finished") > -1) {
        var ssv = message.split(" ");  // space-separated values
        updateStatus(cur_row, ssv[1]);
    }
    
    // change type, features, and param fields to appropriate values
    if (message.search("ModelSpec") > -1) {
        var csv = message.split(":");  // colon-separated values
        updateModelSpec(cur_row, csv[2].trim());
    }
});
// ----------------------------------------------------------------------------
