// -- Constants --
const COL_INDEX_UID = 0;
const COL_INDEX_TYPE = 1;
const COL_INDEX_FEATURES = 2;
const COL_INDEX_PARAMS = 3;
const COL_INDEX_STATUS = 4;
const NUM_COLS = 5;


// -- Variables --                          
var dets = createDetails();                         
var socket = io.connect('http://localhost:3000');   // socket.io


// Functions
// ----------------------------------------------------------------------------
/* 
    This function adds a row to the Model page's table of models
    @param uid - unique identifier of model
    @param stats - status of model
 */
function addModel(uid, state) {
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

/*
    This function creates the details element.
*/
function createDetails() {
    var details = $('<tr>', {'class': 'row details'});
    
    // create image
    // TODO: replace this with a Google Charts of the model training history loss vs. iterations 
    var detsImage = $('<img>', {'src': 'https://upload.wikimedia.org/wikipedia/commons/1/16/Acanthocybium_solandri.png'});
    var detsImageContainer = $('<div>', {'class': 'det-img'})
    detsImageContainer.append(detsImage);
    
    // create blurb
    // TODO: replace this with meaningful information
    var detsBlurb = $('<p>', {'class': 'det-txt'});                       // Element - blurb
    detsBlurb.text("");
    
    // create button
    var detsButton = $('<button>', {'type': 'button'});
    detsButton.text('More Details');
    detsButton.on('click', function() {
        var uid = $('.row.details').prev().attr('id');
        //window.location = 'models/' + uid;
         window.open('models/' + uid, '_blank');
    });
    
    // arrange DOM elements as desired
    var detsContent = [];
    for (var i = 0; i < 2; i++) {
    	detsContent.push( $('<td>', {'class': 'col-sm-2'}) );
    }
    
    detsContent[0].append(detsImageContainer);
    detsContent[1].append(detsButton);
    detsContent[1].append(detsBlurb);
    details.append(detsContent[0]);
    details.append(detsContent[1]);
    
    return details;
}

/*
    This function retrieves loss vs. iteration data from the row element
    corresponding to the given id (if it exists).
    @param uid - unique identifier of model
*/
function getLossData(uid) {
    return $('.row#' + uid).data('loss');
}

/* This function hides details about a clicked model. */
function hideModel() {
	var id = $(this).attr('id');
	if (typeof id != 'undefined') {
		var row_id = '.row#' + id; 
		//$(row_id).css({"background-color": "red"});
		dets = $(row_id).closest("tbody").find(".row.details").detach();
	}
}

/* 
    This function renders an image for the details element. 
    @param uid - unique identifier of model
    @return a jQuery object containing the image to be displayed in the details element
*/
function renderDetailsImage(uid) {
    var lossData = getLossData(uid);
    if (lossData == null) {
        return ( $('<img src=\"https://upload.wikimedia.org/wikipedia/commons/1/16/Acanthocybium_solandri.png\">') );
    }
    else {
        return renderGoogleChart(uid, lossData);
    }
}

/*
    This function renders a Google Chart scatter plot given loss data.
    @return a jQuery object containing the Google Chart
*/
function renderGoogleChart(uid, loss) {
    var chart_el = $('<div>', {'id': ('chart-' + uid), 'style': 'width:400; height:300'});
    
    // create data table
    var data = new google.visualization.DataTable();
    data.addColumn('number', 'Iteration');
    data.addColumn('number', 'Obj.');
    for(var i =0; i < loss.length; i++) {
        data.addRow( [i, loss[i]] );
    }
    
    // set chart options
    var options = {'title': 'Objective History', 'width': 400, 'height': 300};
    
    // draw chart
    chart = new google.visualization.ScatterChart(chart_el[0]); // script halts at this line
    chart.draw(data, options);
    
    return chart_el;
}

/* This function displays details about a clicked model. */
function showModel() {
	var id = $(this).attr('id');
	if (typeof id != 'undefined') {
        // update text
        var paramCol = $(this).children(':nth-child(4)');
        updateDetsTxt(paramCol.html());
        
        // update image
        updateDetsImage(renderDetailsImage(id));
        
		var row_id = '.row#' + id;
		//$(row_id).css({"background-color": "yellow"});  // TODO: remove for production
		$(row_id).after(dets);
	}
}

/* 
    This function stores loss vs. iteration data on the row element corresponding 
    to the given id.
    @param uid - unique identifier of model
    @param loss - array of loss values
*/
function storeLossData(uid, loss) {
    $('.row#' + uid).data('loss', loss);
}

/*
    This function updates the details element with the provided jQuery object.
*/
function updateDetsImage(img) {
    dets.find('.det-img:first-child').children(':first').replaceWith(img);
}

/*
    This function updates the details element with the provided jQuery object.
*/
function updateDetsTxt(blurb) {
    dets.find('.det-txt').replaceWith(blurb);
}

/*
    This function updates a row in the Model page's table of models with the given status
    @param uid - unique identifier of model
    @param spec - model specification
*/
function updateModelSpec(uid, spec) {
    $('#type-' + uid).find('.replace').text(spec.specType);                 // update type field
    $('#features-' + uid).find('.replace').text(spec.features.join());      // update features field
    
    // update parameters field
    var paramsEl = $('#params-' + uid).find('.replace');
    paramsEl.text(spec.parameters.join('\n'));
    paramsEl.html(paramsEl.html().replace(/\n/g,'<br/>'));
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


// Script
// ----------------------------------------------------------------------------
google.load('visualization', '1', {packages:['corechart']});

/* event handler - handles page loads */
$(document).ready( function() {
    // make each row of table appear clickable
	$('.row').css("cursor", "pointer");
	
	// add on click event handler - toggles detail view
	$('.row').toggle(showModel, hideModel);
});

/* evend handler - handles socket connections */
socket.on('connection', function (data) {
    console.log("Socket connected.");
    socket.emit('ack', { status: 'Success!' });
});

/* event handler - handles socket messages */
socket.on('message', function(data) {
    try {
        var message = JSON.parse(data.message);
        
        // handle lossData messages
        if(message.msgType == "lossData") {
            storeLossData(message.msg.uid, message.msg.lossData);
        }
        
        // handle modelSpec messages
        if(message.msgType == "modelSpec") {
            updateModelSpec(message.msg.uid, message.msg.modelSpec);
        }
        
        // handle status messages
        if(message.msgType == "status") {
            var status = message.msg.status;
            if(status == "Running") {
                addModel(message.msg.uid, message.msg.status);
            }
            else {
                updateStatus(message.msg.uid, message.msg.status);
            }
        }
    }
    
    catch (err) {
        alert(err.message);
    }
});
// ----------------------------------------------------------------------------
