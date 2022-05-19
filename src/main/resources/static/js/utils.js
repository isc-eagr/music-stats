	function secondsToString(seconds){
		var secondsInADay = 24*60*60;
		var secondsInAnHour = 60 * 60;
	
		var totalMinutes = Math.trunc(seconds / 60);
		var totalHours = Math.trunc(totalMinutes / 60);
		var finalDays = Math.trunc(totalHours / 24);
	
		var remainingSecondsAfterDays = Math.trunc(seconds - (secondsInADay*finalDays));
		var finalHours = Math.trunc(remainingSecondsAfterDays / 60 / 60);
		var remainingSecondsAfterHours = Math.trunc(remainingSecondsAfterDays - (secondsInAnHour*finalHours));
		var finalMinutes = Math.trunc(remainingSecondsAfterHours / 60);
		var finalSeconds = Math.trunc(remainingSecondsAfterHours - (60*finalMinutes));
		return finalDays+" days, "+ finalHours+" hours, "+finalMinutes+" minutes, "+finalSeconds+" seconds.";
	}
	
	function secondsToDays(seconds){
		var secondsInADay = 24*60*60;
		
		return Math.trunc(seconds/secondsInADay);
	}
	
	function htmlDecode(input) {
		  var e = document.createElement('textarea');
		  e.innerHTML = input;
		  return e.childNodes.length === 0 ? "" : e.childNodes[0].nodeValue;
	}
	
	var dateConfig= 
		{minDate: "2010-01-01"};
	
	window.onload = function(){
		
		
		flatpickr("#dateStart", dateConfig);
		flatpickr("#dateEnd", dateConfig);
	}
	
	