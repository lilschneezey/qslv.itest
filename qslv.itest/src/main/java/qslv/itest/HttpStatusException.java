package qslv.itest;

import org.springframework.http.ResponseEntity;

public class HttpStatusException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	private ResponseEntity<?> response;
	
	HttpStatusException(ResponseEntity<?> response) {
		this.response = response;
	}
	public ResponseEntity<?> getResponse() {
		return response;
	}
	public void setResponse(ResponseEntity<?> response) {
		this.response = response;
	}

}
