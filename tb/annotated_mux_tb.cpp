#include <iostream>
#include <hls_stream.h>

#include "data/annotated_mux_config.h"
#include "annotated_mux.hpp"

void Testbench_annotated_mux_rr(hls::stream<T0> &in0, hls::stream<T1> &in1, hls::stream<T2> &in2, hls::stream<TO> &out);
void Testbench_annotated_mux_rr_complete(hls::stream<T0> &in0, hls::stream<T1> &in1, hls::stream<T2> &in2, hls::stream<T0> &out0, hls::stream<T1> &out1, hls::stream<T2> &out2);

template<typename T>
bool matches_expected(unsigned int iter, unsigned int total_iter, unsigned int stream_index, hls::stream<T> &out, hls::stream<T> &expected) {
	if (out.empty()) {
		std::cout << "[REP " << iter+1 << "/" << total_iter << "] ERROR: Output stream empty. Stream index: " << stream_index << std::endl;
	}
	if (expected.empty()) {
		std::cout << "[REP " << iter+1 << "/" << total_iter << "] ERROR: Expected stream empty. Stream index: " << stream_index << std::endl;
	}
	T actual = out.read();
	T exp = expected.read();
	if (actual != exp) {
		std::cout << "[REP " << iter+1 << "/" << total_iter << "] ERROR: Data mismatch on stream " << stream_index << ". Expected " << exp << " but got " << actual << std::endl;
		return false;
	}
	return true;
}


int main() {
	/***************** TEST 1 - Only Mux *****************/
	std::cout << "\nTEST 1\n-------------\n";
	hls::stream<T0> t1in0;
	hls::stream<T1> t1in1;
	hls::stream<T2> t1in2;
	hls::stream<TO> t1out;
	hls::stream<TO> t1expected;

	// First let all inputs have the same availability
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t1in0.write((i+1)*2);
		t1in1.write((i+1)*3);
		t1in2.write((i+1)*4);
		t1expected.write(0);
		t1expected.write((i+1)*2);
		t1expected.write(1);
		t1expected.write((i+1)*3);
		t1expected.write(2);
		t1expected.write((i+1)*4);
	}

	// Now only t1in0 sends
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t1in0.write(99);
		t1expected.write(0);
		t1expected.write(99);
	}

	bool has_error = false;
	unsigned int timeout = 20;
	while (timeout > 0) {
		TO header;
		TO data;
		Testbench_annotated_mux_rr(t1in0, t1in1, t1in2, t1out);
		if (t1out.empty()) {
			timeout--;
			continue;
		}
		header = t1out.read();
		data = t1out.read();
		TO exp_header, exp_data;
		exp_header = t1expected.read();
		exp_data = t1expected.read();
		if (exp_header != header) {
			std::cout << "ERROR: Header mismatch. Expected " << exp_header << " but got " << header << std::endl;
			has_error = true;
		}
		if (exp_data != data) {
			std::cout << "ERROR: Data mismatch. Expected " << exp_data << " but got " << data << std::endl;
			has_error = true;
		}
	}
	std::cout << "Done.\n";

	/***************** TEST 2 - Complete Pipeline *****************/
	std::cout << "\nTEST 2\n-------------\n";
	hls::stream<T0> t2in0;
	hls::stream<T1> t2in1;
	hls::stream<T2> t2in2;
	hls::stream<T0> t2out0;
	hls::stream<T1> t2out1;
	hls::stream<T2> t2out2;
	hls::stream<T0> t2expected0;
	hls::stream<T1> t2expected1;
	hls::stream<T2> t2expected2;
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t2in0.write((i+1)*2);	
		t2in1.write((i+1)*3);	
		t2in2.write((i+1)*4);	
		t2expected0.write((i+1)*2);	
		t2expected1.write((i+1)*3);	
		t2expected2.write((i+1)*4);	
	}
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t2in0.write(99);
		t2expected0.write(99);
	}

	// Call more times than necessary
	for (unsigned int i = 0; i < REP_COUNT*5; i++) {
		Testbench_annotated_mux_rr_complete(t2in0, t2in1, t2in2, t2out0, t2out1, t2out2);
	}
	
	T0 t2out, t2expected;
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		has_error |= matches_expected(i, REP_COUNT, 0, t2out0, t2expected0);
		has_error |= matches_expected(i, REP_COUNT, 1, t2out1, t2expected1);
		has_error |= matches_expected(i, REP_COUNT, 2, t2out2, t2expected2);
	}
	
	// Again for the case where only s0 received data
	std::cout << "Checking extra inputs from stream 0" << std::endl;
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		has_error |= matches_expected(i, REP_COUNT, 0, t2out0, t2expected0);
	}
	std::cout << "Done.\n";
	return has_error;
}