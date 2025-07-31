#include <iostream>
#include <hls_stream.h>

#include "data/annotated_mux_config.h"
#include "annotated_mux.hpp"

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
	/***************** TEST 0 - Test util functions *****************/
	std::cout << "\nTEST 0\n-------------\n";
	static_assert(AnnotatedMultiplex::enough_space(512, 64, 400, 100, 256));
	static_assert(!AnnotatedMultiplex::enough_space(512, 64, 400, 500, 256));
	static_assert(AnnotatedMultiplex::enough_space(10, 2, 8, 3, 1));
	static_assert(!AnnotatedMultiplex::enough_space(10, 2, 9, 3, 1));
	static_assert(AnnotatedMultiplex::enough_space(2, 1, 1, 1, 1));
	static_assert(AnnotatedMultiplex::enough_space(1, 0, 0, 0, 0));
	std::cout << "Done.\n";


	/***************** TEST 1 - Complete Pipeline *****************/
	bool has_error = false;
	std::cout << "\nTEST 1\n-------------\n";
	hls::stream<T0> t1in0("t1in0");
	hls::stream<T1> t1in1("t1in1");
	hls::stream<T2> t1in2("t1in2");
	hls::stream<T0> t1out0("t1out0");
	hls::stream<T1> t1out1("t1out1");
	hls::stream<T2> t1out2("t1out2");
	hls::stream<T0> t1expected0("t1expected0");
	hls::stream<T1> t1expected1("t1expected1");
	hls::stream<T2> t1expected2("t1expected2");
	
	// Write the data into the input streams and multiplex as many times
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t1in0.write((i+1)*2);	
		t1in1.write((i+1)*3);	
		t1in2.write((i+1)*4);	
		t1expected0.write((i+1)*2);	
		t1expected1.write((i+1)*3);	
		t1expected2.write((i+1)*4);	
	}

	// Test that RR works properly by only sending into one stream
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t1in0.write(99);
		t1expected0.write(99);
	}

	// Call more times than necessary
	for (unsigned int i = 0; i < REP_COUNT*5; i++) {
		Testbench_annotated_mux_rr_complete(t1in0, t1in1, t1in2, t1out0, t1out1, t1out2);
	}
	
	T0 t1out, t1expected;
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		has_error |= !matches_expected(i, REP_COUNT, 0, t1out0, t1expected0);
		has_error |= !matches_expected(i, REP_COUNT, 1, t1out1, t1expected1);
		has_error |= !matches_expected(i, REP_COUNT, 2, t1out2, t1expected2);
	}
	
	// Again for the case where only s0 received data
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		has_error |= !matches_expected(i, REP_COUNT, 0, t1out0, t1expected0);
	}
	std::cout << "Done.\n\n";
	return has_error;
}