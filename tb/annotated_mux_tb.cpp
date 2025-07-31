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
	unsigned int count = 0;
	
	// Write the data into the input streams and multiplex as many times
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t1in0.write((i+1)*2);	
		t1in1.write((i+1)*3);	
		t1in2.write((i+1)*4);	
		t1expected0.write((i+1)*2);	
		t1expected1.write((i+1)*3);	
		t1expected2.write((i+1)*4);	
		count += 3;
	}

	// Test that RR works properly by only sending into one stream
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t1in0.write(99);
		t1expected0.write(99);
		count++;
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
		count -= 3;
	}
	
	// Again for the case where only s0 received data
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		has_error |= !matches_expected(i, REP_COUNT, 0, t1out0, t1expected0);
		count--;
	}

	if (count != 0 || t1expected0.size() != 0 || t1expected1.size() != 0 || t1expected2.size() != 0) {
		has_error = true;
		std::cout << "ERROR: Mismatch in transaction counts!" << std::endl;
	}
	std::cout << "Done.\n\n";
	

	/***************** TEST 2 - Complete Pipeline Irregular Pattern *****************/
	std::cout << "TEST 2\n-------------\n";
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		t1in0.write(1);
		t1in0.write(1);
		t1in1.write(2);
		count += 3;
		Testbench_annotated_mux_rr_complete(t1in0, t1in1, t1in2, t1out0, t1out1, t1out2);
		if (i % 2 == 0) {
			t1expected0.write(1);
		} else {
			t1expected1.write(2);
		}
	}

	// Calculate how many elements each stream should contain
	static_assert(REP_COUNT % 2 == 0, "Even REP_COUNT required for Test 2!");
	auto o0_left = 2 * REP_COUNT - REP_COUNT / 2;
	auto o1_left = REP_COUNT / 2;
	for (unsigned int i = 0; i < REP_COUNT / 2; i++) {
		has_error |= matches_expected(i, REP_COUNT/2, 0, t1out0, t1expected0);
		has_error |= matches_expected(i, REP_COUNT/2, 0, t1out1, t1expected1);
		count -= 2;
	}
	if (t1in0.size() != o0_left) {
		std::cout << "ERROR: Stream 0 has " << t1in0.size() << " data left, expected " << o0_left << std::endl;
		has_error = true;
	}
	if (t1in1.size() != o1_left) {
		std::cout << "ERROR: Stream 1 has " << t1in1.size() << " data left, expected " << o1_left << std::endl;
		has_error = true;
	}
	if (t1expected0.size() != 0 || t1expected1.size() != 0 || t1expected2.size() != 0 || count != (3-1)*REP_COUNT) {
		std::cout << "ERROR: Transaction count mismatch!" << std::endl;
		std::cout << "\t(" << t1expected0.size() << ", " << t1expected1.size() << ", " << t1expected2.size() << ", " << count << ")\n";
		has_error = true;
	}
	std::cout << "Done.\n\n";

	if (has_error) {
		std::cout << "REP_COUNT: " << REP_COUNT << "\n" << std::endl;
	}

	// TODO: Clear leftover stream data

	return has_error;
}