#include <iostream>
#include <hls_stream.h>

#include "data/annotated_mux_config.h"
#include "annotated_mux.hpp"

void Testbench_annotated_mux_rr(hls::stream<T0> &in0, hls::stream<T1> &in1, hls::stream<T2> &in2, hls::stream<TO> &out);

int main() {
	hls::stream<T0> in0;
	hls::stream<T1> in1;
	hls::stream<T2> in2;
	hls::stream<TO> out;
	hls::stream<TO> expected;

	// First let all inputs have the same availability
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		in0.write((i+1)*2);
		in1.write((i+1)*3);
		in2.write((i+1)*4);
		expected.write((i+1)*2);
		expected.write((i+1)*3);
		expected.write((i+1)*4);
	}

	// Now only in0 sends
	for (unsigned int i = 0; i < REP_COUNT; i++) {
		in0.write(99);
		expected.write(99);
	}

	bool has_error = false;
	unsigned int timeout = 20;
	while (timeout > 0) {
		TO header;
		TO data;
		Testbench_annotated_mux_rr(in0, in1, in2, out);
		if (out.empty()) {
			timeout--;
			continue;
		}
		header = out.read();
		data = out.read();
		TO exp_header, exp_data;
		exp_header = expected.read();
		exp_data = expected.read();
		if (exp_header != header) {
			std::cout << "ERROR: Header mismatch. Expected " << exp_header << " but got " << header << std::endl;
			has_error = true;
		}
		if (exp_data != data) {
			std::cout << "ERROR: Data mismatch. Expected " << exp_data << " but got " << data << std::endl;
			has_error = true;
		}
	}
	return !has_error;
}