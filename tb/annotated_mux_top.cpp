#include <hls_stream.h>

#include "data/annotated_mux_config.h"
#include "annotated_mux.hpp"

void Testbench_annotated_mux_rr(hls::stream<T0> &in0, hls::stream<T1> &in1, hls::stream<T2> &in2, hls::stream<TO> &out) {
#pragma HLS INTERFACE axis port=in0
#pragma HLS INTERFACE axis port=in1
#pragma HLS INTERFACE axis port=in2
#pragma HLS INTERFACE axis port=out
#pragma HLS INTERFACE ap_ctrl_none port=return
    AnnotatedMultiplex::StreamingNetworkMultiplex<MultiplexStrategy::ROUND_ROBIN, TO_WIDTH, T0, T1, T2>(out, in0, in1, in2);
}
